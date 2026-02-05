// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type uniquenessCheck struct {
	uniquenessCheckApplicability

	execCfg *sql.ExecutorConfig
	indexID descpb.IndexID
	// tableVersion is the descriptor version recorded when the check was planned.
	// It is used to detect concurrent schema changes for non-AS OF inspections.
	tableVersion descpb.DescriptorVersion
	asOf         hlc.Timestamp

	tableDesc catalog.TableDescriptor
	index     catalog.Index
	rowIter   isql.Rows
	state     checkState // State of the check when run on a span.
}

// uniquenessCheckApplicability is a lightweight version that only implements
// applicability logic.
type uniquenessCheckApplicability struct {
	tableID descpb.ID
}

var _ inspectCheck = (*uniquenessCheck)(nil)
var _ inspectCheckApplicability = (*uniquenessCheckApplicability)(nil)

func (c *uniquenessCheckApplicability) AppliesTo(
	codec keys.SQLCodec, span roachpb.Span,
) (bool, error) {
	return spanContainsTable(c.tableID, codec, span)
}

func (c *uniquenessCheckApplicability) IsSpanLevel() bool {
	return false
}

// Started implements the inspectCheck interface.
func (c *uniquenessCheck) Started() bool {
	return c.state != checkNotStarted
}

// Start implements the inspectCheck interface.
func (c *uniquenessCheck) Start(
	ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int,
) error {
	if err := assertCheckApplies(c, cfg.Codec, span); err != nil {
		return err
	}

	if err := c.loadCatalogInfo(ctx); err != nil {
		return err
	}

	keyColNames := make([]string, c.index.NumKeyColumns())
	for i := 0; i < c.index.NumKeyColumns(); i++ {
		colID := c.index.GetKeyColumnID(i)
		col, err := catalog.MustFindColumnByID(c.tableDesc, colID)
		if err != nil {
			return err
		}
		keyColNames[i] = col.GetName()
	}

	predicate, queryArgs, err := getPredicateAndQueryArgs(
		ctx, cfg, span, c.tableDesc, c.tableDesc.GetPrimaryIndex(), c.asOf, keyColNames,
	)
	if err != nil {
		return err
	}

	query := c.buildUniquenessCheckQuery(keyColNames, predicate)
	queryWithAsOf := fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", query, c.asOf.AsOfSystemTime())

	qos := getInspectQoS(&c.execCfg.Settings.SV)
	it, err := c.execCfg.DistSQLSrv.DB.Executor().QueryIteratorEx(
		ctx, "inspect-uniqueness-check", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.NodeUserName(),
			QualityOfService: &qos,
		},
		queryWithAsOf,
		queryArgs...,
	)
	if err != nil {
		return err
	}

	c.rowIter = it
	c.state = checkRunning

	return nil
}

func (c *uniquenessCheck) Next(
	ctx context.Context, cfg *execinfra.ServerConfig,
) (*inspectIssue, error) {
	if c.rowIter == nil {
		return nil, errors.AssertionFailedf("nil rowIter unexpected")
	}

	ok, err := c.rowIter.Next(ctx)
	if err != nil {
		// Close the iterator to prevent further usage. The close may emit the
		// internal error too, but we only need to capture it once.
		_ = c.Close(ctx)
		c.state = checkDone

		issue := errorToInternalInspectIssue(err, c.asOf, c.tableDesc, c.index, nil /* details */)
		return issue, nil
	}
	if !ok {
		c.state = checkDone
		return nil, nil
	}

	duplicateCount := int64(tree.MustBeDInt(c.rowIter.Cur()[0]))

	pkColumnCount := c.tableDesc.GetPrimaryIndex().NumKeyColumns()
	var primaryKeyDatums tree.Datums
	for i := 0; i < pkColumnCount; i++ {
		primaryKeyDatums = append(primaryKeyDatums, c.rowIter.Cur()[1+i])
	}
	primaryKey := tree.NewDString(primaryKeyDatums.String())

	details := make(map[redact.RedactableString]interface{})
	details["index_name"] = c.index.GetName()
	details["duplicate_count"] = duplicateCount

	return &inspectIssue{
		ErrorType:  DuplicateUniqueValue,
		AOST:       c.asOf.GoTime(),
		DatabaseID: c.tableDesc.GetParentID(),
		SchemaID:   c.tableDesc.GetParentSchemaID(),
		ObjectID:   c.tableDesc.GetID(),
		PrimaryKey: primaryKey.String(),
		Details:    details,
	}, nil
}

// Done implements the inspectCheck interface.
func (c *uniquenessCheck) Done(context.Context) bool {
	return c.state == checkDone
}

// Close implements the inspectCheck interface.
func (c *uniquenessCheck) Close(context.Context) error {
	if c.rowIter != nil {
		// Clear the iter ahead of close to ensure we only attempt the close once.
		it := c.rowIter
		c.rowIter = nil
		if err := it.Close(); err != nil {
			return errors.Wrap(err, "closing uniqueness check iterator")
		}
	}
	return nil
}

// buildUniquenessCheckQuery constructs a query that finds duplicate key values
// in a unique index by hashing and grouping the key columns.
func (c *uniquenessCheck) buildUniquenessCheckQuery(keyColNames []string, predicate string) string {
	hashExpr := c.buildHashExpression(keyColNames)

	encodedColNames := make([]string, len(keyColNames))
	for i, colName := range keyColNames {
		encodedColNames[i] = encodeColumnName(colName)
	}
	colList := strings.Join(encodedColNames, ", ")

	whereClause := buildWhereClause(predicate, nil /* nullFilters */)

	return fmt.Sprintf(`
WITH hashed_keys AS (
  SELECT
    %s,
    %s AS hash
  FROM [%d AS t]@{FORCE_INDEX=[%d]}%s
)
SELECT
  count(*) AS duplicate_count,
  %s
FROM hashed_keys
GROUP BY hash, %s
HAVING count(*) > 1`,
		colList,
		hashExpr,
		c.tableDesc.GetID(),
		c.index.GetID(),
		whereClause,
		colList,
		colList,
	)
}

// buildHashExpression creates a hash expression for the given columns.
// Uses fnv64 hash of the datums_to_bytes encoding of the columns.
func (c *uniquenessCheck) buildHashExpression(columnNames []string) string {
	args := make([]string, len(columnNames))
	for i, col := range columnNames {
		args[i] = encodeColumnName(col)
	}
	encoded := fmt.Sprintf("crdb_internal.datums_to_bytes(%s)", strings.Join(args, ", "))

	return fmt.Sprintf("fnv64(COALESCE(%s, ''::BYTES))", encoded)
}

// loadCatalogInfo loads the table descriptor and validates the specified
// index. It verifies that the index exists on the table and is
// eligible for uniqueness checking. If the index is valid, it stores the
// descriptor and index metadata in the uniquenessCheck struct.
func (c *uniquenessCheck) loadCatalogInfo(ctx context.Context) error {
	tableDesc, err := loadTableDesc(ctx, c.execCfg, c.tableID, c.tableVersion, c.asOf)
	if err != nil {
		return err
	}
	c.tableDesc = tableDesc

	index, err := findIndexByID(c.tableDesc, c.indexID)
	if err != nil {
		return err
	}

	if !index.IsUnique() {
		if !buildutil.CrdbTestBuild { // For ease of testing, allow non-unique indexes to be checked.
			return errors.AssertionFailedf("non-unique index shouldn't be unique checked")
		}
	}

	var isMultiRegion bool
	err = c.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbDesc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, tableDesc.GetParentID())
		if err != nil {
			return errors.Wrap(err, "getting database descriptor")
		}
		isMultiRegion = dbDesc.IsMultiRegion()
		return nil
	})
	if err != nil {
		return err
	}

	if isMultiRegion {
		return errors.Newf("uniqueness check is not supported for multi-region databases")
	}

	c.index = index

	return nil
}
