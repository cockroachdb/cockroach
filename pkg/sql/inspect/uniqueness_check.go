// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	// tableVersion is the descriptor version recorded when the check was
	// planned. It is used to detect concurrent schema changes for non-AS OF
	// inspections.
	tableVersion descpb.DescriptorVersion
	asOf         hlc.Timestamp

	tableDesc    catalog.TableDescriptor
	index        catalog.Index
	uniqueColIdx int
	rowIter      isql.Rows
	state        checkState // State of the check when run on a span.
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

	lPredicate, lQueryArgs, err := getPredicateAndQueryArgs(
		ctx, cfg, span, c.tableDesc, c.tableDesc.GetPrimaryIndex(), c.asOf, keyColNames, 1, /* endPlaceholderOffset */
	)
	if err != nil {
		return err
	}

	// An empty predicate means the local span has no rows so the check is done
	// early.
	if lPredicate == "" {
		c.state = checkDone
		return nil
	}

	localRegion, err := extractRegionFromSpan(ctx, cfg, span, c.tableDesc)
	if err != nil {
		return errors.Wrap(err, "extracting local region from span")
	}

	remoteRegions, err := c.getRemoteRegions(ctx, localRegion)
	if err != nil {
		return errors.Wrap(err, "getting remote regions")
	}

	remotePredicates, remoteArgs, err := c.getRemotePredicatesAndQueryArgs(ctx, remoteRegions, cfg, span, keyColNames, len(lQueryArgs))
	if err != nil {
		return errors.Wrap(err, "getting remote predicates and query args")
	}

	queries, queryArgs, err := c.buildUniquenessCheckQueries(ctx, lPredicate, lQueryArgs, remotePredicates, remoteArgs, keyColNames)
	if err != nil {
		return errors.Wrap(err, "building uniqueness check queries")
	}

	var query string
	switch len(queries) {
	case 0: // All the remote regions were empty.
		c.state = checkDone
		return nil
	case 1:
		query = queries[0]
	default:
		// Union the remote region queries.
		var wrappedQueries []string
		for _, q := range queries {
			wrappedQueries = append(wrappedQueries, fmt.Sprintf("(%s)", q))
		}
		query = strings.Join(wrappedQueries, "\nUNION ALL\n")
	}

	encodedColNames := make([]string, len(keyColNames))
	for i, colName := range keyColNames {
		encodedColNames[i] = encodeColumnName(colName)
	}
	colList := strings.Join(encodedColNames, ", ")

	queryWithRemoteRegionsGrouped := fmt.Sprintf(`
WITH sub AS (
  %s
)
SELECT %s, array_agg(remote_region) as remote_regions
FROM sub
GROUP BY %s
`,
		query, colList, colList)

	queryWithAsOf := fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", queryWithRemoteRegionsGrouped, c.asOf.AsOfSystemTime())

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

// getRemoteRegions retrieves the list of regions greater than local in this
// table.
func (c *uniquenessCheck) getRemoteRegions(
	ctx context.Context, localRegion string,
) ([]string, error) {
	regionColID := c.tableDesc.GetPrimaryIndex().GetKeyColumnID(0)
	regionCol, err := catalog.MustFindColumnByID(c.tableDesc, regionColID)
	if err != nil {
		return nil, err
	}

	regionType := regionCol.GetType()
	if regionType.TypeMeta.EnumData == nil {
		return nil, errors.AssertionFailedf("region column is not an enum type")
	}
	allRegions := regionType.TypeMeta.EnumData.LogicalRepresentations

	var remoteRegions []string
	for _, region := range allRegions {
		if region > localRegion {
			remoteRegions = append(remoteRegions, region)
		}
	}
	return remoteRegions, nil
}

// getRemotePredicatesAndQueryArgs gets the predicates and query arguments for
// the remote regions. Regions without rows in the corresponding span are
// filtered out.
func (c *uniquenessCheck) getRemotePredicatesAndQueryArgs(
	ctx context.Context,
	remoteRegions []string,
	cfg *execinfra.ServerConfig,
	span roachpb.Span,
	keyColNames []string,
	endPlaceholderOffset int,
) (remotePredicates []string, remoteArgs []interface{}, err error) {
	for _, region := range remoteRegions {
		remoteSpan, err := spanForRegion(ctx, cfg, span, c.tableDesc, region)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "creating span for region %q", region)
		}

		rPredicate, rQueryArgs, err := getPredicateAndQueryArgs(
			ctx, cfg, remoteSpan, c.tableDesc, c.tableDesc.GetPrimaryIndex(), c.asOf, keyColNames, endPlaceholderOffset+len(remoteArgs)+1,
		)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "getting predicate for region %q", region)
		}

		// An empty predicate means the remote span has no rows so the span in
		// that region is skipped.
		if rPredicate == "" {
			continue
		}

		remotePredicates = append(remotePredicates, rPredicate)
		remoteArgs = append(remoteArgs, rQueryArgs...)
	}

	return remotePredicates, remoteArgs, nil
}

// buildUniquenessCheckQueries builds the queries to find duplicates between the
// local region and each remote region. The placeholders in the remote
// predicates are renumbered so queryArgs can be flattened with lQueryArgs.
func (c *uniquenessCheck) buildUniquenessCheckQueries(
	ctx context.Context,
	lPredicate string,
	lQueryArgs []interface{},
	remotePredicates []string,
	remoteQueryArgs []interface{},
	keyColNames []string,
) (queries []string, queryArgs []interface{}, err error) {
	// Add the local query arguments once (shared by all queries)
	queryArgs = append(slices.Clone(lQueryArgs), remoteQueryArgs...)

	for _, rPredicate := range remotePredicates {
		query := c.buildUniquenessCheckQuery(keyColNames, lPredicate, rPredicate)
		queries = append(queries, query)
	}

	return queries, queryArgs, nil
}

// Next implements the inspectCheck interface.
func (c *uniquenessCheck) Next(
	ctx context.Context, cfg *execinfra.ServerConfig,
) (*inspectIssue, error) {
	if c.state == checkDone {
		return nil, nil
	}

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

	pkColumnCount := c.tableDesc.GetPrimaryIndex().NumKeyColumns()
	var primaryKeyDatums tree.Datums
	for i := 0; i < pkColumnCount; i++ {
		primaryKeyDatums = append(primaryKeyDatums, c.rowIter.Cur()[i])
	}
	primaryKey := tree.NewDString(primaryKeyDatums.String())

	details := make(map[redact.RedactableString]interface{})
	details["index_name"] = c.index.GetName()

	remoteRegionsDatum := c.rowIter.Cur()[pkColumnCount]
	if remoteRegionsArray, ok := remoteRegionsDatum.(*tree.DArray); ok {
		var remoteRegions []string
		for _, regionDatum := range remoteRegionsArray.Array {
			if regionEnum, ok := regionDatum.(*tree.DEnum); ok {
				remoteRegions = append(remoteRegions, regionEnum.LogicalRep)
			}
		}
		details["remote_regions"] = remoteRegions
	}

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

// buildUniquenessCheckQuery constructs a query that finds rows in the right
// table that match rows in the left table on the unique columns starting from
// uniqueColIdx. Returns the key column values from the right table.
func (c *uniquenessCheck) buildUniquenessCheckQuery(
	keyColNames []string, lPredicate, rPredicate string,
) string {
	encodedColNames := make([]string, len(keyColNames))
	for i, colName := range keyColNames {
		encodedColNames[i] = encodeColumnName(colName)
	}

	lWhereClause := buildWhereClause(lPredicate, nil /* nullFilters */)
	rWhereClause := buildWhereClause(rPredicate, nil /* nullFilters */)

	// Build the column list for SELECT.
	colList := strings.Join(encodedColNames, ", ")

	joinOn := fmt.Sprintf("l.%s = r.%s", encodedColNames[c.uniqueColIdx], encodedColNames[c.uniqueColIdx])

	var lColList []string
	for _, col := range encodedColNames {
		lColList = append(lColList, "l."+col)
	}
	lColListStr := strings.Join(lColList, ", ")

	return fmt.Sprintf(`
WITH
  l AS (
    SELECT %s
    FROM [%d AS t]@{FORCE_INDEX=[%d]}%s
  ),
  r AS (
    SELECT %s
    FROM [%d AS t]@{FORCE_INDEX=[%d]}%s
  )
SELECT %s, r.crdb_region as remote_region
FROM l
INNER JOIN r ON %s`,
		colList,
		c.tableDesc.GetID(),
		c.index.GetID(),
		lWhereClause,
		colList,
		c.tableDesc.GetID(),
		c.index.GetID(),
		rWhereClause,
		lColListStr,
		joinOn,
	)
}

// loadCatalogInfo loads the table descriptor and validates the specified index.
// It verifies that the index exists on the table and is eligible for uniqueness
// checking. If the index is valid, it stores the descriptor and index metadata
// in the uniquenessCheck struct.
func (c *uniquenessCheck) loadCatalogInfo(ctx context.Context) error {
	tableDesc, err := loadTableDesc(ctx, c.execCfg, c.tableID, c.tableVersion, c.asOf)
	if err != nil {
		return err
	}
	if !tableDesc.IsLocalityRegionalByRow() {
		if !buildutil.CrdbTestBuild && !testing.Testing() { // For ease of testing, allow faked multi-region databases.
			return errors.AssertionFailedf("uniqueness check is only supported for tables with REGIONAL BY ROW locality")
		}
	}
	c.tableDesc = tableDesc

	index, err := findIndexByID(c.tableDesc, c.indexID)
	if err != nil {
		return err
	}

	if !index.Primary() {
		return errors.AssertionFailedf("uniqueness check is only supported for primary indexes")
	}

	if !index.IsUnique() {
		return errors.AssertionFailedf("non-unique index shouldn't be unique checked")
	}

	uniqueColIdxs, err := findUniqueColIdxs(c.tableDesc, index)
	if err != nil {
		return err
	}
	if len(uniqueColIdxs) == 0 {
		return errors.AssertionFailedf("index has no `unique_rowid()` or `unordered_unique_rowid()` column to be unique checked")
	} else if len(uniqueColIdxs) > 1 {
		return errors.AssertionFailedf("uniqueness check for indexes with multiple `unique_rowid()` or `unordered_unique_rowid()` columns is not supported")
	}

	// TODO(154551): Support full scans of the remote regions.
	if uniqueColIdxs[0] != 1 {
		return errors.AssertionFailedf("the unique column must be immediately after the regionality column")
	}

	c.index = index
	c.uniqueColIdx = uniqueColIdxs[0]

	return nil
}
