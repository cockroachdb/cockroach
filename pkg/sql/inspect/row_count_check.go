// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/inspect/inspectpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// inspectCheckRowCount extends an inspectCheck that counts rows in addition to
// its primary validation.
type inspectCheckRowCount interface {
	// Rows returns the number of rows counted by the check.
	RowCount() uint64
}

// rowCountCheck verifies a table's row count matches the expected count.
// It uses row counts exposed by other checks when possible.
type rowCountCheck struct {
	rowCountCheckApplicability

	execCfg *sql.ExecutorConfig
	// tableVersion is the descriptor version recorded when the check was planned.
	// It is used to detect concurrent schema changes for non-AS OF inspections.
	tableVersion descpb.DescriptorVersion
	asOf         hlc.Timestamp

	state checkState // State of the check when run on a span.

	expected uint64

	tableDesc catalog.TableDescriptor // Populated from the tableID for the cluster check.
	rowCount  uint64                  // Populated from the job progress for the cluster check.

	clusterState checkClusterState // State of the check when run as a cluster-level check.
}

// rowCountCheckApplicability is a lightweight version that only implements applicability logic.
type rowCountCheckApplicability struct {
	tableID descpb.ID
}

// AppliesTo implements the inspectCheckApplicability interface.
func (c *rowCountCheckApplicability) AppliesTo(
	codec keys.SQLCodec, span roachpb.Span,
) (bool, error) {
	return spanContainsTable(c.tableID, codec, span)
}

func (c *rowCountCheckApplicability) IsSpanLevel() bool {
	return true
}

// AppliesToCluster implements the inspectCheckClusterApplicability interface.
func (c *rowCountCheckApplicability) AppliesToCluster() (bool, error) {
	return true, nil
}

var _ inspectCheck = (*rowCountCheck)(nil)
var _ inspectCheckApplicability = (*rowCountCheckApplicability)(nil)
var _ inspectCheckClusterApplicability = (*rowCountCheckApplicability)(nil)

var _ inspectSpanCheck = (*rowCountCheck)(nil)
var _ inspectClusterCheck = (*rowCountCheck)(nil)

// Started implements the inspectCheck interface.
func (c *rowCountCheck) Started() bool {
	return c.state != checkNotStarted
}

// Start implements the inspectCheck interface.
func (c *rowCountCheck) Start(
	ctx context.Context, cfg *execinfra.ServerConfig, span roachpb.Span, workerIndex int,
) error {
	if err := assertCheckApplies(c, cfg.Codec, span); err != nil {
		return err
	}

	c.state = checkRunning

	return nil
}

func (c *rowCountCheck) Next(
	ctx context.Context, cfg *execinfra.ServerConfig,
) (*inspectIssue, error) {
	c.state = checkDone
	return nil, nil
}

// Done implements the inspectCheck interface.
func (c *rowCountCheck) Done(context.Context) bool {
	return c.state == checkDone
}

// Close implements the inspectCheck interface.
func (c *rowCountCheck) Close(context.Context) error {
	return nil
}

// CheckSpan implements the inspectSpanCheck interface.
func (c *rowCountCheck) CheckSpan(
	ctx context.Context,
	checks inspectChecks,
	span roachpb.Span,
	logger *inspectLoggerBundle,
	data *inspectpb.InspectProcessorSpanCheckData,
) error {
	// Find the row count among the registered checks.
	for _, check := range checks {
		// TODO(#160989): Handle inconsistency btwn checks on the same span.
		if check, ok := check.(inspectCheckRowCount); ok {
			data.SpanRowCount = check.RowCount()
			return nil
		}
	}

	// If none of the checks provide a row count, do the count on the primary index.
	tableDesc, err := loadTableDesc(ctx, c.execCfg, c.tableID, c.tableVersion, c.asOf)
	if err != nil {
		return err
	}
	c.tableDesc = tableDesc

	predicate, queryArgs, err := getPredicateAndQueryArgs(
		ctx,
		&c.execCfg.DistSQLSrv.ServerConfig,
		span,
		c.tableDesc,
		c.tableDesc.GetPrimaryIndex(),
		c.asOf,
		c.tableDesc.TableDesc().PrimaryIndex.KeyColumnNames,
		1, /* endPlaceholderOffset */
	)
	if err != nil {
		return err
	}

	rowCount, err := c.computeRowCount(ctx, predicate, queryArgs)
	if err != nil {
		return err
	}
	data.SpanRowCount = uint64(rowCount)

	return nil
}

// StartedCluster implements the inspectClusterCheck interface.
func (c *rowCountCheck) StartedCluster() bool {
	return c.clusterState != clusterCheckNotStarted
}

// StartCluster implements the inspectClusterCheck interface.
func (c *rowCountCheck) StartCluster(
	ctx context.Context, checkData *inspectpb.InspectSpanCheckData,
) error {
	c.clusterState = clusterCheckRunning

	tableDesc, err := loadTableDesc(ctx, c.execCfg, c.tableID, c.tableVersion, c.asOf)
	if err != nil {
		return err
	}
	c.tableDesc = tableDesc

	c.rowCount = checkData.RowCount

	return nil
}

// NextCluster implements the inspectClusterCheck interface.
func (c *rowCountCheck) NextCluster(ctx context.Context) (*inspectIssue, error) {
	if c.clusterState != clusterCheckRunning {
		return nil, nil
	}

	c.clusterState = clusterCheckDone

	if c.rowCount != c.expected {
		return &inspectIssue{
			ErrorType:  RowCountMismatch,
			AOST:       c.asOf.GoTime(),
			DatabaseID: c.tableDesc.GetParentID(),
			SchemaID:   c.tableDesc.GetParentSchemaID(),
			ObjectID:   c.tableDesc.GetID(),
			Details: map[redact.RedactableString]interface{}{
				"expected": c.expected,
				"actual":   c.rowCount,
			},
		}, nil
	}

	return nil, nil
}

// DoneCluster implements the inspectClusterCheck interface.
func (c *rowCountCheck) DoneCluster(ctx context.Context) bool {
	return c.clusterState == clusterCheckDone
}

// CloseCluster implements the inspectClusterCheck interface.
func (c *rowCountCheck) CloseCluster(ctx context.Context) error {
	return nil
}

// computeRowCount executes a row count query for the primary index and returns
// the row count.
func (c *rowCountCheck) computeRowCount(
	ctx context.Context, predicate string, queryArgs []interface{},
) (int64, error) {
	query := c.buildRowCountQuery(c.tableDesc.GetID(), c.tableDesc.GetPrimaryIndex(), predicate)
	queryWithAsOf := fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", query, c.asOf.AsOfSystemTime())

	qos := getInspectQoS(&c.execCfg.Settings.SV)
	row, err := c.execCfg.DistSQLSrv.DB.Executor().QueryRowEx(
		ctx, "inspect-row-count", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.NodeUserName(),
			QualityOfService: &qos,
		},
		queryWithAsOf,
		queryArgs...,
	)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, errors.AssertionFailedf("row count query returned no rows")
	}
	if len(row) != 1 {
		return 0, errors.AssertionFailedf("row count query returned unexpected column count: %d", len(row))
	}
	return int64(tree.MustBeDInt(row[0])), nil
}

// buildRowCountQuery constructs a query that computes row count for the specified index.
func (c *rowCountCheck) buildRowCountQuery(
	tableID descpb.ID, index catalog.Index, predicate string,
) string {
	whereClause := buildWhereClause(predicate, nil /* nullFilters */)
	return fmt.Sprintf(`
SELECT
  count(*) AS row_count
FROM [%d AS t]@{FORCE_INDEX=[%d]}%s`,
		tableID,
		index.GetID(),
		whereClause,
	)
}
