// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/inspect/inspectpb"
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
// It relies on other jobs to perform the row count.
type rowCountCheck struct {
	rowCountCheckApplicability

	tableLoader // Embeds table loading logic.

	state checkState // State of the check when run on a span.

	expected uint64

	rowCount uint64 // Populated from the job progress for the cluster check.

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
	logger *inspectLoggerBundle,
	data *inspectpb.InspectProcessorSpanCheckData,
) error {
	for _, check := range checks {
		// TODO(#160989): Handle inconsistency btwn checks on the same span.
		if check, ok := check.(inspectCheckRowCount); ok {
			data.SpanRowCount = check.RowCount()
			return nil
		}
	}

	return errors.AssertionFailedf("rowCountCheck requires an inspectCheckRowCount to be registered")
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

	if err := c.loadTableDesc(ctx, c.tableID); err != nil {
		return err
	}
	c.rowCount = checkData.RowCount

	return nil
}

// tableLoader is a composition type used to share table loading logic between
// inspect checks.
type tableLoader struct {
	execCfg *sql.ExecutorConfig
	// tableVersion is the descriptor version recorded when the check was planned.
	// It is used to detect concurrent schema changes for non-AS OF inspections.
	tableVersion descpb.DescriptorVersion
	asOf         hlc.Timestamp

	tableDesc catalog.TableDescriptor
}

func (l *tableLoader) loadTableDesc(ctx context.Context, tableID descpb.ID) error {
	return l.execCfg.DistSQLSrv.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if !l.asOf.IsEmpty() {
			if err := txn.KV().SetFixedTimestamp(ctx, l.asOf); err != nil {
				return err
			}
		}

		byIDGetter := txn.Descriptors().ByIDWithLeased(txn.KV())
		if !l.asOf.IsEmpty() {
			byIDGetter = txn.Descriptors().ByIDWithoutLeased(txn.KV())
		}

		var err error
		l.tableDesc, err = byIDGetter.WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return err
		}
		if l.tableVersion != 0 && l.tableDesc.GetVersion() != l.tableVersion {
			return errors.WithHintf(
				errors.Newf(
					"table %s [%d] has had a schema change since the job has started at %s",
					l.tableDesc.GetName(),
					l.tableDesc.GetID(),
					l.tableDesc.GetModificationTime().GoTime().Format(time.RFC3339),
				),
				"use AS OF SYSTEM TIME to avoid schema changes during inspection",
			)
		}

		return nil
	})
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
