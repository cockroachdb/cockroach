// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TriggerJob starts an inspect job for the snapshot. This always uses a fresh
// transaction to create and start the job.
func TriggerJob(
	ctx context.Context,
	jobRecordDescription string,
	execCfg *sql.ExecutorConfig,
	checks []*jobspb.InspectDetails_Check,
	asOf hlc.Timestamp,
) (*jobs.StartableJob, error) {
	record := jobs.Record{
		JobID:       execCfg.JobRegistry.MakeJobID(),
		Description: jobRecordDescription,
		Details: jobspb.InspectDetails{
			Checks: checks,
			AsOf:   asOf,
		},
		Progress:  jobspb.InspectProgress{},
		CreatedBy: nil,
		Username:  username.NodeUserName(),
		DescriptorIDs: func() descpb.IDs {
			ids := catalog.DescriptorIDSet{}
			for _, check := range checks {
				ids.Add(check.TableID)
			}
			return ids.Ordered()
		}(),
	}

	var job *jobs.StartableJob
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		return execCfg.JobRegistry.CreateStartableJobWithTxn(ctx, &job, record.JobID, txn, record)
	}); err != nil {
		if job != nil {
			if cleanupErr := job.CleanupOnRollback(ctx); cleanupErr != nil {
				log.Dev.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
			}
		}
		return nil, err
	}

	if err := job.Start(ctx); err != nil {
		return nil, err
	}
	log.Dev.Infof(ctx, "created and started inspect job %d", job.ID())

	return job, nil
}

// checksForDatabase generates checks on every supported index on every
// table in the given database.
func checksForDatabase(
	ctx context.Context, p sql.PlanHookState, db catalog.DatabaseDescriptor,
) ([]*jobspb.InspectDetails_Check, error) {
	avoidLeased := false
	if aost := p.ExtendedEvalContext().AsOfSystemTime; aost != nil {
		avoidLeased = true
	}
	byNameGetter := p.Descriptors().ByNameWithLeased(p.Txn())
	if avoidLeased {
		byNameGetter = p.Descriptors().ByName(p.Txn())
	}
	tables, err := byNameGetter.Get().GetAllTablesInDatabase(ctx, p.Txn(), db)
	if err != nil {
		return nil, err
	}

	checks := []*jobspb.InspectDetails_Check{}

	if err := tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
		tableChecks, err := ChecksForTable(ctx, p, desc.(catalog.TableDescriptor), nil /* rowCount */)
		if err != nil {
			return err
		}
		checks = append(checks, tableChecks...)
		return nil
	}); err != nil {
		return nil, err
	}

	return checks, nil
}

// ChecksForTable generates checks on every supported index on the given table.
func ChecksForTable(
	ctx context.Context, p sql.PlanHookState, table catalog.TableDescriptor, expectedRowCount *uint64,
) ([]*jobspb.InspectDetails_Check, error) {
	checks := []*jobspb.InspectDetails_Check{}

	// Skip virtual tables since they don't have physical storage to inspect.
	if table.IsVirtualTable() {
		return checks, nil
	}

	for _, index := range table.PublicNonPrimaryIndexes() {
		if reason := isSupportedIndexForIndexConsistencyCheck(index, table); reason != "" {
			if p != nil {
				p.BufferClientNotice(ctx, pgnotice.Newf(
					"skipping index %q on table %q: not supported for index consistency checking", index.GetName(), table.GetName()))
			}
			continue
		}
		check := jobspb.InspectDetails_Check{
			Type:         jobspb.InspectCheckIndexConsistency,
			TableID:      table.GetID(),
			IndexID:      index.GetID(),
			TableVersion: table.GetVersion(),
		}
		checks = append(checks, &check)
	}

	if expectedRowCount != nil {
		var includesRowCounterCheck bool
		for _, check := range checks {
			switch check.Type {
			case jobspb.InspectCheckIndexConsistency:
				includesRowCounterCheck = true
			}
		}

		// If none of the previous checks provide a row count, skip the check.
		if includesRowCounterCheck {
			checks = append(checks, &jobspb.InspectDetails_Check{
				Type:     jobspb.InspectCheckRowCount,
				TableID:  table.GetID(),
				RowCount: *expectedRowCount,
			})
		} else {
			if p != nil {
				p.BufferClientNotice(ctx, pgnotice.Newf(
					"skipping row count on table %q: no other checks provide a row count", table.GetName()))
			}
		}
	}

	return checks, nil
}

type indexKey struct {
	descpb.ID
	descpb.IndexID
}

// checksByIndexNames generates checks for the specified index names.
// If index names are not found or are not supported for inspection, an error is returned.
// Index names are deduplicated.
func checksByIndexNames(
	ctx context.Context, p sql.PlanHookState, names tree.TableIndexNames,
) ([]*jobspb.InspectDetails_Check, error) {
	checks := []*jobspb.InspectDetails_Check{}

	var seenIndexes = make(map[indexKey]struct{})
	for _, indexName := range names {
		_, table, index, err := p.GetTableAndIndex(ctx, indexName, privilege.INSPECT, false /* skipCache */)
		if err != nil {
			return nil, err
		}

		if _, ok := seenIndexes[indexKey{table.GetID(), index.GetID()}]; ok {
			continue
		}
		seenIndexes[indexKey{table.GetID(), index.GetID()}] = struct{}{}

		if reason := isSupportedIndexForIndexConsistencyCheck(index, table); reason != "" {
			return nil, pgerror.Newf(pgcode.InvalidName, "index %q on table %q is not supported for index consistency checking", index.GetName(), table.GetName())
		}

		checks = append(checks, &jobspb.InspectDetails_Check{
			Type:         jobspb.InspectCheckIndexConsistency,
			TableID:      table.GetID(),
			IndexID:      index.GetID(),
			TableVersion: table.GetVersion(),
		})
	}

	return checks, nil
}

// isSupportedIndexForIndexConsistencyCheck returns an empty string if a given
// index is supported for index consistency checking or a reason if it is not.
func isSupportedIndexForIndexConsistencyCheck(
	index catalog.Index, table catalog.TableDescriptor,
) (reason string) {
	if !index.Public() {
		return "index is not public"
	}

	if index.Primary() {
		return "cannot check primary index consistency against itself"
	}

	// We can only check a secondary index that has a 1-to-1 mapping between
	// keys in the primary index. Unsupported indexes should be filtered out
	// when the job is created.
	// TODO(154862): support partial indexes
	if index.IsPartial() {
		return "partial index"
	}
	// TODO(154762): support hash sharded indexes
	if index.IsSharded() {
		return "hash-sharded index"
	}
	// TODO(154772): support expression indexes
	if table.IsExpressionIndex(index) {
		return "expression index"
	}

	// Check if any of the index key columns are virtual columns.
	// TODO(155841): add support for indexes on virtual columns.
	for i := 0; i < index.NumKeyColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col := catalog.FindColumnByID(table, colID)
		if col != nil && col.IsVirtual() {
			return "index on virtual column"
		}
	}

	switch t := index.GetType(); t {
	// TODO(154860): support inverted indexes
	case idxtype.INVERTED, idxtype.VECTOR:
		return t.String()
	}

	return ""
}
