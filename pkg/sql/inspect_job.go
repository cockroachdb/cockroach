// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(148365): Move this file to the `pkg/sql/inspect` package. The scrub command
// will have to forgo its dependency on inspect.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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

// TriggerInspectJob starts an inspect job for the snapshot.
// TODO(148365): Stop exporting this function when it's moved to the inspect
// package.
func TriggerInspectJob(
	ctx context.Context,
	jobRecordDescription string,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	checks []*jobspb.InspectDetails_Check,
	asOf hlc.Timestamp,
) (*jobs.StartableJob, error) {
	// TODO(sql-queries): add row count check when that is implemented.
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
	if txn == nil {
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
	} else {
		if err := func() (err error) {
			defer func() {
				if err == nil || job == nil {
					return
				}
				if cleanupErr := job.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Dev.Errorf(ctx, "failed to cleanup job: %v", cleanupErr)
				}
			}()
			if err := execCfg.JobRegistry.CreateStartableJobWithTxn(ctx, &job, record.JobID, txn, record); err != nil {
				return err
			}

			return txn.KV().Commit(ctx)
		}(); err != nil {
			return nil, err
		}
	}

	if err := job.Start(ctx); err != nil {
		return nil, err
	}
	log.Dev.Infof(ctx, "created and started inspect job %d", job.ID())

	return job, nil
}

// InspectChecksForDatabase generates checks on every supported index on every
// table in the given database.
// TODO(148365): Stop exporting this function after moving to pkg/sql/inspect.
func InspectChecksForDatabase(
	ctx context.Context, p PlanHookState, db catalog.DatabaseDescriptor,
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
		tableChecks, err := InspectChecksForTable(ctx, p, desc.(catalog.TableDescriptor))
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

// InspectChecksForTable generates checks on every supported index on the given
// table.
func InspectChecksForTable(
	ctx context.Context, p PlanHookState, table catalog.TableDescriptor,
) ([]*jobspb.InspectDetails_Check, error) {
	checks := []*jobspb.InspectDetails_Check{}

	// Skip virtual tables since they don't have physical storage to inspect.
	if table.IsVirtualTable() {
		return checks, nil
	}

	for _, index := range table.PublicNonPrimaryIndexes() {
		if isUnsupportedIndexForIndexConsistencyCheck(index, table) {
			if p != nil {
				p.BufferClientNotice(ctx, pgnotice.Newf(
					"skipping index %q on table %q: not supported for index consistency checking", index.GetName(), table.GetName()))
			}
			continue
		}
		check := jobspb.InspectDetails_Check{Type: jobspb.InspectCheckIndexConsistency, TableID: table.GetID(), IndexID: index.GetID()}
		checks = append(checks, &check)
	}

	return checks, nil
}

type indexKey struct {
	descpb.ID
	descpb.IndexID
}

// InspectChecksByIndexNames generates checks for the specified index names.
// If index names are not found or are not supported for inspection, an error is returned.
// Index names are deduplicated.
func InspectChecksByIndexNames(
	ctx context.Context, p PlanHookState, names tree.TableIndexNames,
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

		if isUnsupportedIndexForIndexConsistencyCheck(index, table) {
			return nil, pgerror.Newf(pgcode.InvalidName, "index %q on table %q is not supported for index consistency checking", index.GetName(), table.GetName())
		}

		checks = append(checks, &jobspb.InspectDetails_Check{Type: jobspb.InspectCheckIndexConsistency, TableID: table.GetID(), IndexID: index.GetID()})
	}

	return checks, nil
}

func isUnsupportedIndexForIndexConsistencyCheck(
	index catalog.Index, table catalog.TableDescriptor,
) bool {
	if index.Primary() {
		return true
	}

	if index.IsPartial() {
		return true
	} else if index.IsSharded() {
		return true
	} else if table.IsExpressionIndex(index) {
		return true
	}

	// Check if any of the index key columns are virtual columns.
	// TODO(155841): add support for indexes on virtual columns.
	for i := 0; i < index.NumKeyColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col := catalog.FindColumnByID(table, colID)
		if col != nil && col.IsVirtual() {
			return true
		}
	}

	switch t := index.GetType(); t {
	case idxtype.VECTOR:
		return true
	case idxtype.INVERTED:
		return true
	}

	return false
}
