// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(148365): Move this file to the `pkg/sql/inspect` package. The scrub command
// will have to forgo its dependecy on inspect.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
			seen := make(map[descpb.ID]struct{})
			ids := descpb.IDs{}
			for _, index := range checks {
				if _, ok := seen[index.TableID]; ok {
					continue
				}
				seen[index.TableID] = struct{}{}
				ids = append(ids, index.TableID)
			}
			return ids
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

	log.Dev.Infof(ctx, "created and started inspect job %d", job.ID())
	if err := job.Start(ctx); err != nil {
		return nil, err
	}
	return job, nil
}

// InspectChecksForDatabase generates checks on every supported index on every
// table in the given database.
// TODO(148365): Stop exporting this function after moving to pkg/sql/inspect.
func InspectChecksForDatabase(ctx context.Context, p PlanHookState, db catalog.DatabaseDescriptor) ([]*jobspb.InspectDetails_Check, error) {
	tables, err := p.Descriptors().ByName(p.Txn()).Get().GetAllTablesInDatabase(ctx, p.Txn(), db)
	if err != nil {
		return nil, err
	}

	var checks []*jobspb.InspectDetails_Check
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
func InspectChecksForTable(ctx context.Context, p PlanHookState, table catalog.TableDescriptor) ([]*jobspb.InspectDetails_Check, error) {
	var checks []*jobspb.InspectDetails_Check

	for _, index := range table.PublicNonPrimaryIndexes() {
		if skip, _ := isUnsupportedIndexForIndexConsistencyCheck(index, table); skip {
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

func InspectChecksByIndexName(ctx context.Context, p PlanHookState, names tree.TableIndexNames) ([]*jobspb.InspectDetails_Check, error) {
	var checks []*jobspb.InspectDetails_Check

	for _, indexName := range names {
		_, table, index, err := p.GetTableAndIndex(ctx, indexName, privilege.INSPECT, false /* skipCache */)
		if err != nil {
			return nil, err
		}

		if skip, _ := isUnsupportedIndexForIndexConsistencyCheck(index, table); skip {
			p.BufferClientNotice(ctx, pgnotice.Newf(
				"skipping index %q on table %q: not supported for index consistency checking", index.GetName(), table.GetName()))
			continue
		}

		checks = append(checks, &jobspb.InspectDetails_Check{Type: jobspb.InspectCheckIndexConsistency, TableID: table.GetID(), IndexID: index.GetID()})
	}

	return checks, nil
}

func isUnsupportedIndexForIndexConsistencyCheck(
	index catalog.Index, table catalog.TableDescriptor,
) (bool, string) {
	if index.Primary() {
		return true, "primary indexes are not supported"
	}

	if index.IsPartial() {
		return true, "partial indexes are not supported"
	} else if index.IsSharded() {
		return true, "sharded indexes are not supported"
	} else if table.IsExpressionIndex(index) {
		return true, "expression indexes are not supported"
	}

	switch t := index.GetType(); t {
	case idxtype.VECTOR:
		return true, "vector indexes are not supported"
	case idxtype.INVERTED:
		return true, "inverted indexes are not supported"
	}

	return false, ""
}
