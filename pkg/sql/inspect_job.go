// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// IndexTableDesc represents an index descriptor and its associated table.
type IndexTableDesc struct {
	catalog.Index
	catalog.TableDescriptor
}

// TriggerInspectJob starts an inspect job for the snapshot.
func TriggerInspectJob(
	ctx context.Context,
	jobRecordDescription string,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	indexes []IndexTableDesc,
	asOf hlc.Timestamp,
) (*jobs.StartableJob, error) {
	checks := make([]*jobspb.InspectDetails_Check, 0, len(indexes))
	for _, index := range indexes {
		check := jobspb.InspectDetails_Check{
			Type:    jobspb.InspectCheckIndexConsistency,
			TableID: index.TableDescriptor.GetID(),
			IndexID: index.Index.GetID(),
		}
		checks = append(checks, &check)
	}

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
			ids := make(descpb.IDs, 0, len(indexes))
			for _, index := range indexes {
				ids = append(ids, index.TableDescriptor.GetID())
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

// TriggerInspectJobForTable starts an inspect job for the snapshot. The job is
// a check on every compatible secondary index on the table.
func TriggerInspectJobForTable(
	ctx context.Context,
	planner *planner,
	jobRecordDescription string,
	execCfg *ExecutorConfig,
	tableDesc catalog.TableDescriptor,
	asOf hlc.Timestamp,
) (jobspb.JobID, error) {
	secIndexes := tableDesc.PublicNonPrimaryIndexes()
	if len(secIndexes) == 0 {
		return jobspb.InvalidJobID, errors.AssertionFailedf("must have at least one secondary index")
	}

	// Create checks for all secondary indexes, filtering out unsupported types.
	indexes := make([]IndexTableDesc, 0, len(secIndexes))
	for _, index := range secIndexes {
		// Skip unsupported index types.
		if skip, reason := IsUnsupportedIndexForInspect(index, tableDesc); skip {
			if planner != nil {
				planner.BufferClientNotice(ctx, pgnotice.Newf(
					"skipping index %q on table %q: %s", index.GetName(), tableDesc.GetName(), reason))
			}
			continue
		}
		indexes = append(indexes, IndexTableDesc{
			TableDescriptor: tableDesc,
			Index:           index,
		})
	}

	job, err := TriggerInspectJob(ctx, jobRecordDescription, execCfg, nil, indexes, asOf)
	if err != nil {
		return jobspb.InvalidJobID, err
	}

	return job.ID(), nil
}

func IsUnsupportedIndexForInspect(
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
