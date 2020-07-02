// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func (p *planner) getVirtualTabler() VirtualTabler {
	return p.extendedEvalCtx.VirtualSchemas
}

// createDropDatabaseJob queues a job for dropping a database.
func (p *planner) createDropDatabaseJob(
	ctx context.Context,
	databaseID sqlbase.ID,
	droppedDetails []jobspb.DroppedTableDetails,
	jobDesc string,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionSchemaChangeJob) {
		if descs.MigrationSchemaChangeRequiredFromContext(ctx) {
			return descs.ErrSchemaChangeDisallowedInMixedState
		}
	}
	// TODO (lucy): This should probably be deleting the queued jobs for all the
	// tables being dropped, so that we don't have duplicate schema changers.
	descriptorIDs := make([]sqlbase.ID, 0, len(droppedDetails))
	for _, d := range droppedDetails {
		descriptorIDs = append(descriptorIDs, d.ID)
	}
	jobRecord := jobs.Record{
		Description:   jobDesc,
		Username:      p.User(),
		DescriptorIDs: descriptorIDs,
		Details: jobspb.SchemaChangeDetails{
			DroppedTables:     droppedDetails,
			DroppedDatabaseID: databaseID,
			FormatVersion:     jobspb.JobResumerFormatVersion,
		},
		Progress: jobspb.SchemaChangeProgress{},
	}
	_, err := p.extendedEvalCtx.QueueJob(jobRecord)
	return err
}

// createOrUpdateSchemaChangeJob queues a new job for the schema change if there
// is no existing schema change job for the table, or updates the existing job
// if there is one.
func (p *planner) createOrUpdateSchemaChangeJob(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	jobDesc string,
	mutationID sqlbase.MutationID,
) error {
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionSchemaChangeJob) {
		if descs.MigrationSchemaChangeRequiredFromContext(ctx) {
			return descs.ErrSchemaChangeDisallowedInMixedState
		}
	}
	var job *jobs.Job
	// Iterate through the queued jobs to find an existing schema change job for
	// this table, if it exists.
	// TODO (lucy): Looking up each job to determine this is not ideal. Maybe
	// we need some additional state in extraTxnState to help with lookups.
	for _, jobID := range *p.extendedEvalCtx.Jobs {
		var err error
		j, err := p.ExecCfg().JobRegistry.LoadJobWithTxn(ctx, jobID, p.txn)
		if err != nil {
			return err
		}
		schemaDetails, ok := j.Details().(jobspb.SchemaChangeDetails)
		if !ok {
			continue
		}
		if schemaDetails.TableID == tableDesc.ID {
			job = j
			break
		}
	}

	var spanList []jobspb.ResumeSpanList
	jobExists := job != nil
	if jobExists {
		spanList = job.Details().(jobspb.SchemaChangeDetails).ResumeSpanList
	}
	span := tableDesc.PrimaryIndexSpan(p.ExecCfg().Codec)
	for i := len(tableDesc.ClusterVersion.Mutations) + len(spanList); i < len(tableDesc.Mutations); i++ {
		spanList = append(spanList,
			jobspb.ResumeSpanList{
				ResumeSpans: []roachpb.Span{span},
			},
		)
	}

	if !jobExists {
		// Queue a new job.
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: sqlbase.IDs{tableDesc.GetID()},
			Details: jobspb.SchemaChangeDetails{
				TableID:        tableDesc.ID,
				MutationID:     mutationID,
				ResumeSpanList: spanList,
				FormatVersion:  jobspb.JobResumerFormatVersion,
			},
			Progress: jobspb.SchemaChangeProgress{},
		}
		newJob, err := p.extendedEvalCtx.QueueJob(jobRecord)
		if err != nil {
			return err
		}
		// Only add a MutationJob if there's an associated mutation.
		// TODO (lucy): get rid of this when we get rid of MutationJobs.
		if mutationID != sqlbase.InvalidMutationID {
			tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: *newJob.ID()})
		}
		log.Infof(ctx, "queued new schema change job %d for table %d, mutation %d",
			*newJob.ID(), tableDesc.ID, mutationID)
	} else {
		// Update the existing job.
		oldDetails := job.Details().(jobspb.SchemaChangeDetails)
		newDetails := jobspb.SchemaChangeDetails{
			TableID:        tableDesc.ID,
			MutationID:     oldDetails.MutationID,
			ResumeSpanList: spanList,
			FormatVersion:  jobspb.JobResumerFormatVersion,
		}
		if oldDetails.MutationID != sqlbase.InvalidMutationID {
			// The previous queued schema change job was associated with a mutation,
			// which must have the same mutation ID as this schema change, so just
			// check for consistency.
			if mutationID != sqlbase.InvalidMutationID && mutationID != oldDetails.MutationID {
				return errors.AssertionFailedf(
					"attempted to update job for mutation %d, but job already exists with mutation %d",
					mutationID, oldDetails.MutationID)
			}
		} else {
			// The previous queued schema change job didn't have a mutation.
			if mutationID != sqlbase.InvalidMutationID {
				newDetails.MutationID = mutationID
				// Also add a MutationJob on the table descriptor.
				// TODO (lucy): get rid of this when we get rid of MutationJobs.
				tableDesc.MutationJobs = append(tableDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
					MutationID: mutationID, JobID: *job.ID()})
			}
		}
		if err := job.WithTxn(p.txn).SetDetails(ctx, newDetails); err != nil {
			return err
		}
		if jobDesc != "" {
			if err := job.WithTxn(p.txn).SetDescription(
				ctx,
				func(ctx context.Context, description string) (string, error) {
					return strings.Join([]string{description, jobDesc}, ";"), nil
				},
			); err != nil {
				return err
			}
		}
		log.Infof(ctx, "job %d: updated with schema change for table %d, mutation %d",
			*job.ID(), tableDesc.ID, mutationID)
	}
	return nil
}

// writeSchemaChange effectively writes a table descriptor to the
// database within the current planner transaction, and queues up
// a schema changer for future processing.
// TODO (lucy): The way job descriptions are handled needs improvement.
// Currently, whenever we update a job, the provided job description string, if
// non-empty, is appended to the end of the existing description, regardless of
// whether the particular schema change written in this method call came from a
// separate statement in the same transaction, or from updating a dependent
// table descriptor during a schema change to another table, or from a step in a
// larger schema change to the same table.
func (p *planner) writeSchemaChange(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	mutationID sqlbase.MutationID,
	jobDesc string,
) error {
	if !p.EvalContext().TxnImplicit {
		telemetry.Inc(sqltelemetry.SchemaChangeInExplicitTxnCounter)
	}
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return fmt.Errorf("table %q is being dropped", tableDesc.Name)
	}
	if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, mutationID); err != nil {
		return err
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *planner) writeSchemaChangeToBatch(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, b *kv.Batch,
) error {
	if !p.EvalContext().TxnImplicit {
		telemetry.Inc(sqltelemetry.SchemaChangeInExplicitTxnCounter)
	}
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return fmt.Errorf("table %q is being dropped", tableDesc.Name)
	}
	return p.writeTableDescToBatch(ctx, tableDesc, b)
}

func (p *planner) writeDropTable(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, queueJob bool, jobDesc string,
) error {
	if queueJob {
		if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *planner) writeTableDesc(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	b := p.txn.NewBatch()
	if err := p.writeTableDescToBatch(ctx, tableDesc, b); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeTableDescToBatch(
	ctx context.Context, tableDesc *sqlbase.MutableTableDescriptor, b *kv.Batch,
) error {
	if tableDesc.IsVirtualTable() {
		return errors.AssertionFailedf("virtual descriptors cannot be stored, found: %v", tableDesc)
	}

	if tableDesc.IsNewTable() {
		if err := runSchemaChangesInTxn(
			ctx, p, tableDesc, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		); err != nil {
			return err
		}
	} else {
		// Only increment the table descriptor version once in this transaction.
		tableDesc.MaybeIncrementVersion()
	}

	if err := tableDesc.ValidateTable(); err != nil {
		return errors.AssertionFailedf("table descriptor is not valid: %s\n%v", err, tableDesc)
	}

	if err := p.Tables().AddUncommittedTable(*tableDesc); err != nil {
		return err
	}

	return catalogkv.WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.ExecCfg().Settings,
		b,
		p.ExecCfg().Codec,
		tableDesc.GetID(),
		tableDesc,
	)
}
