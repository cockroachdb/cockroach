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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	databaseID descpb.ID,
	schemasToDrop []descpb.ID,
	tableDropDetails []jobspb.DroppedTableDetails,
	typesToDrop []*typedesc.Mutable,
	jobDesc string,
) error {
	// TODO (lucy): This should probably be deleting the queued jobs for all the
	// tables being dropped, so that we don't have duplicate schema changers.
	tableIDs := make([]descpb.ID, 0, len(tableDropDetails))
	for _, d := range tableDropDetails {
		tableIDs = append(tableIDs, d.ID)
	}
	typeIDs := make([]descpb.ID, 0, len(typesToDrop))
	for _, t := range typesToDrop {
		typeIDs = append(typeIDs, t.ID)
	}
	jobRecord := jobs.Record{
		Description:   jobDesc,
		Username:      p.User(),
		DescriptorIDs: tableIDs,
		Details: jobspb.SchemaChangeDetails{
			DroppedSchemas:    schemasToDrop,
			DroppedTables:     tableDropDetails,
			DroppedTypes:      typeIDs,
			DroppedDatabaseID: databaseID,
			FormatVersion:     jobspb.DatabaseJobFormatVersion,
		},
		Progress:      jobspb.SchemaChangeProgress{},
		NonCancelable: true,
	}
	newJob, err := p.extendedEvalCtx.QueueJob(ctx, jobRecord)
	if err != nil {
		return err
	}
	log.Infof(ctx, "queued new drop database job %d for database %d", newJob.ID(), databaseID)
	return nil
}

// CreateNonDropDatabaseChangeJob covers all database descriptor updates other
// than dropping the database.
// TODO (lucy): This should ideally look into the set of queued jobs so that we
// don't queue multiple jobs for the same database.
func (p *planner) createNonDropDatabaseChangeJob(
	ctx context.Context, databaseID descpb.ID, jobDesc string,
) error {
	jobRecord := jobs.Record{
		Description: jobDesc,
		Username:    p.User(),
		Details: jobspb.SchemaChangeDetails{
			DescID:        databaseID,
			FormatVersion: jobspb.DatabaseJobFormatVersion,
		},
		Progress:      jobspb.SchemaChangeProgress{},
		NonCancelable: true,
	}
	newJob, err := p.extendedEvalCtx.QueueJob(ctx, jobRecord)
	if err != nil {
		return err
	}
	log.Infof(ctx, "queued new database schema change job %d for database %d", newJob.ID(), databaseID)
	return nil
}

// createOrUpdateSchemaChangeJob queues a new job for the schema change if there
// is no existing schema change job for the table, or updates the existing job
// if there is one.
func (p *planner) createOrUpdateSchemaChangeJob(
	ctx context.Context, tableDesc *tabledesc.Mutable, jobDesc string, mutationID descpb.MutationID,
) error {
	if tableDesc.GetDeclarativeSchemaChangerState() != nil {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot perform a schema change on table %q while it is undergoing a declarative schema change",
			// We use the cluster version because the table may have been renamed.
			// This is a bit of a hack.
			tableDesc.ClusterVersion.GetName(),
		)
	}

	record, recordExists := p.extendedEvalCtx.SchemaChangeJobRecords[tableDesc.ID]
	if p.extendedEvalCtx.ExecCfg.TestingKnobs.RunAfterSCJobsCacheLookup != nil {
		p.extendedEvalCtx.ExecCfg.TestingKnobs.RunAfterSCJobsCacheLookup(record)
	}

	var spanList []jobspb.ResumeSpanList
	if recordExists {
		spanList = record.Details.(jobspb.SchemaChangeDetails).ResumeSpanList
		prefix := p.ExecCfg().Codec.TenantPrefix()
		for i := range spanList {
			for j := range spanList[i].ResumeSpans {
				sp, err := keys.RewriteSpanToTenantPrefix(spanList[i].ResumeSpans[j], prefix)
				if err != nil {
					return err
				}
				spanList[i].ResumeSpans[j] = sp
			}
		}
	}
	span := tableDesc.PrimaryIndexSpan(p.ExecCfg().Codec)
	for i := len(tableDesc.ClusterVersion.Mutations) + len(spanList); i < len(tableDesc.Mutations); i++ {
		var resumeSpans []roachpb.Span
		mut := tableDesc.Mutations[i]
		if mut.GetIndex() != nil && mut.GetIndex().UseDeletePreservingEncoding {
			// Resume spans for merging the delete preserving temporary indexes are
			// the spans of the temporary indexes.
			resumeSpans = []roachpb.Span{tableDesc.IndexSpan(p.ExecCfg().Codec, mut.GetIndex().ID)}
		} else {
			resumeSpans = []roachpb.Span{span}
		}
		spanList = append(spanList, jobspb.ResumeSpanList{
			ResumeSpans: resumeSpans,
		})
	}

	if !recordExists {
		// Queue a new job.
		newRecord := jobs.Record{
			JobID:         p.extendedEvalCtx.ExecCfg.JobRegistry.MakeJobID(),
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: descpb.IDs{tableDesc.GetID()},
			Details: jobspb.SchemaChangeDetails{
				DescID:          tableDesc.ID,
				TableMutationID: mutationID,
				ResumeSpanList:  spanList,
				// The version distinction for database jobs doesn't matter for jobs on
				// tables.
				FormatVersion: jobspb.DatabaseJobFormatVersion,
			},
			Progress: jobspb.SchemaChangeProgress{},
			// Mark jobs without a mutation ID as non-cancellable,
			// since we expect these to be trivial.
			//
			// The job should be cancelable when we are adding a table that doesn't
			// have mutations, e.g., in CREATE TABLE AS VALUES.
			NonCancelable: mutationID == descpb.InvalidMutationID && !tableDesc.Adding(),
		}
		p.extendedEvalCtx.SchemaChangeJobRecords[tableDesc.ID] = &newRecord
		// Only add a MutationJob if there's an associated mutation.
		// TODO (lucy): get rid of this when we get rid of MutationJobs.
		if mutationID != descpb.InvalidMutationID {
			tableDesc.MutationJobs = append(tableDesc.MutationJobs, descpb.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: newRecord.JobID})
		}
		log.Infof(ctx, "queued new schema-change job %d for table %d, mutation %d",
			newRecord.JobID, tableDesc.ID, mutationID)
		return nil
	}

	// Update the existing job.
	oldDetails := record.Details.(jobspb.SchemaChangeDetails)
	newDetails := jobspb.SchemaChangeDetails{
		DescID:          tableDesc.ID,
		TableMutationID: oldDetails.TableMutationID,
		ResumeSpanList:  spanList,
		// The version distinction for database jobs doesn't matter for jobs on
		// tables.
		FormatVersion: jobspb.DatabaseJobFormatVersion,
	}
	if oldDetails.TableMutationID != descpb.InvalidMutationID {
		// The previous queued schema change job was associated with a mutation,
		// which must have the same mutation ID as this schema change, so just
		// check for consistency.
		if mutationID != descpb.InvalidMutationID && mutationID != oldDetails.TableMutationID {
			return errors.AssertionFailedf(
				"attempted to update job for mutation %d, but job already exists with mutation %d",
				mutationID, oldDetails.TableMutationID)
		}
	} else {
		// The previous queued schema change job didn't have a mutation.
		if mutationID != descpb.InvalidMutationID {
			newDetails.TableMutationID = mutationID
			// Also add a MutationJob on the table descriptor.
			// TODO (lucy): get rid of this when we get rid of MutationJobs.
			tableDesc.MutationJobs = append(tableDesc.MutationJobs, descpb.TableDescriptor_MutationJob{
				MutationID: mutationID, JobID: record.JobID})
			// For existing records, if a mutation ID ever gets assigned
			// at a later point then mark it as cancellable again.
			record.NonCancelable = false
		}
	}
	record.Details = newDetails
	record.AppendDescription(jobDesc)
	log.Infof(ctx, "job %d: updated with schema change for table %d, mutation %d",
		record.JobID, tableDesc.ID, mutationID)
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
	ctx context.Context, tableDesc *tabledesc.Mutable, mutationID descpb.MutationID, jobDesc string,
) error {
	if !p.EvalContext().TxnImplicit {
		telemetry.Inc(sqltelemetry.SchemaChangeInExplicitTxnCounter)
	}
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return errors.Errorf("no schema changes allowed on table %q as it is being dropped",
			tableDesc.Name)
	}
	if !tableDesc.IsNew() {
		if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, mutationID); err != nil {
			return err
		}
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *planner) writeSchemaChangeToBatch(
	ctx context.Context, tableDesc *tabledesc.Mutable, b *kv.Batch,
) error {
	if !p.EvalContext().TxnImplicit {
		telemetry.Inc(sqltelemetry.SchemaChangeInExplicitTxnCounter)
	}
	if tableDesc.Dropped() {
		// We don't allow schema changes on a dropped table.
		return errors.Errorf("no schema changes allowed on table %q as it is being dropped",
			tableDesc.Name)
	}
	return p.writeTableDescToBatch(ctx, tableDesc, b)
}

func (p *planner) writeDropTable(
	ctx context.Context, tableDesc *tabledesc.Mutable, queueJob bool, jobDesc string,
) error {
	if queueJob {
		if err := p.createOrUpdateSchemaChangeJob(ctx, tableDesc, jobDesc, descpb.InvalidMutationID); err != nil {
			return err
		}
	}
	return p.writeTableDesc(ctx, tableDesc)
}

func (p *planner) writeTableDesc(ctx context.Context, tableDesc *tabledesc.Mutable) error {
	b := p.txn.NewBatch()
	if err := p.writeTableDescToBatch(ctx, tableDesc, b); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeTableDescToBatch(
	ctx context.Context, tableDesc *tabledesc.Mutable, b *kv.Batch,
) error {
	if tableDesc.IsVirtualTable() {
		return errors.AssertionFailedf("virtual descriptors cannot be stored, found: %v", tableDesc)
	}

	if tableDesc.IsNew() {
		if err := runSchemaChangesInTxn(
			ctx, p, tableDesc, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		); err != nil {
			return err
		}
	}

	version := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	if err := descbuilder.ValidateSelf(tableDesc, version); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "table descriptor is not valid\n%v\n", tableDesc)
	}

	return p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), tableDesc, b,
	)
}
