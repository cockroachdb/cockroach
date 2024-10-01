// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func schemaExists(
	ctx context.Context, txn *kv.Txn, col *descs.Collection, parentID descpb.ID, schema string,
) (bool, descpb.ID, error) {
	// Check statically known schemas.
	if schema == catconstants.PublicSchemaName {
		return true, descpb.InvalidID, nil
	}
	for _, s := range virtualSchemas {
		if s.name == schema {
			return true, descpb.InvalidID, nil
		}
	}
	// Now lookup in the namespace for other schemas.
	schemaID, err := col.LookupSchemaID(ctx, txn, parentID, schema)
	if err != nil || schemaID == descpb.InvalidID {
		return false, descpb.InvalidID, err
	}
	return true, schemaID, nil
}

func (p *planner) writeSchemaDesc(ctx context.Context, desc *schemadesc.Mutable) error {
	b := p.txn.NewBatch()
	if err := p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), desc, b,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeSchemaDescChange(
	ctx context.Context, desc *schemadesc.Mutable, jobDesc string,
) error {
	// Exit early with an error if the schema is undergoing a declarative schema
	// change.
	if catalog.HasConcurrentDeclarativeSchemaChange(desc) {
		return scerrors.ConcurrentSchemaChangeError(desc)
	}
	record, recordExists := p.extendedEvalCtx.jobs.uniqueToCreate[desc.ID]
	if recordExists {
		// Update it.
		record.AppendDescription(jobDesc)
		log.Infof(ctx, "job %d: updated job's specification for change on schema %d", record.JobID, desc.ID)
	} else {
		// Or, create a new job.
		jobRecord := jobs.Record{
			JobID:         p.extendedEvalCtx.ExecCfg.JobRegistry.MakeJobID(),
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: descpb.IDs{desc.ID},
			Details: jobspb.SchemaChangeDetails{
				DescID: desc.ID,
				// The version distinction for database jobs doesn't matter for schema
				// jobs.
				FormatVersion: jobspb.DatabaseJobFormatVersion,
				SessionData:   &p.SessionData().SessionData,
			},
			Progress:      jobspb.SchemaChangeProgress{},
			NonCancelable: true,
		}
		p.extendedEvalCtx.jobs.uniqueToCreate[desc.ID] = &jobRecord
		log.Infof(ctx, "queued new schema change job %d for schema %d", jobRecord.JobID, desc.ID)
	}

	return p.writeSchemaDesc(ctx, desc)
}
