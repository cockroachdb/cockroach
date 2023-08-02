// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
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
