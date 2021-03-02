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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func schemaExists(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, parentID descpb.ID, schema string,
) (bool, descpb.ID, error) {
	// Check statically known schemas.
	if schema == tree.PublicSchema {
		return true, descpb.InvalidID, nil
	}
	for _, s := range virtualSchemas {
		if s.name == schema {
			return true, descpb.InvalidID, nil
		}
	}
	// Now lookup in the namespace for other schemas.
	exists, schemaID, err := catalogkv.LookupObjectID(ctx, txn, codec, parentID, keys.RootNamespaceID, schema)
	if err != nil {
		return false, descpb.InvalidID, err
	}
	return exists, schemaID, nil
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
	job, jobExists := p.extendedEvalCtx.SchemaChangeJobCache[desc.ID]
	if jobExists {
		// Update it.
		if err := job.SetDescription(ctx, p.txn,
			func(ctx context.Context, desc string) (string, error) {
				return desc + "; " + jobDesc, nil
			},
		); err != nil {
			return err
		}
		log.Infof(ctx, "job %d: updated with for change on schema %d", job.ID(), desc.ID)
	} else {
		// Or, create a new job.
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: descpb.IDs{desc.ID},
			Details: jobspb.SchemaChangeDetails{
				DescID: desc.ID,
				// The version distinction for database jobs doesn't matter for schema
				// jobs.
				FormatVersion: jobspb.DatabaseJobFormatVersion,
			},
			Progress:      jobspb.SchemaChangeProgress{},
			NonCancelable: true,
		}
		newJob, err := p.extendedEvalCtx.QueueJob(ctx, jobRecord)
		if err != nil {
			return err
		}
		log.Infof(ctx, "queued new schema change job %d for schema %d", newJob.ID(), desc.ID)
	}

	return p.writeSchemaDesc(ctx, desc)
}
