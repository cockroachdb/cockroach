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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

//
// This file contains routines for low-level access to stored database
// descriptors, as well as accessors for the database cache.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

func (p *planner) renameDatabase(
	ctx context.Context, desc *sqlbase.MutableDatabaseDescriptor, newName string,
) error {
	oldName := desc.GetName()
	desc.SetName(newName)

	if exists, _, err := catalogkv.LookupDatabaseID(ctx, p.txn, p.ExecCfg().Codec, newName); err == nil && exists {
		return pgerror.Newf(pgcode.DuplicateDatabase,
			"the new database name %q already exists", newName)
	} else if err != nil {
		return err
	}

	// Add the new namespace entry.
	newKey := catalogkv.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, newName).Key(p.ExecCfg().Codec)
	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, desc.ID)
	}
	b.CPut(newKey, desc.ID, nil)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Add the old name to the draining names on the database descriptor.
	renameDetails := descpb.NameInfo{
		ParentID:       keys.RootNamespaceID,
		ParentSchemaID: keys.RootNamespaceID,
		Name:           oldName,
	}
	desc.DrainingNames = append(desc.DrainingNames, renameDetails)

	if err := p.createDropDatabaseJob(
		// TODO (lucy): !!! update the job description from the AST node
		ctx, desc.GetID(), nil /* DroppedTableDetails */, nil /* typesToDrop */, "rename database",
	); err != nil {
		return err
	}
	return p.writeDatabaseChange(ctx, desc)
}

// writeDatabaseChange writes a MutableDatabaseDescriptor's changes to the
// store. Unlike with tables, callers are responsible for queuing the
// accompanying job.
func (p *planner) writeDatabaseChange(
	ctx context.Context, desc *sqlbase.MutableDatabaseDescriptor,
) error {
	desc.MaybeIncrementVersion()
	if err := desc.Validate(); err != nil {
		return err
	}
	if err := p.Descriptors().AddUncommittedDescriptor(desc); err != nil {
		return err
	}
	b := p.txn.NewBatch()
	if err := catalogkv.WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.ExecCfg().Settings,
		b,
		p.ExecCfg().Codec,
		desc.ID,
		desc,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}
