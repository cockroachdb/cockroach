// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/errors"
)

//
// This file contains routines for low-level access to stored database
// descriptors, as well as accessors for the database cache.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

// renameDatabase implements the DatabaseDescEditor interface.
func (p *planner) renameDatabase(
	ctx context.Context, desc *dbdesc.Mutable, newName string, stmt string,
) error {
	oldNameKey := descpb.NameInfo{
		ParentID:       desc.GetParentID(),
		ParentSchemaID: desc.GetParentSchemaID(),
		Name:           desc.GetName(),
	}

	// Check that the new name is available.
	if dbID, err := p.Descriptors().LookupDatabaseID(ctx, p.txn, newName); err == nil && dbID != descpb.InvalidID {
		return pgerror.Newf(pgcode.DuplicateDatabase,
			"the new database name %q already exists", newName)
	} else if err != nil {
		return err
	}

	// Update the descriptor with the new name.
	desc.SetName(newName)

	// Populate the namespace update batch.
	b := p.txn.NewBatch()
	if err := p.renameNamespaceEntry(ctx, b, oldNameKey, desc); err != nil {
		return err
	}

	// Write the updated database descriptor.
	if err := p.writeNonDropDatabaseChange(ctx, desc, stmt); err != nil {
		return err
	}

	// Run the namespace update batch.
	return p.txn.Run(ctx, b)
}

// writeNonDropDatabaseChange writes an updated database descriptor, and can
// only be called when database descriptor leasing is enabled. See
// writeDatabaseChangeToBatch. Also queues a job to complete the schema change.
func (p *planner) writeNonDropDatabaseChange(
	ctx context.Context, desc *dbdesc.Mutable, jobDesc string,
) error {
	// Exit early with an error if the table is undergoing a declarative schema
	// change.
	if catalog.HasConcurrentDeclarativeSchemaChange(desc) {
		return scerrors.ConcurrentSchemaChangeError(desc)
	}
	if err := p.createNonDropDatabaseChangeJob(ctx, desc.ID, jobDesc); err != nil {
		return err
	}
	b := p.Txn().NewBatch()
	if err := p.writeDatabaseChangeToBatch(ctx, desc, b); err != nil {
		return err
	}
	return p.Txn().Run(ctx, b)
}

// writeDatabaseChangeToBatch writes an updated database descriptor, and
// can only be called when database descriptor leasing is enabled. Does not
// queue a job to complete the schema change.
func (p *planner) writeDatabaseChangeToBatch(
	ctx context.Context, desc *dbdesc.Mutable, b *kv.Batch,
) error {
	return p.Descriptors().WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		desc,
		b,
	)
}

// forEachMutableTableInDatabase calls the given function on every table
// descriptor inside the given database. Tables that have been
// dropped are skipped.
func (p *planner) forEachMutableTableInDatabase(
	ctx context.Context,
	dbDesc catalog.DatabaseDescriptor,
	fn func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error,
) error {
	all, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}

	// TODO(ajwerner): Rewrite this to not use the internalLookupCtx.
	lCtx := newInternalLookupCtx(all.OrderedDescriptors(), dbDesc)
	var droppedRemoved []descpb.ID
	for _, tbID := range lCtx.tbIDs {
		desc := lCtx.tbDescs[tbID]
		if desc.Dropped() {
			continue
		}
		droppedRemoved = append(droppedRemoved, tbID)
	}
	descs, err := p.Descriptors().MutableByID(p.Txn()).Descs(ctx, droppedRemoved)
	if err != nil {
		return err
	}
	for _, d := range descs {
		mutable := d.(*tabledesc.Mutable)
		schemaName, found, err := lCtx.GetSchemaName(ctx, d.GetParentSchemaID(), d.GetParentID(), p.ExecCfg().Settings.Version)
		if err != nil {
			return err
		}
		if !found {
			return errors.AssertionFailedf("schema id %d not found", d.GetParentSchemaID())
		}
		if err := fn(ctx, schemaName, mutable); err != nil {
			return err
		}
	}
	return nil
}
