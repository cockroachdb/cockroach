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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	oldName := desc.GetName()
	desc.SetName(newName)

	if exists, _, err := catalogkv.LookupDatabaseID(ctx, p.txn, p.ExecCfg().Codec, newName); err == nil && exists {
		return pgerror.Newf(pgcode.DuplicateDatabase,
			"the new database name %q already exists", newName)
	} else if err != nil {
		return err
	}

	b := &kv.Batch{}
	newKey := catalogkeys.MakeDatabaseNameKey(p.ExecCfg().Codec, newName)
	descID := desc.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
	}
	b.CPut(newKey, descID, nil)

	desc.DrainingNames = append(desc.DrainingNames, descpb.NameInfo{
		ParentID:       keys.RootNamespaceID,
		ParentSchemaID: keys.RootNamespaceID,
		Name:           oldName,
	})
	if err := p.writeNonDropDatabaseChange(ctx, desc, stmt); err != nil {
		return err
	}

	return p.txn.Run(ctx, b)
}

// writeNonDropDatabaseChange writes an updated database descriptor, and can
// only be called when database descriptor leasing is enabled. See
// writeDatabaseChangeToBatch. Also queues a job to complete the schema change.
func (p *planner) writeNonDropDatabaseChange(
	ctx context.Context, desc *dbdesc.Mutable, jobDesc string,
) error {
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
	allDescs, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
	if err != nil {
		return err
	}

	lCtx := newInternalLookupCtx(ctx, allDescs, dbDesc, nil /* fallback */)
	for _, tbID := range lCtx.tbIDs {
		desc := lCtx.tbDescs[tbID]
		if desc.Dropped() {
			continue
		}
		mutable := tabledesc.NewBuilder(desc.TableDesc()).BuildExistingMutableTable()
		if err := fn(ctx, lCtx.schemaNames[desc.GetParentSchemaID()], mutable); err != nil {
			return err
		}
	}
	return nil
}
