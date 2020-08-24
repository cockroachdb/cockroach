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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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
	newKey := catalogkv.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, newName).Key(p.ExecCfg().Codec)
	descID := desc.GetID()
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
	}
	b.CPut(newKey, descID, nil)

	if p.Descriptors().DatabaseLeasingUnsupported() {
		descKey := catalogkeys.MakeDescMetadataKey(p.ExecCfg().Codec, descID)
		descDesc := desc.DescriptorProto()

		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
		}
		b.Put(descKey, descDesc)
		err := catalogkv.RemoveDatabaseNamespaceEntry(
			ctx, p.txn, p.ExecCfg().Codec, oldName, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		)
		if err != nil {
			return err
		}

		p.Descriptors().AddUncommittedDatabaseDeprecated(oldName, descID, descs.DBDropped)
		p.Descriptors().AddUncommittedDatabaseDeprecated(newName, descID, descs.DBCreated)
	} else {
		desc.DrainingNames = append(desc.DrainingNames, descpb.NameInfo{
			ParentID:       keys.RootNamespaceID,
			ParentSchemaID: keys.RootNamespaceID,
			Name:           oldName,
		})
		if err := p.writeNonDropDatabaseChange(ctx, desc, stmt); err != nil {
			return err
		}
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
	if p.Descriptors().DatabaseLeasingUnsupported() {
		log.Fatal(ctx, "invalid attempted write of database descriptor")
	}
	desc.MaybeIncrementVersion()
	if err := desc.Validate(); err != nil {
		return err
	}
	if err := p.Descriptors().AddUncommittedDescriptor(desc); err != nil {
		return err
	}
	return catalogkv.WriteDescToBatch(
		ctx,
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		p.ExecCfg().Settings,
		b,
		p.ExecCfg().Codec,
		desc.ID,
		desc,
	)
}
