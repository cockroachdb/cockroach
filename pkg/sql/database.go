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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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
	desc.MaybeIncrementVersion()
	desc.SetName(newName)

	if exists, _, err := catalogkv.LookupDatabaseID(ctx, p.txn, p.ExecCfg().Codec, newName); err == nil && exists {
		return pgerror.Newf(pgcode.DuplicateDatabase,
			"the new database name %q already exists", newName)
	} else if err != nil {
		return err
	}

	err := catalogkv.RemoveDatabaseNamespaceEntry(
		ctx, p.txn, p.ExecCfg().Codec, oldName, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	p.Descriptors().AddUncommittedDatabase(
		oldName, desc.GetID(), descs.DBRenamed, desc.OriginalVersion()+1)

	newKey := catalogkv.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, newName).Key(p.ExecCfg().Codec)
	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, desc.GetID())
	}
	b.CPut(newKey, desc.GetID(), nil)
	if err := p.writeDatabaseChangeToBatch(ctx, desc, b); err != nil {
		return err
	}
	log.Infof(ctx, "wrote new database descriptor %s %+v", desc.GetName(), desc)
	return p.txn.Run(ctx, b)
}

func (p *planner) writeDatabaseChangeToBatch(
	ctx context.Context, desc *sqlbase.MutableDatabaseDescriptor, b *kv.Batch,
) error {
	desc.MaybeIncrementVersion()
	if err := desc.Validate(); err != nil {
		return err
	}
	if err := p.Descriptors().AddUncommittedDescriptor(desc); err != nil {
		return err
	}
	p.Descriptors().AddUncommittedDatabase(
		desc.GetName(), desc.GetID(), descs.DBCreated, desc.GetVersion())
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
