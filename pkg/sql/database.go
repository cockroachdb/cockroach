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

// renameDatabase implements the DatabaseDescEditor interface.
func (p *planner) renameDatabase(
	ctx context.Context, oldDesc *sqlbase.ImmutableDatabaseDescriptor, newName string,
) error {
	oldName := oldDesc.GetName()
	newDesc := sqlbase.NewMutableExistingDatabaseDescriptor(*oldDesc.DatabaseDesc())
	newDesc.Version++
	newDesc.SetName(newName)
	if err := newDesc.Validate(); err != nil {
		return err
	}

	if exists, _, err := sqlbase.LookupDatabaseID(ctx, p.txn, p.ExecCfg().Codec, newName); err == nil && exists {
		return pgerror.Newf(pgcode.DuplicateDatabase,
			"the new database name %q already exists", newName)
	} else if err != nil {
		return err
	}

	newKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, newName).Key(p.ExecCfg().Codec)

	descID := newDesc.GetID()
	descKey := sqlbase.MakeDescMetadataKey(p.ExecCfg().Codec, descID)
	descDesc := newDesc.DescriptorProto()

	b := &kv.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", newKey, descID)
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.CPut(newKey, descID, nil)
	b.Put(descKey, descDesc)
	err := sqlbase.RemoveDatabaseNamespaceEntry(
		ctx, p.txn, p.ExecCfg().Codec, oldName, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)
	if err != nil {
		return err
	}

	p.Tables().AddUncommittedDatabase(oldName, descID, descs.DBDropped)
	p.Tables().AddUncommittedDatabase(newName, descID, descs.DBCreated)

	return p.txn.Run(ctx, b)
}
