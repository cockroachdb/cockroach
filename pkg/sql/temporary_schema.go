// Copyright 2019 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func createTempSchema(params runParams, sKey sqlbase.DescriptorKey) (sqlbase.ID, error) {
	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if err := params.p.createSchemaWithID(params.ctx, sKey.Key(), id); err != nil {
		return sqlbase.InvalidID, err
	}

	params.p.SetTemporarySchemaName(sKey.Name())

	return id, nil
}

func (p *planner) createSchemaWithID(
	ctx context.Context, schemaNameKey roachpb.Key, schemaID sqlbase.ID,
) error {
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", schemaNameKey, schemaID)
	}

	b := &client.Batch{}
	b.CPut(schemaNameKey, schemaID, nil)

	return p.txn.Run(ctx, b)
}

func temporarySchemaName(sessionID ClusterWideID) string {
	return fmt.Sprintf("pg_temp_%v%v", sessionID.Hi, sessionID.Lo)
}

// Removes all temporary objects (tables, sequences, views) created by the
// session, and returns the queued schema changes that must be  executed to
// remove the actual data. The caller must execute the queued schema changes
// even if this function returns an error.
func cleanupSessionTempObjects(
	ctx context.Context, p *planner, sessionID ClusterWideID,
) (*schemaChangerCollection, error) {
	tempSchemaName := temporarySchemaName(sessionID)
	if p.sessionDataMutator != nil && p.sessionDataMutator.data.SearchPath.HasCreatedTemporarySchema(tempSchemaName) {
		return nil, nil
	}
	dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, p.txn)
	if err != nil {
		return nil, err
	}
	for _, id := range dbIDs {
		dbDesc, err := p.Tables().databaseCache.getDatabaseDescByID(ctx, p.txn, id)
		if err != nil {
			return nil, err
		}
		tbNames, err := GetObjectNames(ctx, p.txn, p, dbDesc, tempSchemaName, true /* explicitPrefix*/)
		for i := range tbNames {
			tbDesc, err := p.ResolveMutableTableDescriptor(ctx, &tbNames[i], true /* required */, ResolveAnyDescType)
			if err != nil {
				return p.ExtendedEvalContext().SchemaChangers, err
			}
			if _, err := p.dropDesc(ctx, tbDesc, tree.DropCascade); err != nil {
				return p.ExtendedEvalContext().SchemaChangers, err
			}
		}
		// TODO(arul): When there is a schema cache, this should probably go through
		// there, so that the cache entry is removed as well.
		//
		// Finally, also remove the temporary schema from the namespace table
		if err := sqlbase.RemoveSchemaNamespaceEntry(ctx, p.txn, id, tempSchemaName); err != nil {
			return p.ExtendedEvalContext().SchemaChangers, err
		}
	}
	return p.ExtendedEvalContext().SchemaChangers, nil
}
