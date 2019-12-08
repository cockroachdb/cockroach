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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
func cleanupSessionTempObjects(ctx context.Context, server *Server, sessionID ClusterWideID) error {
	tempSchemaName := temporarySchemaName(sessionID)
	var queuedSchemaChanges *schemaChangerCollection
	err := server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		p, cleanup := newInternalPlanner(
			"delete-temp-tables", txn, security.RootUser, &MemoryMetrics{}, server.GetExecutorConfig(),
		)
		defer cleanup()
		// The temporary schema needs to be set on the new internal planner, as
		// the planner is responsible for ensuring temporary table descriptors can
		// only be accessed by the session that created them.
		p.SetTemporarySchemaName(tempSchemaName)
		dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, p.txn)
		if err != nil {
			return err
		}
		for _, id := range dbIDs {
			dbDesc, err := MustGetDatabaseDescByID(ctx, p.txn, id)
			if err != nil {
				return err
			}
			tbNames, err := GetObjectNames(ctx, p.txn, p, dbDesc, tempSchemaName, true /* explicitPrefix*/)
			for i := range tbNames {
				tbDesc, err := p.ResolveMutableTableDescriptor(
					ctx, &tbNames[i], true /* required */, ResolveAnyDescType,
				)
				if err != nil {
					queuedSchemaChanges = p.ExtendedEvalContext().SchemaChangers
					return err
				}
				if droppedViews, err := p.dropDesc(ctx, tbDesc, tree.DropCascade); err != nil {
					if len(droppedViews) != 0 {
						return errors.AssertionFailedf("only temp views can refer to temp tables, " +
							"so no views should be dropped until temp views are supported.")
					}
					queuedSchemaChanges = p.ExtendedEvalContext().SchemaChangers
					return err
				}
			}
			if err := sqlbase.RemoveSchemaNamespaceEntry(ctx, p.txn, id, tempSchemaName); err != nil {
				queuedSchemaChanges = p.ExtendedEvalContext().SchemaChangers
				return err
			}
		}
		queuedSchemaChanges = p.ExtendedEvalContext().SchemaChangers
		return nil
	})
	// Even if there is an error above while deleting the metadata, we must run
	// any schema changes that were queued up before the error. As schema changes
	// run after the transaction that queued them has committed, they must be run
	// in a new transaction.
	return errors.CombineErrors(err, server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		ieFactory := func(ctx context.Context, sd *sessiondata.SessionData) sqlutil.InternalExecutor {
			ie := NewSessionBoundInternalExecutor(
				ctx,
				sd,
				server,
				MemoryMetrics{},
				server.cfg.Settings,
			)
			return ie
		}
		err := queuedSchemaChanges.execSchemaChanges(ctx, server.cfg, &SessionTracing{}, ieFactory)
		if err != nil {
			return err
		}
		return nil
	}))
}
