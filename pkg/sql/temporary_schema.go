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

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(ctx context.Context, server *Server, sessionID ClusterWideID) error {
	tempSchemaName := temporarySchemaName(sessionID)
	var queuedSchemaChanges *schemaChangerCollection
	return server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		p, cleanup := newInternalPlanner(
			"delete-temp-tables", txn, security.RootUser, &MemoryMetrics{}, server.GetExecutorConfig(),
		)
		defer cleanup()
		// The temporary schema needs to be set on the planner, as the planner is
		// responsible for ensuring temporary table descriptors can  only be
		// accessed by the session that created them.
		p.SetTemporarySchemaName(tempSchemaName)
		queuedSchemaChanges = p.ExtendedEvalContext().SchemaChangers
		dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, p.txn)
		if err != nil {
			return err
		}
		// We are going to read all database descriptor IDs, then for each database
		// we are going to list all the tables under the temporary schema and drop
		// them using a single DROP TABLE command. By dropping all the tables in
		// a single query, we ensure that the drop happens in a single transaction.
		for _, id := range dbIDs {
			dbDesc, err := MustGetDatabaseDescByID(ctx, p.txn, id)
			if err != nil {
				return err
			}
			tbNames, err := GetObjectNames(
				ctx, p.txn, p, dbDesc, tempSchemaName, true /* explicitPrefix*/, false, /* required */
			)
			if err != nil {
				return err
			}
			var query string
			for i, tbName := range tbNames {
				if i == 0 {
					query = fmt.Sprintf(
						"DROP TABLE %s.%s.%s", tbName.CatalogName, tbName.SchemaName, tbName.Table(),
					)
				} else {
					query = fmt.Sprintf(
						"%s, %s.%s.%s", query, tbName.CatalogName, tbName.SchemaName, tbName.Table(),
					)
				}
			}
			if query != "" {
				ie := NewSessionBoundInternalExecutor(
					ctx, p.SessionData(), server, MemoryMetrics{}, server.cfg.Settings,
				)
				// nil transaction ensures that the internal executor runs the schema
				// change, which drains names and deletes the data.
				_, err = ie.Exec(ctx, "delete-temp_tables", nil /* txn */, query)
				if err != nil {
					return err
				}
			}
			// Even if no query was constructed, the temporary schema may exist (eg.
			// a temporary table was created and then dropped). So we still try to
			// remove the namespace table entry for the temporary schema.
			if err := sqlbase.RemoveSchemaNamespaceEntry(ctx, p.txn, id, tempSchemaName); err != nil {
				return err
			}
		}
		return nil
	})
}
