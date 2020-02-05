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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

	params.p.sessionDataMutator.SetTemporarySchemaName(sKey.Name())
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

// temporarySchemaName returns the session specific temporary schema name given
// the sessionID. When the session creates a temporary object for the first
// time, it must create a schema with the name returned by this function.
func temporarySchemaName(sessionID ClusterWideID) string {
	return fmt.Sprintf("pg_temp_%d_%d", sessionID.Hi, sessionID.Lo)
}

// getTemporaryObjectNames returns all the temporary objects under the
// temporary schema of the given dbID.
func getTemporaryObjectNames(
	ctx context.Context, txn *client.Txn, dbID sqlbase.ID, tempSchemaName string,
) (TableNames, error) {
	dbDesc, err := MustGetDatabaseDescByID(ctx, txn, dbID)
	if err != nil {
		return nil, err
	}
	a := UncachedPhysicalAccessor{}
	return a.GetObjectNames(
		ctx,
		txn,
		dbDesc,
		tempSchemaName,
		tree.DatabaseListFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: false}},
	)
}

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(ctx context.Context, server *Server, sessionID ClusterWideID) error {
	tempSchemaName := temporarySchemaName(sessionID)
	sd := &sessiondata.SessionData{
		SearchPath:    sqlbase.DefaultSearchPath.WithTemporarySchemaName(tempSchemaName),
		User:          security.RootUser,
		SequenceState: &sessiondata.SequenceState{},
	}
	ie := MakeInternalExecutor(ctx, server, MemoryMetrics{}, server.cfg.Settings)
	ie.SetSessionData(sd)
	return server.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// We are going to read all database descriptor IDs, then for each database
		// we will drop all the objects under the temporary schema.
		dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, txn)
		if err != nil {
			return err
		}
		for _, id := range dbIDs {
			if err := cleanupSchemaObjects(ctx, server.cfg.Settings, ie.Exec, txn, id, tempSchemaName); err != nil {
				return err
			}
			// Even if no objects were found under the temporary schema, the schema
			// itself may still exist (eg. a temporary table was created and then
			// dropped). So we remove the namespace table entry of the temporary
			// schema.
			if err := sqlbase.RemoveSchemaNamespaceEntry(ctx, txn, id, tempSchemaName); err != nil {
				return err
			}
		}
		return nil
	})
}

// TestingCleanupSchemaObjects is a wrapper around cleanupSchemaObjects, used for testing only.
func TestingCleanupSchemaObjects(
	ctx context.Context,
	settings *cluster.Settings,
	execQuery func(context.Context, string, *client.Txn, string, ...interface{}) (int, error),
	txn *client.Txn,
	dbID sqlbase.ID,
	schemaName string,
) error {
	return cleanupSchemaObjects(ctx, settings, execQuery, txn, dbID, schemaName)
}

// cleanupSchemaObjects removes all objects that is located within a dbID and schema.
func cleanupSchemaObjects(
	ctx context.Context,
	settings *cluster.Settings,
	execQuery func(context.Context, string, *client.Txn, string, ...interface{}) (int, error),
	txn *client.Txn,
	dbID sqlbase.ID,
	schemaName string,
) error {
	tbNames, err := getTemporaryObjectNames(ctx, txn, dbID, schemaName)
	if err != nil {
		return err
	}
	a := UncachedPhysicalAccessor{}

	// TODO(andrei): We might want to accelerate the deletion of this data.
	var tables TableNames
	var views TableNames
	for _, tbName := range tbNames {
		desc, err := a.GetObjectDesc(
			ctx,
			txn,
			settings,
			&tbName,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		if desc.TableDesc().ViewQuery != "" {
			views = append(views, tbName)
		} else {
			tables = append(tables, tbName)
		}
	}

	// Drop views before tables to avoid deleting required dependencies.
	for _, toDelete := range []struct {
		typeName string
		tbNames  TableNames
	}{
		{"VIEW", views},
		{"TABLE", tables},
	} {
		if len(toDelete.tbNames) > 0 {
			var query strings.Builder
			query.WriteString("DROP ")
			query.WriteString(toDelete.typeName)

			for i, tbName := range toDelete.tbNames {
				if i != 0 {
					query.WriteString(",")
				}
				query.WriteString(
					fmt.Sprintf(" %s.%s.%s", tbName.CatalogName, tbName.SchemaName, tbName.Table()),
				)
			}
			_, err = execQuery(ctx, "delete-temp-"+toDelete.typeName, txn, query.String())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
