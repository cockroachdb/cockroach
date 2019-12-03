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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
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

// temporarySchemaSessionID returns the sessionID of the given temporary schema.
func temporarySchemaSessionID(scName string) (bool, ClusterWideID, error) {
	if !strings.HasPrefix(scName, "pg_temp_") {
		return false, ClusterWideID{}, nil
	}
	parts := strings.Split(scName, "_")
	if len(parts) != 4 {
		return false, ClusterWideID{}, errors.Errorf("malformed temp schema name %s", scName)
	}
	hi, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return false, ClusterWideID{}, err
	}
	lo, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return false, ClusterWideID{}, err
	}
	return true, ClusterWideID{uint128.Uint128{Hi: hi, Lo: lo}}, nil
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

// sessionDataForCleanup returns SessionData that can be used to access the
// temporary schema for the given SessionID. This is used for cleaning up
// temporary data when the session no longer exists.
func sessionDataForCleanup(sessionID ClusterWideID) *sessiondata.SessionData {
	tempSchemaName := temporarySchemaName(sessionID)
	return &sessiondata.SessionData{
		SearchPath:    sqlbase.DefaultSearchPath.WithTemporarySchemaName(tempSchemaName),
		User:          security.RootUser,
		SequenceState: &sessiondata.SequenceState{},
	}
}

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(
	ctx context.Context,
	settings *cluster.Settings,
	db *client.DB,
	execQuery func(context.Context, string, *client.Txn, string, ...interface{}) (int, error),
	sessionID ClusterWideID,
) error {
	tempSchemaName := temporarySchemaName(sessionID)
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// We are going to read all database descriptor IDs, then for each database
		// we will drop all the objects under the temporary schema.
		dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, txn)
		if err != nil {
			return err
		}
		for _, id := range dbIDs {
			if err := cleanupSchemaObjects(ctx, settings, execQuery, txn, id, tempSchemaName); err != nil {
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
	var tables sqlbase.IDs
	var views sqlbase.IDs
	var sequences sqlbase.IDs

	descsByID := make(map[sqlbase.ID]*TableDescriptor, len(tbNames))
	tblNamesByID := make(map[sqlbase.ID]tree.TableName, len(tbNames))
	for _, tbName := range tbNames {
		objDesc, err := a.GetObjectDesc(
			ctx,
			txn,
			settings,
			&tbName,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		desc := objDesc.TableDesc()

		descsByID[desc.ID] = desc
		tblNamesByID[desc.ID] = tbName

		if desc.SequenceOpts != nil {
			sequences = append(sequences, desc.ID)
		} else if desc.ViewQuery != "" {
			views = append(views, desc.ID)
		} else {
			tables = append(tables, desc.ID)
		}
	}

	for _, toDelete := range []struct {
		// typeName is the type of table being deleted, e.g. view, table, sequence
		typeName string
		// ids represents which ids we wish to remove.
		ids sqlbase.IDs
		// preHook is used to perform any operations needed before calling
		// delete on all the given ids.
		preHook func(sqlbase.ID) error
	}{
		// Drop views before tables to avoid deleting required dependencies.
		{"VIEW", views, nil},
		{"TABLE", tables, nil},
		// Drop sequences after tables, because then we reduce the amount of work
		// that may be needed to drop indices.
		{
			"SEQUENCE",
			sequences,
			func(id sqlbase.ID) error {
				desc := descsByID[id]
				// For any dependent tables, we need to drop the sequence dependencies.
				// This can happen if a permanent table references a temporary table.
				for _, d := range desc.DependedOnBy {
					// We have already cleaned out anything we are depended on if we've seen
					// the descriptor already.
					if _, ok := descsByID[d.ID]; ok {
						continue
					}
					dTableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, d.ID)
					if err != nil {
						return err
					}
					db, err := sqlbase.GetDatabaseDescFromID(ctx, txn, dTableDesc.GetParentID())
					if err != nil {
						return err
					}
					schema, err := schema.ResolveNameByID(
						ctx,
						txn,
						dTableDesc.GetParentID(),
						dTableDesc.GetParentSchemaID(),
					)
					if err != nil {
						return err
					}
					dependentColIDs := util.MakeFastIntSet()
					for _, colID := range d.ColumnIDs {
						dependentColIDs.Add(int(colID))
					}
					for _, col := range dTableDesc.Columns {
						if dependentColIDs.Contains(int(col.ID)) {
							tbName := tree.MakeTableNameWithSchema(
								tree.Name(db.Name),
								tree.Name(schema),
								tree.Name(dTableDesc.Name),
							)
							_, err = execQuery(
								ctx,
								"delete-temp-dependent-col",
								txn,
								fmt.Sprintf(
									"ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
									tbName.FQString(),
									tree.NameString(col.Name),
								),
							)
							if err != nil {
								return err
							}
						}
					}
				}
				return nil
			},
		},
	} {
		if len(toDelete.ids) > 0 {
			if toDelete.preHook != nil {
				for _, id := range toDelete.ids {
					if err := toDelete.preHook(id); err != nil {
						return err
					}
				}
			}

			var query strings.Builder
			query.WriteString("DROP ")
			query.WriteString(toDelete.typeName)

			for i, id := range toDelete.ids {
				tbName := tblNamesByID[id]
				if i != 0 {
					query.WriteString(",")
				}
				query.WriteString(" ")
				query.WriteString(tbName.FQString())
			}
			query.WriteString(" CASCADE")
			_, err = execQuery(ctx, "delete-temp-"+toDelete.typeName, txn, query.String())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
