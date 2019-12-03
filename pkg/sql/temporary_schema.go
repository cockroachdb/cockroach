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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
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
	loIdx := strings.LastIndex(scName, "_")
	lo, err := strconv.ParseUint(scName[loIdx:], 10, 64)
	if err != nil {
		return false, ClusterWideID{}, err
	}
	hiIdx := strings.LastIndex(scName[:loIdx], "_")
	hi, err := strconv.ParseUint(scName[hiIdx:loIdx], 10, 64)
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

// sessionDataToAccessTempSchemaObjects returns SessionData that can be
// used to access the temporary schema and objects under that temporary schema,
// for the given SessionID. This should only be used when creating executors
// for cleanup.
func sessionDataToAccessTempSchemaObjects(sessionID ClusterWideID) sessiondata.SessionData {
	tempSchemaName := temporarySchemaName(sessionID)
	return sessiondata.SessionData{
		SearchPath:    sqlbase.DefaultSearchPath.WithTemporarySchemaName(tempSchemaName),
		User:          security.RootUser,
		SequenceState: &sessiondata.SequenceState{},
	}
}

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(
	ctx context.Context, db *client.DB, ie sqlutil.InternalExecutor, sessionID ClusterWideID,
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
			tbNames, err := getTemporaryObjectNames(ctx, txn, id, tempSchemaName)
			if err != nil {
				return err
			}
			// TODO(andrei): We might want to accelerate the deletion of this data.
			var query strings.Builder
			query.WriteString("DROP TABLE")
			for i, tbName := range tbNames {
				if i != 0 {
					query.WriteString(",")
				}
				query.WriteString(
					fmt.Sprintf(" %s.%s.%s", tbName.CatalogName, tbName.SchemaName, tbName.Table()),
				)
			}
			if len(tbNames) > 0 {
				_, err = ie.Exec(ctx, "delete-temp-tables", txn, query.String())
				if err != nil {
					return err
				}
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
