// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestRestoreResolveOptionsForJobDescription tests that
// resolveOptionsForRestoreJobDescription handles every field in the
// RestoreOptions struct.
func TestRestoreResolveOptionsForJobDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The input struct must have a non-zero value for every
	// element of the struct.
	input := tree.RestoreOptions{
		SkipMissingFKs:                   true,
		SkipMissingSequences:             true,
		SkipMissingSequenceOwners:        true,
		SkipMissingViews:                 true,
		SkipMissingUDFs:                  true,
		Detached:                         true,
		SkipLocalitiesCheck:              true,
		AsTenant:                         tree.NewDString("test expr"),
		ForceTenantID:                    tree.NewDInt(42),
		SchemaOnly:                       true,
		VerifyData:                       true,
		UnsafeRestoreIncompatibleVersion: true,
		ExecutionLocality:                tree.NewDString("test expr"),
		ExperimentalOnline:               true,
		RemoveRegions:                    true,

		IntoDB:               tree.NewDString("test expr"),
		NewDBName:            tree.NewDString("test expr"),
		IncrementalStorage:   []tree.Expr{tree.NewDString("http://example.com")},
		DecryptionKMSURI:     []tree.Expr{tree.NewDString("http://example.com")},
		EncryptionPassphrase: tree.NewDString("test expr"),
	}

	ensureAllStructFieldsSet := func(s tree.RestoreOptions, name string) {
		structType := reflect.TypeOf(s)
		require.Equal(t, reflect.Struct, structType.Kind())

		sv := reflect.ValueOf(s)
		for i := 0; i < sv.NumField(); i++ {
			field := sv.Field(i)
			fieldName := structType.Field(i).Name
			require.True(t, field.IsValid(), "RestoreOptions field %s in %s is not valid", fieldName, name)
			require.False(t, field.IsZero(), "RestoreOptions field %s in %s is not non-zero", fieldName, name)
		}
	}

	ensureAllStructFieldsSet(input, "input")
	output, err := resolveOptionsForRestoreJobDescription(
		context.Background(),
		input,
		"into_db",
		"newDBName",
		[]string{"http://example.com"},
		[]string{"http://example.com"})
	require.NoError(t, err)
	ensureAllStructFieldsSet(output, "output")

}

func TestBackupManifestVersionCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name                    string
		backupVersion           roachpb.Version
		clusterVersion          roachpb.Version
		minimumSupportedVersion roachpb.Version
		expectedError           string
	}

	binaryVersion := roachpb.Version{Major: 23, Minor: 1}
	tests := []testCase{
		{
			name:                    "same-version-restore",
			backupVersion:           roachpb.Version{Major: 23, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
		},
		{
			name:                    "previous-version-restore",
			backupVersion:           roachpb.Version{Major: 23, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
		},
		{
			name:                    "unfinalized-restore",
			backupVersion:           roachpb.Version{Major: 23, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 22, Minor: 2},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "backup from version 23.1 is newer than current version 22.2",
		},
		{
			name:                    "alpha-restore",
			backupVersion:           roachpb.Version{Major: 1000022, Minor: 2, Internal: 14},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "backup from version 1000022.2-upgrading-to-1000023.1-step-014 is newer than current version 23.1",
		},
		{
			name:                    "old-backup",
			backupVersion:           roachpb.Version{Major: 22, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "backup from version 22.1 is older than the minimum restorable version 22.2",
		},
		{
			name:                    "legacy-version-backup",
			backupVersion:           roachpb.Version{},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "the backup is from a version older than our minimum restorable version 22.2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			settings := cluster.MakeTestingClusterSettingsWithVersions(binaryVersion, tc.minimumSupportedVersion, false)
			require.NoError(t, clusterversion.Initialize(context.Background(), tc.clusterVersion, &settings.SV))
			version := clusterversion.MakeVersionHandle(&settings.SV, binaryVersion, tc.minimumSupportedVersion)
			manifest := []backuppb.BackupManifest{{ClusterVersion: tc.backupVersion}}

			err := checkBackupManifestVersionCompatability(context.Background(), version, manifest /*unsafe=*/, false)
			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			}

			require.NoError(t, checkBackupManifestVersionCompatability(context.Background(), version, manifest /*unsafe=*/, true))
		})
	}
}

func TestAllocateDescriptorRewrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	opName := redact.SafeString("allocate-descriptor-rewrites")
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	var defaultDB *dbdesc.Mutable
	var db1 *dbdesc.Mutable
	var db2 *dbdesc.Mutable
	var schema1 *schemadesc.Mutable
	var schema2 *schemadesc.Mutable
	var table1 *tabledesc.Mutable
	var table2 *tabledesc.Mutable
	var type1 *typedesc.Mutable
	var type2 *typedesc.Mutable
	var type1array *typedesc.Mutable
	var type2array *typedesc.Mutable
	var func1 *funcdesc.Mutable
	var func2 *funcdesc.Mutable

	var planner sql.PlanHookState

	srv := s.ApplicationLayer()
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	setupPlanner := func() {
		plannerAsInterface, cleanup := sql.NewInternalPlanner(
			opName,
			srv.DB().NewTxn(ctx, "test-allocate-descriptor-rewrite"),
			username.NodeUserName(),
			&sql.MemoryMetrics{},
			&execCfg,
			sql.NewInternalSessionData(ctx, execCfg.Settings, opName))
		defer cleanup()
		planner = plannerAsInterface.(sql.PlanHookState)
	}

	// This is a fairly expensive call. Individual tests should only call it
	// when they specifically need it for cleanup, e.g. after dropping a
	// database.
	setup := func() {
		query := `
        DROP DATABASE IF EXISTS db1 cascade;
        DROP DATABASE IF EXISTS db2 cascade;
        DROP DATABASE IF EXISTS defaultdb cascade;
        CREATE DATABASE db1;
        CREATE DATABASE db2;
        CREATE DATABASE defaultdb;
        CREATE SCHEMA schema1;
        CREATE SCHEMA schema2;
        CREATE TABLE schema1.table1(id int)
        CREATE TABLE schema1.table2(id int)
        CREATE FUNCTION schema1.func1(a INT, b INT) RETURNS INT IMMUTABLE LEAKPROOF LANGUAGE SQL AS 'SELECT a + b';
        CREATE FUNCTION schema1.func2(a INT, b INT) RETURNS INT IMMUTABLE LEAKPROOF LANGUAGE SQL AS 'SELECT a - b';
        CREATE TYPE schema1.type1 AS (x INT, y INT);
        CREATE TYPE schema1.type2 AS ENUM ('a', 'b');
    `
		for _, cmd := range strings.Split(query, "\n") {
			_, err := db.ExecContext(ctx, cmd)
			require.NoError(t, err)
		}

		setupPlanner()

		txn := planner.InternalSQLTxn()
		col := txn.Descriptors()
		cat, err := col.GetAll(ctx, kvDB.NewTxn(ctx, "test-get-all"))
		require.NoError(t, err)
		sqlDescs := cat.OrderedDescriptors()

		type nameAndType struct {
			name    string
			objType string
		}

		asMutable := func(sqlDesc catalog.Descriptor) catalog.MutableDescriptor {
			return sqlDesc.NewBuilder().BuildExistingMutable()
		}

		for _, sqlDesc := range sqlDescs {
			name := sqlDesc.GetName()
			objType := sqlDesc.GetObjectType()
			nt := nameAndType{name: name, objType: string(objType)}
			switch nt {
			case nameAndType{name: "defaultdb", objType: "database"}:
				defaultDB = asMutable(sqlDesc).(*dbdesc.Mutable)
			case nameAndType{name: "db1", objType: "database"}:
				db1 = asMutable(sqlDesc).(*dbdesc.Mutable)
			case nameAndType{name: "db2", objType: "database"}:
				db2 = asMutable(sqlDesc).(*dbdesc.Mutable)
			case nameAndType{name: "schema1", objType: "schema"}:
				schema1 = asMutable(sqlDesc).(*schemadesc.Mutable)
			case nameAndType{name: "schema2", objType: "schema"}:
				schema2 = asMutable(sqlDesc).(*schemadesc.Mutable)
			case nameAndType{name: "table1", objType: "table"}:
				table1 = asMutable(sqlDesc).(*tabledesc.Mutable)
			case nameAndType{name: "table2", objType: "table"}:
				table2 = asMutable(sqlDesc).(*tabledesc.Mutable)
			case nameAndType{name: "type1", objType: "type"}:
				type1 = asMutable(sqlDesc).(*typedesc.Mutable)
			case nameAndType{name: "type2", objType: "type"}:
				type2 = asMutable(sqlDesc).(*typedesc.Mutable)
			case nameAndType{name: "_type1", objType: "type"}:
				type1array = asMutable(sqlDesc).(*typedesc.Mutable)
			case nameAndType{name: "_type2", objType: "type"}:
				type2array = asMutable(sqlDesc).(*typedesc.Mutable)
			case nameAndType{name: "func1", objType: "routine"}:
				func1 = asMutable(sqlDesc).(*funcdesc.Mutable)
			case nameAndType{name: "func2", objType: "routine"}:
				func2 = asMutable(sqlDesc).(*funcdesc.Mutable)
			}
		}
	}
	setup()

	validateSelfIDs := func(
		rewrites jobspb.DescRewriteMap,
		expected []catalog.Descriptor,
	) error {
		if len(rewrites) != len(expected) {
			return errors.Newf("expected %d rewrites, got %d", len(expected), len(rewrites))
		}
		for _, desc := range expected {
			rewrite, ok := rewrites[desc.GetID()]
			if !ok {
				return errors.Newf("no rewrite found for %v", desc)
			}
			if rewrite.ID == desc.GetID() {
				return errors.Newf("expected new ID for %v", desc)
			}
		}
		return nil
	}

	t.Run("allocateDescriptorRewrite", func(t *testing.T) {
		t.Run("succeeds on empty input", func(t *testing.T) {
			rewrites, err := allocateDescriptorRewrites(
				ctx,
				planner,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				0,
				tree.RestoreOptions{},
				"",
				"")
			require.NoError(t, err)
			require.Equal(t, jobspb.DescRewriteMap{}, rewrites)
		})
		t.Run("allocates into existing db", func(t *testing.T) {
			rewrites, err := allocateDescriptorRewrites(
				ctx,
				planner,
				map[descpb.ID]*dbdesc.Mutable{
					defaultDB.GetID(): defaultDB,
				},
				map[descpb.ID]*schemadesc.Mutable{
					schema1.GetID(): schema1,
					schema2.GetID(): schema2,
				},
				map[descpb.ID]*tabledesc.Mutable{
					table1.GetID(): table1,
					table2.GetID(): table2,
				},
				map[descpb.ID]*typedesc.Mutable{
					type1.GetID():      type1,
					type2.GetID():      type2,
					type1array.GetID(): type1array,
					type2array.GetID(): type2array,
				},
				nil,
				nil,
				0,
				tree.RestoreOptions{},
				db2.GetName(),
				"")
			require.NoError(t, err)

			// DB objects are not reallocated
			require.NoError(t, validateSelfIDs(rewrites, []catalog.Descriptor{
				schema1, schema2, table2, table2, type1, type2, type1array, type2array,
			}))
			// New objects' parent ID points to the ID of the target db (db2).
			for _, rewrite := range rewrites {
				require.Equal(t, db2.GetID(), rewrite.ParentID,
					"expected rewrite to have db2 ID as parentID: %v", rewrite)
			}
			// New schema objects have no parent schema
			for _, obj := range []catalog.Descriptor{
				schema1, schema2,
			} {
				rewrite := rewrites[obj.GetID()]
				require.Equalf(t, descpb.InvalidID, rewrite.ParentSchemaID,
					"expected rewrite to have no parent schema, obj: %v, rewrite: %v", obj, rewrite)
			}
			// New non-schema objects point to the new ID of the new schema
			newSchema1ID := rewrites[schema1.GetID()].ID
			for _, obj := range []catalog.Descriptor{
				table2, table2, type1, type2, type1array, type2array,
			} {
				rewrite := rewrites[obj.GetID()]
				require.Equalf(t, newSchema1ID, rewrite.ParentSchemaID,
					"expected rewrite to have new parent schema, obj: %v, rewrite: %v", obj, rewrite,
				)
			}
		})

		t.Run("allocates into new db", func(t *testing.T) {
			rewrites, err := allocateDescriptorRewrites(
				ctx,
				planner,
				map[descpb.ID]*dbdesc.Mutable{
					defaultDB.GetID(): defaultDB,
				},
				map[descpb.ID]*schemadesc.Mutable{
					schema1.GetID(): schema1,
					schema2.GetID(): schema2,
				},
				map[descpb.ID]*tabledesc.Mutable{
					table1.GetID(): table1,
					table2.GetID(): table2,
				},
				map[descpb.ID]*typedesc.Mutable{
					type1.GetID():      type1,
					type2.GetID():      type2,
					type1array.GetID(): type1array,
					type2array.GetID(): type2array,
				},
				nil,
				[]catalog.DatabaseDescriptor{
					defaultDB,
				},
				0,
				tree.RestoreOptions{},
				"",
				"db3")
			require.NoError(t, err)

			require.NoError(t, validateSelfIDs(rewrites, []catalog.Descriptor{
				defaultDB, schema1, schema2, table2, table2, type1, type2, type1array, type2array,
			}))

			defaultDBID := defaultDB.GetID()
			require.Equal(t, descpb.InvalidID, rewrites[defaultDBID].ParentID)

			db3ID := rewrites[defaultDBID].ID

			// New objects' parent ID points to the ID of the target db (db3).
			for id, rewrite := range rewrites {
				if id == defaultDBID {
					continue
				}
				require.Equal(t, db3ID, rewrite.ParentID,
					"expected rewrite to have db3 ID as parentID: %v", rewrite)
			}
			// New schema objects have no parent schema
			for _, obj := range []catalog.Descriptor{
				schema1, schema2,
			} {
				rewrite := rewrites[obj.GetID()]
				require.Equalf(t, descpb.InvalidID, rewrite.ParentSchemaID,
					"expected rewrite to have no parent schema, obj: %v, rewrite: %v", obj, rewrite)
			}
			// New non-schema objects point to the new ID of the new schema
			newSchema1ID := rewrites[schema1.GetID()].ID
			for _, obj := range []catalog.Descriptor{
				table2, table2, type1, type2, type1array, type2array,
			} {
				rewrite := rewrites[obj.GetID()]
				require.Equalf(t, newSchema1ID, rewrite.ParentSchemaID,
					"expected rewrite to have new parent schema, obj: %v, rewrite: %v", obj, rewrite,
				)
			}
		})

		t.Run("allocates functions into new db", func(t *testing.T) {
			rewrites, err := allocateDescriptorRewrites(
				ctx,
				planner,
				map[descpb.ID]*dbdesc.Mutable{
					defaultDB.GetID(): defaultDB,
				},
				nil,
				nil,
				nil,
				map[descpb.ID]*funcdesc.Mutable{
					func1.GetID(): func1,
					func2.GetID(): func2,
				},
				[]catalog.DatabaseDescriptor{defaultDB},
				0,
				tree.RestoreOptions{},
				"",
				"db3")

			require.NoError(t, err)

			require.NoError(t, validateSelfIDs(rewrites, []catalog.Descriptor{
				defaultDB,
				func1,
				func2,
			}))

			newDBID := rewrites[defaultDB.GetID()].ID
			require.Equal(t, newDBID, rewrites[func1.GetID()].ParentID)
			require.Equal(t, newDBID, rewrites[func2.GetID()].ParentID)
		})

		t.Run("allocates multiple dbs", func(t *testing.T) {
			_, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS defaultdb")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "DROP DATABASE IF EXISTS db1")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "DROP DATABASE IF EXISTS db2")
			require.NoError(t, err)
			defer setup()

			// Get a new plan state after dropping the DB.
			setupPlanner()

			rewrites, err := allocateDescriptorRewrites(
				ctx,
				planner,
				map[descpb.ID]*dbdesc.Mutable{
					defaultDB.GetID(): defaultDB,
					db1.GetID():       db1,
					db2.GetID():       db2,
				},
				map[descpb.ID]*schemadesc.Mutable{
					schema1.GetID(): schema1,
					schema2.GetID(): schema2,
				},
				map[descpb.ID]*tabledesc.Mutable{
					table1.GetID(): table1,
					table2.GetID(): table2,
				},
				map[descpb.ID]*typedesc.Mutable{
					type1.GetID():      type1,
					type2.GetID():      type2,
					type1array.GetID(): type1array,
					type2array.GetID(): type2array,
				},
				map[descpb.ID]*funcdesc.Mutable{
					func1.GetID(): func1,
					func2.GetID(): func2,
				},
				[]catalog.DatabaseDescriptor{defaultDB, db1, db2},
				0,
				tree.RestoreOptions{},
				"",
				"")
			require.NoError(t, err)

			// DB objects are reallocated
			require.NoError(t, validateSelfIDs(rewrites, []catalog.Descriptor{
				defaultDB, db1, db2, schema1, schema2, table2, table2, type1, type2, type1array, type2array, func1, func2,
			}))

			oldDBIDs := []descpb.ID{defaultDB.GetID(), db1.GetID(), db2.GetID()}
			newDefaultDBID := rewrites[defaultDB.GetID()].ID
			for oldID, rewrite := range rewrites {
				if slices.Contains(oldDBIDs, oldID) {
					// This is a DB rewrite and has no parent.
					require.Equal(t, descpb.InvalidID, rewrite.ParentID)
					continue
				}
				// This is an object rewrite and its parent is defaultDB.
				require.Equal(t, newDefaultDBID, rewrite.ParentID,
					"expected rewrite to have new defaultDB ID as parentID: %v", rewrite)
			}
			// New schema objects have no parent schema
			for _, obj := range []catalog.Descriptor{
				schema1, schema2,
			} {
				rewrite := rewrites[obj.GetID()]
				require.Equalf(t, descpb.InvalidID, rewrite.ParentSchemaID,
					"expected rewrite to have no parent schema, descriptor: %v, rewrite: %v", obj, rewrite)
			}
			// New non-schema objects point to the new ID of the new schema
			newSchema1ID := rewrites[schema1.GetID()].ID
			for _, obj := range []catalog.Descriptor{
				table2, table2, type1, type2, type1array, type2array, func1, func2,
			} {
				rewrite := rewrites[obj.GetID()]
				require.Equalf(t, newSchema1ID, rewrite.ParentSchemaID,
					"expected rewrite to have new parent schema, descriptor: %v, rewrite: %v", obj, rewrite,
				)
			}
		})
	})
}
