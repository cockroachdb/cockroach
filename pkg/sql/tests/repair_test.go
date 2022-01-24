// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const (
	defaultDBID             = 50
	defaultDBPublicSchemaID = 51
	invalidTableID          = 54

	firstDatabaseID     = 54
	firstPublicSchemaID = 55
	firstTableID        = 56
)

// TestDescriptorRepairOrphanedDescriptors exercises cases where corruptions to
// the catalog could exist due to bugs and can now be repaired programmatically
// from SQL.
//
// We manually create the corruption by injecting values into namespace and
// descriptor, ensure that the doctor would detect these problems, repair them
// with sql queries, and show that not invalid objects are found.
func TestDescriptorRepairOrphanedDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	setup := func(t *testing.T) (serverutils.TestServerInterface, *gosql.DB, func()) {
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		return s, db, func() {
			s.Stopper().Stop(ctx)
		}
	}
	// The below descriptors were created by performing the following on a
	// 20.1.1 cluster:
	//
	//  SET experimental_serial_normalization = 'sql_sequence';
	//  CREATE DATABASE db;
	//  USE db;
	//  CREATE TABLE foo(i SERIAL PRIMARY KEY);
	//  USE defaultdb;
	//  DROP DATABASE db CASCADE;
	//
	// This, due to #51782, leads to the table remaining public but with no
	// parent database (52).
	const (
		orphanedTable = `0aeb010a03666f6f1835203428013a0042380a016910011a0c08011040180030005014600020002a1d6e65787476616c2827666f6f5f695f736571273a3a3a535452494e472930005036480252440a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a020800800100880100900101980100a20106080012001800a8010060026a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201120a077072696d61727910001a016920012800b80101c20100e80100f2010408001200f801008002009202009a0200b20200b80200c0021d`
	)
	// We want to inject a descriptor that has no parent. This will block
	// backups among other things.
	const (
		parentID  = 52
		schemaID  = 29
		descID    = 53
		tableName = "foo"
	)
	// This test will inject the table and demonstrate
	// that there are problems. It will then repair it by just dropping the
	// descriptor and namespace entry. This would normally be unsafe because
	// it would leave table data around.
	t.Run("orphaned view - 51782", func(t *testing.T) {
		_, db, cleanup := setup(t)
		defer cleanup()

		// Drop database postgres and its public schema to get rid of the
		// namespace entries for 52 and 53.
		if err := crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec("DROP DATABASE postgres CASCADE"); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		require.NoError(t, crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec(
				"SELECT crdb_internal.unsafe_upsert_descriptor($1, decode($2, 'hex'), true);",
				descID, orphanedTable); err != nil {
				return err
			}
			_, err := tx.Exec("SELECT crdb_internal.unsafe_upsert_namespace_entry($1, $2, $3, $4, true);",
				parentID, schemaID, tableName, descID)
			return err
		}))

		// Ideally we should be able to query `crdb_internal.invalid_object` but it
		// does not do enough validation. Instead we'll just observe the issue that
		// the parent descriptor cannot be found.
		_, err := db.Exec(
			"SELECT count(*) FROM \"\".crdb_internal.tables WHERE table_id = $1",
			descID)
		require.Regexp(t, fmt.Sprintf(`pq: relation "foo" \(%d\): referenced database ID %d: referenced descriptor not found`, descID, parentID), err)

		// In this case, we're treating the injected descriptor as having no data
		// so we can clean it up by just deleting the erroneous descriptor and
		// namespace entry that was introduced. In the next case we'll go through
		// the dance of adding back a parent database in order to drop the table.
		require.NoError(t, crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec(
				"SELECT crdb_internal.unsafe_delete_descriptor($1, true);",
				descID); err != nil {
				return err
			}
			_, err := tx.Exec("SELECT crdb_internal.unsafe_delete_namespace_entry($1, $2, $3, $4);",
				parentID, schemaID, tableName, descID)
			return err
		}))

		rows, err := db.Query(
			"SELECT count(*) FROM \"\".crdb_internal.tables WHERE table_id = $1",
			descID)
		require.NoError(t, err)
		rowMat, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		require.EqualValues(t, [][]string{{"0"}}, rowMat)
	})
	// This test will inject the table an demonstrate that there are problems. It
	// will then repair it by injecting a new database descriptor and namespace
	// entry and then demonstrate the problem is resolved.
	t.Run("orphaned table with data - 51782", func(t *testing.T) {
		_, db, cleanup := setup(t)
		defer cleanup()

		// Drop database postgres and its public schema to get rid of the
		// namespace entries for 52 and 53.
		if err := crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec("DROP DATABASE postgres CASCADE"); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		require.NoError(t, crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec(
				"SELECT crdb_internal.unsafe_upsert_descriptor($1, decode($2, 'hex'), true);",
				descID, orphanedTable); err != nil {
				return err
			}
			_, err := tx.Exec("SELECT crdb_internal.unsafe_upsert_namespace_entry($1, $2, $3, $4, true);",
				parentID, schemaID, tableName, descID)
			return err
		}))

		// Ideally we should be able to query `crdb_internal.invalid_objects` but it
		// does not do enough validation. Instead we'll just observe the issue that
		// the parent descriptor cannot be found.
		_, err := db.Exec(
			"SELECT count(*) FROM \"\".crdb_internal.tables WHERE table_id = $1",
			descID)
		require.Regexp(t, fmt.Sprintf(`pq: relation "foo" \(%d\): referenced database ID %d: referenced descriptor not found`, descID, parentID), err)

		// In this case, we're going to inject a parent database
		require.NoError(t, crdb.ExecuteTx(ctx, db, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec(
				"SELECT crdb_internal.unsafe_upsert_descriptor($1, crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', $2))",
				parentID, `{
  "database": {
    "id": 52,
    "name": "to_drop",
    "privileges": {
      "owner_proto": "root",
      "users": [
        {
          "privileges": 2,
          "user_proto": "admin"
        },
        {
          "privileges": 2,
          "user_proto": "root"
        }
      ],
      "version": 1
    },
    "state": "PUBLIC",
    "version": 1
  }
}
`,
			); err != nil {
				return err
			}
			if _, err := tx.Exec(
				"SELECT crdb_internal.unsafe_upsert_namespace_entry($1, $2, $3, $4);",
				0, 0, "to_drop", parentID,
			); err != nil {
				return err
			}
			if _, err := tx.Exec("SELECT crdb_internal.unsafe_upsert_namespace_entry($1, $2, $3, $4);",
				parentID, 0, "public", schemaID); err != nil {
				return err
			}

			// We also need to remove the reference to the sequence.

			if _, err := tx.Exec(`
SELECT crdb_internal.unsafe_upsert_descriptor(
        $1,
        crdb_internal.json_to_pb(
            'cockroach.sql.sqlbase.Descriptor',
            jsonb_set(
                jsonb_set(
                    crdb_internal.pb_to_json(
                        'cockroach.sql.sqlbase.Descriptor',
                        descriptor
                    ),
                    ARRAY['table', 'columns', '0', 'default_expr'],
                    '"unique_rowid()"'
                ),
                ARRAY['table', 'columns', '0', 'usesSequenceIds'],
                '[]'
            )
        ),
        true
       )
  FROM system.descriptor
 WHERE id = $1;`,
				descID); err != nil {
				return err
			}
			return nil
		}))

		{
			rows, err := db.Query(
				fmt.Sprintf("SELECT crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) FROM system.descriptor WHERE id = %d",
					descID))
			require.NoError(t, err)
			mat, err := sqlutils.RowsToStrMatrix(rows)
			require.NoError(t, err)
			fmt.Println(sqlutils.MatrixToStr(mat))
		}

		rows, err := db.Query(
			"SELECT count(*) FROM \"\".crdb_internal.tables WHERE table_id = $1",
			descID)
		require.NoError(t, err)
		rowMat, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		require.EqualValues(t, [][]string{{"1"}}, rowMat)

		_, err = db.Exec("DROP DATABASE to_drop CASCADE")
		require.NoError(t, err)
	})
}

func TestDescriptorRepair(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	setup := func(t *testing.T) (serverutils.TestServerInterface, *gosql.DB, func()) {
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		return s, db, func() {
			s.Stopper().Stop(ctx)
		}
	}
	type eventLogPattern struct {
		typ  string
		info string
	}
	for caseIdx, tc := range []struct {
		before             []string
		op                 string
		expErrRE           string
		expEventLogEntries []eventLogPattern
		after              []string
	}{
		{ // 1
			before: []string{
				`CREATE DATABASE test`,
				`CREATE TABLE test.foo ()`,
			},
			op: upsertRepairNoForce,
			expEventLogEntries: []eventLogPattern{
				{
					typ:  "unsafe_upsert_descriptor",
					info: fmt.Sprintf(`"DescriptorID":%d`, firstTableID),
				},
				{
					typ:  "change_table_privilege",
					info: fmt.Sprintf(`"DescriptorID":%d,"Grantee":"newuser1","GrantedPrivileges":\["ALL"\]`, firstTableID),
				},
				{
					typ:  "change_table_privilege",
					info: fmt.Sprintf(`"DescriptorID":%d,"Grantee":"newuser2","GrantedPrivileges":\["ALL"\]`, firstTableID),
				},
			},
		},
		{ // 2
			before: []string{
				`CREATE DATABASE test`,
				fmt.Sprintf(
					`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true)`,
					firstDatabaseID, firstPublicSchemaID, firstTableID),
				upsertRepairForce,
			},
			op: upsertUpdatePrivileges,
			expEventLogEntries: []eventLogPattern{
				{
					typ:  "alter_table_owner",
					info: fmt.Sprintf(`"DescriptorID":%d,"TableName":"foo","Owner":"admin"`, firstTableID),
				},
				{
					typ: "change_table_privilege",
					info: fmt.Sprintf(`"DescriptorID":%d,"Grantee":"newuser1","GrantedPrivileges":\["DROP"\],"RevokedPrivileges":\["ALL"\]`,
						firstTableID),
				},
				{
					typ:  "change_table_privilege",
					info: fmt.Sprintf(`"DescriptorID":%d,"Grantee":"newuser2","RevokedPrivileges":\["ALL"\]`, firstTableID),
				},
			},
		},
		{ // 3
			before: []string{
				`CREATE SCHEMA foo`,
			},
			op: `
SELECT crdb_internal.unsafe_delete_namespace_entry("parentID", 0, 'foo', id)
  FROM system.namespace WHERE name = 'foo';
`,
			expErrRE: `crdb_internal.unsafe_delete_namespace_entry\(\): refusing to delete namespace entry for non-dropped descriptor`,
		},
		{ // 4
			// Upsert a descriptor which is invalid, then try to upsert a namespace
			// entry for it and show that it fails.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
			},
			op: fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d);`,
				defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			expErrRE: fmt.Sprintf(`relation "foo" \(%d\): duplicate column name: "i"`, invalidTableID),
		},
		{ // 5
			// Upsert a descriptor which is invalid, then try to upsert a namespace
			// entry for it and show that it succeeds with the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
			},
			op: fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
				defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			expEventLogEntries: []eventLogPattern{
				{
					typ:  "unsafe_upsert_namespace_entry",
					info: `"Force":true,"FailedValidation":true,"ValidationErrors":".*duplicate column name: \\"i\\""`,
				},
			},
		},
		{ // 6
			// Upsert a descriptor which is invalid, upsert a namespace entry for it,
			// then show that deleting the descriptor fails without the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
				fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
					defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			},
			op: fmt.Sprintf(`SELECT crdb_internal.unsafe_delete_descriptor(%d);`, invalidTableID),
			expErrRE: fmt.Sprintf(`pq: crdb_internal.unsafe_delete_descriptor\(\): relation "foo" \(%d\): duplicate column name: "i"`,
				invalidTableID),
		},
		{ // 7
			// Upsert a descriptor which is invalid, upsert a namespace entry for it,
			// then show that deleting the descriptor succeeds with the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
				fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
					defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			},
			op: fmt.Sprintf(`SELECT crdb_internal.unsafe_delete_descriptor(%d, true);`, invalidTableID),
			expEventLogEntries: []eventLogPattern{
				{
					typ:  "unsafe_delete_descriptor",
					info: `"Force":true,"ForceNotice":".*duplicate column name: \\"i\\""`,
				},
			},
		},
		{ // 8
			// Upsert a descriptor which is invalid, upsert a namespace entry for it,
			// then show that updating the descriptor fails without the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
				fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
					defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			},
			op: updateInvalidateDuplicateColumnDescriptorNoForce,
			expErrRE: fmt.Sprintf(`pq: crdb_internal.unsafe_upsert_descriptor\(\): relation "foo" \(%d\): duplicate column name: "i"`,
				invalidTableID),
		},
		{ // 9
			// Upsert a descriptor which is invalid, upsert a namespace entry for it,
			// then show that updating the descriptor succeeds the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
				fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
					defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			},
			op: updateInvalidateDuplicateColumnDescriptorForce,
			expEventLogEntries: []eventLogPattern{
				{
					typ:  "unsafe_upsert_descriptor",
					info: `"Force":true,"ForceNotice":".*duplicate column name: \\"i\\""`,
				},
			},
			after: []string{
				// Ensure that the table is usable.
				fmt.Sprintf(`INSERT INTO [%d as t] VALUES (1), (2)`, invalidTableID),
			},
		},
		{ // 10
			// Upsert a descriptor which is invalid, upsert a namespace entry for it,
			// then show that deleting the namespace entry fails without the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
				fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
					defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			},
			op: fmt.Sprintf(`SELECT crdb_internal.unsafe_delete_namespace_entry(%d, %d, 'foo', %d);`,
				defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			expErrRE: fmt.Sprintf(`pq: crdb_internal.unsafe_delete_namespace_entry\(\): failed to retrieve descriptor %d: relation "foo" \(%d\): duplicate column name: "i"`,
				invalidTableID, invalidTableID),
		},
		{ // 11
			// Upsert a descriptor which is invalid, upsert a namespace entry for it,
			// then show that deleting the namespace entry succeeds with the force flag.
			before: []string{
				upsertInvalidateDuplicateColumnDescriptorBefore,
				fmt.Sprintf(`SELECT crdb_internal.unsafe_upsert_namespace_entry(%d, %d, 'foo', %d, true);`,
					defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			},
			op: fmt.Sprintf(`SELECT crdb_internal.unsafe_delete_namespace_entry(%d, %d, 'foo', %d, true);`,
				defaultDBID, defaultDBPublicSchemaID, invalidTableID),
			expEventLogEntries: []eventLogPattern{
				{
					typ:  "unsafe_delete_namespace_entry",
					info: `"Force":true,"ForceNotice":".*duplicate column name: \\"i\\""`,
				},
			},
		},
		{ // 12
			// Upsert a descriptor with the force flag set to skip descriptor
			// validation at txn commit time, then check that subsequently upserting
			// an invalid descriptor with the force flag unset correctly triggers a
			// validation error.
			before: []string{
				`CREATE DATABASE test`,
				`CREATE TABLE test.foo ()`,
				`SELECT crdb_internal.unsafe_upsert_descriptor(id, descriptor, true) FROM system.descriptor WHERE id = 'test.foo'::REGCLASS::OID`,
			},
			op:       upsertInvalidNameInTestFooNoForce,
			expErrRE: `pq: crdb_internal.unsafe_upsert_descriptor\(\): relation \"\" \(56\): empty table name`,
		},
	} {
		t.Run(fmt.Sprintf("case #%d: %s", caseIdx+1, tc.op), func(t *testing.T) {
			s, db, cleanup := setup(t)
			now := s.Clock().Now().GoTime()
			defer cleanup()
			tdb := sqlutils.MakeSQLRunner(db)
			descs.ValidateOnWriteEnabled.Override(ctx, &s.ClusterSettings().SV, false)
			for _, op := range tc.before {
				tdb.Exec(t, op)
			}
			descs.ValidateOnWriteEnabled.Override(ctx, &s.ClusterSettings().SV, true)
			_, err := db.Exec(tc.op)
			if tc.expErrRE == "" {
				require.NoError(t, err)
			} else {
				require.Regexp(t, tc.expErrRE, err)
			}
			rows := tdb.Query(t, `SELECT "eventType", info FROM system.eventlog WHERE timestamp > $1`,
				now)
			mat, err := sqlutils.RowsToStrMatrix(rows)
			require.NoError(t, err)
		outer:
			for _, exp := range tc.expEventLogEntries {
				for _, e := range mat {
					if e[0] != exp.typ {
						continue
					}
					if matched, err := regexp.MatchString(exp.info, e[1]); err != nil {
						t.Fatal(err)
					} else if matched {
						continue outer
					}
				}
				t.Errorf("failed to find log entry matching %+v in:\n%s", exp, sqlutils.MatrixToStr(mat))
			}
		})
	}
}

// This is the json representation of a descriptor which has duplicate
// columns i and will subsequently fail validation.
var invalidDuplicateColumnDescriptor = fmt.Sprintf(`'{
  "table": {
    "auditMode": "DISABLED",
    "columns": [
      {
        "id": 1,
        "name": "i",
        "type": {"family": "IntFamily", "oid": 20, "width": 64}
      },
      {
        "id": 1,
        "name": "i",
        "type": {"family": "IntFamily", "oid": 20, "width": 64}
      }
    ],
    "families": [
      {
        "columnIds": [
          1
        ],
        "columnNames": [
          "i"
        ],
        "defaultColumnId": 0,
        "id": 0,
        "name": "primary"
      }
    ],
    "formatVersion": 3,
    "id": %d,
    "name": "foo",
    "nextColumnId": 2,
    "nextFamilyId": 1,
    "nextIndexId": 2,
    "nextMutationId": 2,
    "parentId": %d,
    "primaryIndex": {
      "keyColumnDirections": [
        "ASC"
      ],
      "keyColumnIds": [
        1
      ],
      "keyColumnNames": [
        "i"
      ],
      "compositeColumnIds": [],
      "createdExplicitly": false,
      "encodingType": 1,
      "id": 1,
      "name": "primary",
      "type": "FORWARD",
      "unique": true,
      "version": 4
    },
    "privileges": {
      "ownerProto": "root",
      "users": [
        {
          "privileges": 2,
          "userProto": "admin"
        },
        {
          "privileges": 2,
          "userProto": "root"
        }
      ],
      "version": 1
    },
    "state": "PUBLIC",
    "unexposedParentSchemaId": %d,
    "version": 1
  }
}'`, invalidTableID, defaultDBID, defaultDBPublicSchemaID)

// This is a statement to insert the invalid descriptor above using
// crdb_internal.unsafe_upsert_descriptor.
var upsertInvalidateDuplicateColumnDescriptorBefore = fmt.Sprintf(`
SELECT crdb_internal.unsafe_upsert_descriptor(%d,
    crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', `+
	invalidDuplicateColumnDescriptor+`), true)`, firstDatabaseID)

// These are CTEs for the below statements to update the above descriptor
// and fix its validation problems.
var updateInvalidateDuplicateColumnDescriptorCTEs = fmt.Sprintf(`
  WITH as_json AS (
                SELECT crdb_internal.pb_to_json(
                            'cockroach.sql.sqlbase.Descriptor',
                            descriptor,
                            false -- emit_defaults
                        ) AS descriptor
                  FROM system.descriptor
                 WHERE id = %d
               ),
       updated AS (
                SELECT crdb_internal.json_to_pb(
                        'cockroach.sql.sqlbase.Descriptor',
                        json_set(
                            descriptor,
                            ARRAY['table', 'columns'],
                            json_build_array(
                                descriptor->'table'->'columns'->0
                            )
                        )
                       ) AS descriptor
                  FROM as_json
               )
`, firstDatabaseID)

// This is a statement to update the above descriptor fixing its validity
// problems without the force flag.
var updateInvalidateDuplicateColumnDescriptorNoForce = fmt.Sprintf(``+
	updateInvalidateDuplicateColumnDescriptorCTEs+`
SELECT crdb_internal.unsafe_upsert_descriptor(%d, descriptor)
  FROM updated;
`, firstDatabaseID)

// This is a statement to update the above descriptor fixing its validity
// problems with the force flag.
var updateInvalidateDuplicateColumnDescriptorForce = fmt.Sprintf(``+
	updateInvalidateDuplicateColumnDescriptorCTEs+`
SELECT crdb_internal.unsafe_upsert_descriptor(%d, descriptor, true)
  FROM updated;
`, firstDatabaseID)

// This is the descriptor definition used in the upsertRepair* statements.
var repairedDescriptor = fmt.Sprintf(`'{
  "table": {
    "columns": [ { "id": 1, "name": "i", "type": { "family": "IntFamily", "oid": 20, "width": 64 } } ],
    "families": [
      {
        "columnIds": [ 1 ],
        "columnNames": [ "i" ],
        "defaultColumnId": 0,
        "id": 0,
        "name": "primary"
      }
    ],
    "formatVersion": 3,
    "id": %d,
    "name": "foo",
    "nextColumnId": 2,
    "nextFamilyId": 1,
    "nextIndexId": 2,
    "nextMutationId": 1,
    "nextConstraintId": 2,
    "parentId": %d,
    "primaryIndex": {
      "encodingType": 1,
      "keyColumnDirections": [ "ASC" ],
      "keyColumnIds": [ 1 ],
      "keyColumnNames": [ "i" ],
      "id": 1,
      "name": "primary",
      "type": "FORWARD",
      "unique": true,
      "version": 4,
			"constraintId": 1
    },
    "privileges": {
      "owner_proto": "root",
      "users": [
        { "privileges": 2, "user_proto": "admin" },
        { "privileges": 2, "user_proto": "newuser1" },
        { "privileges": 2, "user_proto": "newuser2" },
        { "privileges": 2, "user_proto": "root" }
      ],
      "version": 1
    },
    "state": "PUBLIC",
    "unexposedParentSchemaId": %d,
    "version": 2
  }
}'`, firstTableID, firstDatabaseID, firstPublicSchemaID)

// This is a statement to repair an invalid descriptor using
// crdb_internal.unsafe_upsert_descriptor.
var upsertRepairNoForce = fmt.Sprintf(`
SELECT crdb_internal.unsafe_upsert_descriptor(%d,
	crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', `+
	repairedDescriptor+`), false)`, firstTableID)

// This is a statement to force-repair an invalid descriptor using
// crdb_internal.unsafe_upsert_descriptor.
var upsertRepairForce = fmt.Sprintf(`
SELECT crdb_internal.unsafe_upsert_descriptor(%d,
	crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', `+
	repairedDescriptor+`), true)`, firstTableID)

const upsertInvalidNameInTestFooNoForce = `
WITH
	as_json
		AS (
			SELECT
				id,
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor,
					false
				)
					AS d
			FROM
				system.descriptor
			WHERE
				id = 'test.foo'::REGCLASS::OID
		),
	updated
		AS (
			SELECT
				id,
				crdb_internal.json_to_pb(
					'cockroach.sql.sqlbase.Descriptor',
					json_set(
						json_set(
							json_set(
								d,
								ARRAY['table', 'name'],
								'""'::JSONB
							),
							ARRAY['table', 'version'],
							((d->'table'->>'version')::INT8 + 1)::STRING::JSONB
						),
						ARRAY['table', 'modificationTime'],
						json_build_object(
							'wallTime',
							(
								(extract('epoch', now()) * 1000000)::INT8
								* 1000
							)::STRING
						)
					)
				)
					AS descriptor
			FROM
				as_json
		)
SELECT
	crdb_internal.unsafe_upsert_descriptor(id, descriptor, false)
FROM
	updated
`

// This is a statement to update the above descriptor's privileges.
// It will change the table owner, add privileges for a new user,
// alter the privilege of an existing user, and revoke all privileges for an old user.
var upsertUpdatePrivileges = fmt.Sprintf(`
SELECT crdb_internal.unsafe_upsert_descriptor(%d, crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor',
'{
	"table": {
    "columns": [ { "id": 1, "name": "i", "type": { "family": "IntFamily", "oid": 20, "width": 64 } } ],
    "families": [
      {
        "columnIds": [ 1 ],
        "columnNames": [ "i" ],
        "defaultColumnId": 0,
        "id": 0,
        "name": "primary"
      }
    ],
    "formatVersion": 3,
    "id": %d,
    "name": "foo",
    "nextColumnId": 2,
    "nextFamilyId": 1,
    "nextIndexId": 2,
    "nextMutationId": 1,
		"nextConstraintId": 2,
    "parentId": %d,
    "primaryIndex": {
      "encodingType": 1,
      "keyColumnDirections": [ "ASC" ],
      "keyColumnIds": [ 1 ],
      "keyColumnNames": [ "i" ],
      "id": 1,
      "name": "primary",
      "type": "FORWARD",
      "unique": true,
      "version": 4,
			"constraintId": 1
    },
    "privileges": {
      "owner_proto": "admin",
      "users": [
        { "privileges": 2, "user_proto": "admin" },
        { "privileges": 2, "user_proto": "root" },
        { "privileges": 8, "user_proto": "newuser1" }
      ],
      "version": 1
    },
    "state": "PUBLIC",
    "unexposedParentSchemaId": %d,
    "version": 2
  }
}
'))
`, firstTableID, firstTableID, firstDatabaseID, firstPublicSchemaID)

// TestDescriptorRepairIdGeneration verifies that `unsafe_upsert_descriptor`
// properly takes the descriptor ID sequence into account. Otherwise a CREATE
// statement might eventually overwrite a descriptor inserted in this way.
func TestDescriptorRepairIdGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	const q = `SELECT crdb_internal.unsafe_upsert_descriptor(123, crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', $2::JSONB), $1)`
	const d = `{
  "database": {
    "id": 123,
    "name": "foo",
    "privileges": {
      "ownerProto": "root",
      "users": [
        {
          "privileges": 2,
          "userProto": "admin"
        },
        {
          "privileges": 2,
          "userProto": "root"
        }
      ],
      "version": 2
    },
    "version": "1"
  }
}`

	// Required so test doesn't fail due to namespace validation failures.
	descs.ValidateOnWriteEnabled.Override(ctx, &s.ClusterSettings().SV, false)

	// Inserting a descriptor with an ID too high should fail.
	tdb.ExpectErr(t, "descriptor ID 123 must be less than the descriptor ID sequence value", q, false /* force */, d)
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.descriptor WHERE id >= 123", [][]string{{"0"}})

	// Force the insertion.
	tdb.Exec(t, q, true /* force */, d)
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.descriptor WHERE id >= 123", [][]string{{"1"}})

	// Subsequent new descriptors should have an even greater ID.
	tdb.Exec(t, "CREATE DATABASE bar;")
	// There should be 3 descriptors with an id >= 123, including the database
	// descriptor and the public schema descriptor.
	tdb.CheckQueryResults(t, "SELECT count(*) FROM system.descriptor WHERE id >= 123", [][]string{{"3"}})
}

// TestCorruptDescriptorRepair tests that a corrupt table descriptor can be
// repaired to a point where it can subsequently be dropped.
func TestCorruptDescriptorRepair(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	// Set up the test database with a parent table with a dangling foreign key
	// back-reference.
	tdb.Exec(t, `CREATE DATABASE testdb`)
	tdb.Exec(t, `CREATE TABLE testdb.parent (k INT PRIMARY KEY, v STRING)`)
	tdb.Exec(t, `CREATE TABLE testdb.child (k INT NOT NULL, FOREIGN KEY (k) REFERENCES testdb.parent (k))`)
	tdb.Exec(t, `SELECT crdb_internal.unsafe_delete_descriptor(id) FROM system.namespace WHERE name = 'child'`)
	tdb.Exec(t, `SELECT crdb_internal.unsafe_delete_namespace_entry("parentID", "parentSchemaID", name, id) FROM system.namespace WHERE name = 'child'`)

	// Dropping the table should fail, because the table descriptor will fail
	// the validation checks when being read from storage.
	tdb.ExpectErr(t, "invalid foreign key backreference", `DROP TABLE testdb.parent`)

	const parentVersion = `SELECT
				crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', sd.descriptor, false)->'table'->>'version'
			FROM
				system.descriptor AS sd INNER JOIN system.namespace AS sn ON sd.id = sn.id
			WHERE
				sn.name = 'parent'`
	tdb.CheckQueryResults(t, parentVersion, [][]string{{"2"}})

	// Repair query, with the following args:
	// - $1: new version counter value
	// - $2: force flag for unsafe_upsert_descriptor
	const repair = `
WITH
	to_json
		AS (
			SELECT
				sd.id,
				crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', sd.descriptor, false) AS d
			FROM
				system.descriptor AS sd INNER JOIN system.namespace AS sn ON sd.id = sn.id
			WHERE
				sn.name = 'parent'
		),
	modified
		AS (
			SELECT
				id,
				json_set(
					json_set(
						json_set(d, ARRAY['table', 'inboundFks'], '[]'::JSONB),
						ARRAY['table', 'version'],
						$1
					),
					ARRAY['table', 'modificationTime'],
					json_build_object(
						'wallTime',
						((extract('epoch', now()) * 1000000)::INT8 * 1000)::STRING
					)
				)
					AS d
			FROM
				to_json
		)
SELECT
	crdb_internal.unsafe_upsert_descriptor(
		id,
		crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', d),
		$2
	)
FROM
	modified;
`
	// Repairing without the force flag should fail.
	tdb.ExpectErr(t, "invalid foreign key backreference", repair, 3, false)

	// Repairing with the force flag should succeed regardless of the supplied
	// version.
	tdb.Exec(t, repair, 12345, true)
	tdb.Exec(t, `DROP TABLE testdb.parent`)
}
