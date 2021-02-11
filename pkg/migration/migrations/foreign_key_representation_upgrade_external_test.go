// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestForeignKeyMigration(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.ForeignKeyRepresentationMigration - 1),
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `
  WITH encoded_descs AS (SELECT decode(unnest($1::STRING[]), 'hex') AS encoded),
       decoded AS (
                SELECT crdb_internal.pb_to_json(
                        'cockroach.sql.sqlbase.Descriptor',
                        encoded
                       ) AS descr,
                       encoded
                  FROM encoded_descs
               ),
       upserted_descriptors AS (
                                SELECT crdb_internal.unsafe_upsert_descriptor(
                                        COALESCE(
                                            (descr->'database'->>'id')::INT8,
                                            (descr->'table'->>'id')::INT8
                                        ),
                                        encoded
                                       )
                                  FROM decoded
                            ),
       upserted_db_namespace_entry AS (
                                    SELECT
                                        crdb_internal.unsafe_upsert_namespace_entry(
                                            0,
                                            0,
                                            descr->'database'->>'name',
                                            (descr->'database'->>'id')::INT8
                                        )
                                    FROM
                                        decoded
                                    WHERE
                                        (descr->'database') IS NOT NULL
                                   ),
       database_id AS (
                    SELECT (descr->'database'->>'id')::INT8 AS id
                      FROM decoded
                     WHERE (descr->'database') IS NOT NULL
                   ),
       upserted_table_namespace_entries AS (
                                            SELECT
                                                crdb_internal.unsafe_upsert_namespace_entry(
                                                    id,
                                                    29,
                                                    descr->'table'->>'name',
                                                    (descr->'table'->>'id')::INT8
                                                )
                                            FROM
                                                decoded
                                                LEFT JOIN database_id ON true
                                            WHERE
                                                (descr->'table') IS NOT NULL
                                        )
SELECT EXISTS(SELECT * FROM upserted_descriptors)
   AND EXISTS(SELECT * FROM upserted_db_namespace_entry)
   AND EXISTS(SELECT * FROM upserted_table_namespace_entries);`,
		pq.Array(strings.Split(descriptorsToInject, "\n")))

	numUnupgradedDescriptors := func() (numUnupgraded int) {
		tdb.QueryRow(t, `
WITH dd AS (
            SELECT id,
                   crdb_internal.pb_to_json(
                    'cockroach.sql.sqlbase.Descriptor',
                    descriptor,
                    false
                   ) AS descriptor
              FROM system.descriptor
          ),
       tables AS (
                SELECT id, descriptor->'table' AS tab
                  FROM dd
                 WHERE (descriptor->'table') IS NOT NULL
              ),
       indexes AS (
                 SELECT id, tab->'primaryIndex' AS idx FROM tables
                 UNION SELECT id, json_array_elements(tab->'indexes') AS idx
                         FROM tables
               )
SELECT count(*)
  FROM indexes
 WHERE idx->'foreignKey' != '{}' OR (idx->'referencedBy') IS NOT NULL;`).Scan(&numUnupgraded)
		return numUnupgraded
	}
	require.Equal(t, 3, numUnupgradedDescriptors())
	tdb.Exec(t, "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.ForeignKeyRepresentationMigration).String())
	require.Equal(t, 0, numUnupgradedDescriptors())
}

/*
 These descriptors were crafted in v19.1 with the following SQL:

  CREATE DATABASE db;
  SET database = db;
  BEGIN;
    CREATE TABLE parent (i INT8 PRIMARY KEY, j INT8 NOT NULL UNIQUE);
    CREATE TABLE child_pk (i INT8 PRIMARY KEY REFERENCES parent (i));
    CREATE TABLE child_secondary (i INT8 PRIMARY KEY, j INT8 REFERENCES parent (i));
  COMMIT;

 The descriptors were then extracted using:

  SELECT encode(descriptor, 'hex') AS descriptor
    FROM system.descriptor
   WHERE id
         IN (
              SELECT id
                FROM system.namespace
               WHERE "parentID"
                     = (
                          SELECT id
                            FROM system.namespace
                           WHERE "parentID" = 0 AND name = 'db'
                      )
                  OR "parentID" = 0 AND name = 'db'
          );
*/
var descriptorsToInject = `
121d0a02646210341a150a090a0561646d696e10020a080a04726f6f741002
0aa5020a06706172656e741835203428013a0a0882f0e7a4b8b5b0b11642130a016910011a0808011040180030032000300042130a016a10021a08080110401800300320003000480352530a077072696d61727910011801220169300140004a10080010001a00200028003000380040005210083610011a00200028003000380040005210083710021a00200028003000380040005a007a0208008001005a360a0c706172656e745f6a5f6b65791002180122016a3002380140004a10080010001a00200028003000380040005a007a02080080010060036a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f80100800200
0ac0010a086368696c645f706b1836203428013a0a0882f0e7a4b8b5b0b11642130a016910011a080801104018003003200030004802523e0a077072696d61727910011801220169300140004a1f083510011a0f666b5f695f7265665f706172656e74200028013000380040005a007a02080080010060026a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201120a077072696d61727910001a016920012800b80101c20100e80100f2010408001200f80100800200
0ab7020a0f6368696c645f7365636f6e646172791837203428013a0a0882f0e7a4b8b5b0b11642130a016910011a0808011040180030032000300042130a016a10021a080801104018003003200130004803522f0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a0208008001005a630a2a6368696c645f7365636f6e646172795f6175746f5f696e6465785f666b5f6a5f7265665f706172656e741002180022016a3002380140004a1f083510011a0f666b5f6a5f7265665f706172656e74200028013000380040005a007a02080080010060036a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f80100800200
`
