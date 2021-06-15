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
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestForeignKeyMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	/*
	   These descriptors were crafted in v19.1 with the following SQL:

	   	CREATE DATABASE db;
	   	SET database = db;
	   	BEGIN;
	   		CREATE TABLE parent (i INT8 PRIMARY KEY, j INT8 NOT NULL UNIQUE);
	   		CREATE TABLE child_pk (i INT8 PRIMARY KEY REFERENCES parent (i));
	   		CREATE TABLE child_secondary (i INT8 PRIMARY KEY, j INT8 REFERENCES parent (j));
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
	const descriptorStringsToInject = `
121d0a02646210341a150a090a0561646d696e10020a080a04726f6f741002
0aa5020a06706172656e741835203428013a0a08d3d9d1d782e393b31642130a016910011a0808011040180030032000300042130a016a10021a08080110401800300320003000480352410a077072696d61727910011801220169300140004a10080010001a00200028003000380040005210083610011a00200028003000380040005a007a0208008001005a480a0c706172656e745f6a5f6b65791002180122016a3002380140004a10080010001a00200028003000380040005210083710021a00200028003000380040005a007a02080080010060036a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f80100800200
0ac0010a086368696c645f706b1836203428013a0a08d3d9d1d782e393b31642130a016910011a080801104018003003200030004802523e0a077072696d61727910011801220169300140004a1f083510011a0f666b5f695f7265665f706172656e74200028013000380040005a007a02080080010060026a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201120a077072696d61727910001a016920012800b80101c20100e80100f2010408001200f80100800200
0ab7020a0f6368696c645f7365636f6e646172791837203428013a0a08d3d9d1d782e393b31642130a016910011a0808011040180030032000300042130a016a10021a080801104018003003200130004803522f0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a0208008001005a630a2a6368696c645f7365636f6e646172795f6175746f5f696e6465785f666b5f6a5f7265665f706172656e741002180022016a3002380140004a1f083510021a0f666b5f6a5f7265665f706172656e74200028013000380040005a007a02080080010060036a150a090a0561646d696e10020a080a04726f6f741002800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f80100800200
`
	var descriptorsToInject []*descpb.Descriptor
	for _, s := range strings.Split(strings.TrimSpace(descriptorStringsToInject), "\n") {
		encoded, err := hex.DecodeString(s)
		require.NoError(t, err)
		var desc descpb.Descriptor
		require.NoError(t, protoutil.Unmarshal(encoded, &desc))
		descriptorsToInject = append(descriptorsToInject, &desc)
	}

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
	db := tc.ServerConn(0)
	require.NoError(t, sqlutils.InjectDescriptors(
		ctx, db, descriptorsToInject, true, /* force */
	))
	tdb := sqlutils.MakeSQLRunner(db)
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
	require.Equal(t, 4, numUnupgradedDescriptors())
	tdb.Exec(t, "SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.ForeignKeyRepresentationMigration).String())
	require.Equal(t, 0, numUnupgradedDescriptors())
}
