// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const countToDeleteFunctionQuery = `
WITH to_json AS (
    SELECT 
      id,
      crdb_internal.pb_to_json(
        'cockroach.sql.sqlbase.Descriptor',
        descriptor,
        false
      ) AS d
    FROM 
      system.descriptor
),
to_delete AS (
    SELECT id
    FROM to_json
    WHERE 
      d->'function' IS NOT NULL
      AND d->'function'->>'declarativeSchemaChangerState' IS NULL
      AND d->'function'->>'state' = 'DROP'
)
SELECT count(id)
FROM to_delete
`

// Similar to countToDeleteFunctionQuery but includes functions dropped by
// declarative schema changer.
const countTotalDroppedFunctionQuery = `
WITH to_json AS (
    SELECT 
      id,
      crdb_internal.pb_to_json(
        'cockroach.sql.sqlbase.Descriptor',
        descriptor,
        false
      ) AS d
    FROM 
      system.descriptor
),
to_delete AS (
    SELECT id
    FROM to_json
    WHERE 
      d->'function' IS NOT NULL
      AND d->'function'->>'state' = 'DROP'
)
SELECT count(id)
FROM to_delete
`

// Similar to countToDeleteFunctionQuery but includes functions dropped by
// declarative schema changer.
const countTotalFunctionQuery = `
WITH to_json AS (
    SELECT 
      id,
      crdb_internal.pb_to_json(
        'cockroach.sql.sqlbase.Descriptor',
        descriptor,
        false
      ) AS d
    FROM 
      system.descriptor
),
to_delete AS (
    SELECT id
    FROM to_json
    WHERE 
      d->'function' IS NOT NULL
)
SELECT count(id)
FROM to_delete
`

func TestDeleteDescriptorsOfDroppedFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.V23_1_DeleteDroppedFunctionDescriptors)
	)

	droppedFunctionsHex := []string{
		"2a5d0a016610731864206532100a0c08011040180030005014600010003801420953454c45435420313b48015000580162210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418028001028a01009001029a0100",
		"2a5d0a016710741864206532100a0c08011040180030005014600010003801420953454c45435420313b48015000580162210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418028001028a01009001029a0100",
		"2a5d0a016610751864206532100a0c08011040180030005014600010003801420953454c45435420313b48015000580162210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418028001028a01009001029a0100",
		"2a5d0a016710761864206532100a0c08011040180030005014600010003801420953454c45435420313b48015000580162210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418028001028a01009001029a0100",
	}

	droppedFunctionsHexDeclarative := "2ae5030a04666e6577106c1864206532100a0c08011040180030005014600010003801420953454c45435420313b48015000580162210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418028001028a01009001029a0100a20181030a150a0bea0308086c1204726f6f7412040801100118010a1a0a10f2030d086c120561646d696e1802200212040801100118010a190a0ff2030c086c1204726f6f741802200212040801100118010a1e0a144a12086c220e0a0c08011040180030005014600012040801100118010a110a07a20604086c106512040801100118010a150a0b820a08086c1204666e657712040801100118010a130a098a0a06086c1202080112040801100118010a0f0a05920a02086c12040801100118010a130a099a0a06086c5a02080112040801100118010a1e0a14a20a11086c120953454c45435420313b1a020801120408011001180112510a4f0a1244524f502046554e4354494f4e20666e6577122a44524f502046554e4354494f4e20e280b92222e280ba2ee280b92222e280ba2ee280b9666e6577e280ba1a0d44524f502046554e4354494f4e1a170a04726f6f74120f2420636f636b726f6163682073716c220a01010105010101010101288180de85ed86c7cd0b320a00010203040506070809"

	publicFunctionHex := "2a640a08667075626c696335106d1864206532100a0c08011040180030005014600010003801420953454c45435420313b48015000580162210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f7418028001008a01009001019a0100"

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          v0,
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)

	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	var parentID, parentSchemaID descpb.ID
	tdb.Exec(t, "CREATE TABLE temp_tbl()")
	tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
		Scan(&parentID, &parentSchemaID)

	for _, fnHex := range droppedFunctionsHex {
		decodeFunctionDescriptorAndInsert(t, ctx, sqlDB, fnHex, parentID, parentSchemaID, true /* dropped */)
	}
	decodeFunctionDescriptorAndInsert(t, ctx, sqlDB, droppedFunctionsHexDeclarative, parentID, parentSchemaID, true /* dropped */)
	decodeFunctionDescriptorAndInsert(t, ctx, sqlDB, publicFunctionHex, parentID, parentSchemaID, false /* dropped */)

	// Make sure that the number of function descriptors to delete is right.
	row := tdb.QueryRow(t, countToDeleteFunctionQuery)
	var cntFnToDelete int
	row.Scan(&cntFnToDelete)
	require.Equal(t, 4, cntFnToDelete)

	row = tdb.QueryRow(t, countTotalDroppedFunctionQuery)
	row.Scan(&cntFnToDelete)
	require.Equal(t, 5, cntFnToDelete)

	row = tdb.QueryRow(t, countTotalFunctionQuery)
	row.Scan(&cntFnToDelete)
	require.Equal(t, 6, cntFnToDelete)

	var originalTotalDescCnt int
	row = tdb.QueryRow(t, `SELECT count(*) FROM system.descriptor`)
	row.Scan(&originalTotalDescCnt)

	// Upgrade to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`, v1.String())
	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{v1.String()}})

	// Make sure there is nothing to delete.
	row = tdb.QueryRow(t, countToDeleteFunctionQuery)
	row.Scan(&cntFnToDelete)
	require.Equal(t, 0, cntFnToDelete)

	// Make sure other descriptors are intact.
	const deletedFnCount = 4
	const newSystemTableCount = 6
	var newTotalDescCnt int
	row = tdb.QueryRow(t, `SELECT count(*) FROM system.descriptor`)
	row.Scan(&newTotalDescCnt)
	require.Equal(t, originalTotalDescCnt-deletedFnCount+newSystemTableCount, newTotalDescCnt)

	row = tdb.QueryRow(t, countTotalDroppedFunctionQuery)
	row.Scan(&cntFnToDelete)
	require.Equal(t, 1, cntFnToDelete)

	row = tdb.QueryRow(t, countTotalFunctionQuery)
	row.Scan(&cntFnToDelete)
	require.Equal(t, 2, cntFnToDelete)
}

func decodeFunctionDescriptorAndInsert(
	t *testing.T,
	ctx context.Context,
	sqlDB *gosql.DB,
	hexEncodedDescriptor string,
	parentID, parentSchemaID descpb.ID,
	dropped bool,
) {
	decodedDescriptor, err := hex.DecodeString(hexEncodedDescriptor)
	require.NoError(t, err)
	b, err := descbuilder.FromBytesAndMVCCTimestamp(decodedDescriptor, hlc.Timestamp{WallTime: 1})
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Equal(t, catalog.Function, b.DescriptorType())
	// Run post deserialization changes.
	require.NoError(t, b.RunPostDeserializationChanges())
	// Modify this descriptor's parentID and parentSchemaID
	fnDesc := b.(funcdesc.FunctionDescriptorBuilder).BuildCreatedMutableFunction()
	fnDesc.ParentID = parentID
	fnDesc.ParentSchemaID = parentSchemaID
	require.Equal(t, dropped, fnDesc.Dropped())
	// Insert the descriptor into test cluster.
	require.NoError(t, sqlutils.InjectDescriptors(
		ctx, sqlDB, []*descpb.Descriptor{fnDesc.DescriptorProto()}, true, /* force */
	))
}
