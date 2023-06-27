// Copyright 2022 The Cockroach Authors.
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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestUpgradeSeqToBeReferencedByID tests that sequence references by name will be upgraded
// to be by ID in tables or views.
func TestUpgradeSeqToBeReferencedByID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.UpgradeSequenceToBeReferencedByID - 1)
		v1 = clusterversion.ByKey(clusterversion.UpgradeSequenceToBeReferencedByID)
	)

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

	/*
		The hex for the descriptor to inject was created by running the following
		commands in a 20.2 binary, in which sequences are referenced by name in
		tables and views.

				CREATE SEQUENCE s;
				CREATE TABLE tbl (i INT PRIMARY KEY, j INT NOT NULL DEFAULT nextval('s'));
				CREATE VIEW v AS (SELECT nextval('s'));
				CREATE TABLE tbl2 (i INT PRIMARY KEY, j INT NOT NULL); -- a table which does not need to be upgraded

				SELECT encode(descriptor, 'hex')
				FROM system.descriptor
				WHERE id = (
						SELECT id
						FROM system.namespace
						WHERE name = 's'
				);

				SELECT encode(descriptor, 'hex')
				FROM system.descriptor
				WHERE id = (
						SELECT id
						FROM system.namespace
						WHERE name = 'tbl'
				);

				SELECT encode(descriptor, 'hex')
				FROM system.descriptor
				WHERE id = (
						SELECT id
						FROM system.namespace
						WHERE name = 'v'
				);

				SELECT encode(descriptor, 'hex')
				FROM system.descriptor
				WHERE id = (
						SELECT id
						FROM system.namespace
						WHERE name = 'tbl2'
				);
	*/

	var parentID, parentSchemaID descpb.ID
	tdb.Exec(t, "CREATE TABLE temp_tbl()")
	tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
		Scan(&parentID, &parentSchemaID)
	var table, createTable string
	const sequenceDescriptorToInject = "0aa0020a01731834203228033a0042210a0576616c756510011a0c080110401800300050146000200030006800700078004800524e0a077072696d61727910011800220576616c7565300140004a10080010001a00200028003000380040005a007a020800800100880100900100980100a20106080012001800a80100b20100ba010060006a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801800100880103980100b201160a077072696d61727910001a0576616c756520012801b80100c20100d20106083510001802d2010408361000e201180801100118ffffffffffffffff7f20012800320408001000e80100f2010408001200f801008002009202009a020a08c0f0f4deb8b4f4f816b20200b80200c0021dc80200"
	const tableDescriptorToInject = "0a9e020a0374626c1835203228013a00421d0a016910011a0c0801104018003000501460002000300068007000780042360a016a10021a0c08011040180030005014600020002a156e65787476616c282773273a3a3a535452494e4729300050346800700078004803524a0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a020800800100880100900101980100a20106080012001800a80100b20100ba010060026a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f801008002009202009a0200b20200b80200c0021dc80200"
	const viewDescriptorToInject = "0ae3010a01761836203228013a0042230a076e65787476616c10011a0c080110401800300050146000200130006800700078004802523c0a00100018004a10080010001a00200028003000380040005a007a020800800100880100900100980100a20106080012001800a80100b20100ba010060006a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801800101880103980100b80100c2011e2853454c454354206e65787476616c282773273a3a3a535452494e472929c80134e80100f2010408001200f801008002009202009a0200b20200b80200c0021dc80200"
	const table2DescriptorToInject = "0a86020a0474626c32183e203428013a00421d0a016910011a0c08011040180030005014600020003000680070007800421d0a016a10021a0c080110401800300050146000200030006800700078004803524a0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a020800800100880100900101980100a20106080012001800a80100b20100ba010060026a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801800101880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f801008002009202009a0200b20200b80200c0021dc80200"

	// A function that decode a table descriptor from a hex-encoded string and
	// insert it into the test cluster.
	decodeTableDescriptorAndInsert := func(hexEncodedDescriptor string) {
		decodedDescriptor, err := hex.DecodeString(hexEncodedDescriptor)
		require.NoError(t, err)
		b, err := descbuilder.FromBytesAndMVCCTimestamp(decodedDescriptor, hlc.Timestamp{WallTime: 1})
		require.NoError(t, err)
		require.NotNil(t, b)
		require.Equal(t, catalog.Table, b.DescriptorType())
		// Run post deserialization changes.
		require.NoError(t, b.RunPostDeserializationChanges())
		// Modify this descriptor's parentID and parentSchemaID
		tableDesc := b.(tabledesc.TableDescriptorBuilder).BuildCreatedMutableTable()
		tableDesc.ParentID = parentID
		tableDesc.UnexposedParentSchemaID = parentSchemaID
		// Insert the descriptor into test cluster.
		require.NoError(t, sqlutils.InjectDescriptors(
			ctx, sqlDB, []*descpb.Descriptor{tableDesc.DescriptorProto()}, true, /* force */
		))
	}

	// A function that looks up version of a descriptor.
	queryDescVersion := func(descName string) uint32 {
		descID := tdb.QueryStr(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = '%v'`, descName))[0][0]
		id, err := strconv.Atoi(descID)
		require.NoError(t, err)
		descVersion := tdb.QueryStr(t, fmt.Sprintf(`SELECT crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) -> 'table' ->> 'version' FROM system.descriptor WHERE id = %v;`, id))[0][0]
		version, err := strconv.Atoi(descVersion)
		require.NoError(t, err)
		return uint32(version)
	}

	// Decode and insert all descriptors, and assert by-name sequence reference in
	// some descriptors.
	for _, descHexCode := range []string{sequenceDescriptorToInject, tableDescriptorToInject, viewDescriptorToInject, table2DescriptorToInject} {
		decodeTableDescriptorAndInsert(descHexCode)
	}
	tdb.QueryRow(t, `SHOW CREATE tbl`).Scan(&table, &createTable)
	require.True(t, strings.Contains(createTable, "j INT8 NOT NULL DEFAULT nextval('s':::STRING)"))
	tdb.QueryRow(t, `SHOW CREATE v`).Scan(&table, &createTable)
	require.True(t, strings.Contains(createTable, "SELECT nextval('s':::STRING)"))

	tblOldVersion := queryDescVersion("tbl")
	viewOldVersion := queryDescVersion("v")
	tbl2OldVersion := queryDescVersion("tbl2")

	// Upgrade to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`, v1.String())
	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{v1.String()}})

	// Assert the upgrade logic correctly changed the sequence reference from by
	// name to by ID in both the table and view descriptor.
	tdb.QueryRow(t, `SHOW CREATE tbl`).Scan(&table, &createTable)
	require.True(t, strings.Contains(createTable, "j INT8 NOT NULL DEFAULT nextval('public.s'::REGCLASS)"))
	tdb.QueryRow(t, `SHOW CREATE v`).Scan(&table, &createTable)
	require.True(t, strings.Contains(createTable, "SELECT nextval('public.s'::REGCLASS)"))

	// Assert the upgrade logic also correctly skip descriptors that do not
	// reference sequences by name by checking its versions.
	tblNewVersion := queryDescVersion("tbl")
	viewNewVersion := queryDescVersion("v")
	tbl2NewVersion := queryDescVersion("tbl2")
	require.Equal(t, tblOldVersion+1, tblNewVersion)
	require.Equal(t, viewOldVersion+1, viewNewVersion)
	require.Equal(t, tbl2OldVersion, tbl2NewVersion)
}
