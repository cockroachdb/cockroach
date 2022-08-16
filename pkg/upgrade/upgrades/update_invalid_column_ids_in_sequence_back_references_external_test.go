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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestUpgradeSeqToBeReferencedByID tests that sequence references by name will be upgraded
// to be by ID in tables or views.
func TestUpdateInvalidColumnIDsInSequenceBackReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.UpdateInvalidColumnIDsInSequenceBackReferences - 1)
		v1 = clusterversion.ByKey(clusterversion.UpdateInvalidColumnIDsInSequenceBackReferences)
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
					commands in a 21.1 binary, in which `ADD COLUMN DEFAULT nextval('s')`
				  incorrectly store a 0-value column ID in the sequence's DependedOnBy because
			    we'd add the dependency before allocating an ID to the column.

							CREATE SEQUENCE s;
							CREATE TABLE tbl (i INT PRIMARY KEY);
		          ALTER TABLE tbl ADD COLUMN j INT DEFAULT nextval('s');

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
	*/

	var parentID, parentSchemaID descpb.ID
	tdb.Exec(t, "CREATE TABLE temp_tbl()")
	tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
		Scan(&parentID, &parentSchemaID)
	const sequenceDescriptorToInject = "0aa5020a01731834203228023a0042240a0576616c756510011a0c08011040180030005014600020003000680070007800800100480052500a077072696d61727910011800220576616c7565300140004a10080010001a00200028003000380040005a007a0408002000800100880100900100980100a20106080012001800a80100b20100ba010060006a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801800100880103980100b201160a077072696d61727910001a0576616c756520012801b80100c20100d201080835100018002001e2011a0801100118ffffffffffffffff7f200128003204080010003801e80100f2010408001200f801008002009202009a020a08d0b197c78df6f98517b20200b80200c0021dc80200e00200"
	const tableDescriptorToInject = "0ab4020a0374626c1835203228043a0042200a016910011a0c08011040180030005014600020003000680070007800800100423a0a016a10021a0c08011040180030005014600020012a166e65787476616c2835323a3a3a524547434c41535329300050346800700078008001004803524c0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a0408002000800100880100900103980100a20106080012001800a80100b20100ba010060026a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741801800102880103980100b201170a077072696d61727910001a01691a016a200120022802b80101c20100e80100f2010408001200f801008002009202009a020a08888d94b9b0f6f98517b20200b80200c0021dc80200e00200"

	// A function that decode a table descriptor from a hex-encoded string and insert it into the test cluster.
	decodeTableDescriptorAndInsert := func(hexEncodedDescriptor string) {
		var desc descpb.Descriptor
		decodedDescriptor, err := hex.DecodeString(hexEncodedDescriptor)
		require.NoError(t, err)
		require.NoError(t, protoutil.Unmarshal(decodedDescriptor, &desc))
		// Run post deserialization changes.
		b := descbuilder.NewBuilderWithMVCCTimestamp(&desc, hlc.Timestamp{WallTime: 1})
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

	// Decode and insert the sequence descriptor and table descriptor.
	decodeTableDescriptorAndInsert(sequenceDescriptorToInject)
	decodeTableDescriptorAndInsert(tableDescriptorToInject)

	// Assert that the sequence has a back reference with invalid column ID.
	query := "select crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor)->'table'->'dependedOnBy' from system.descriptor where id = 's'::REGCLASS;"
	var dependedOnBy string
	tdb.QueryRow(t, query).Scan(&dependedOnBy)
	require.True(t, strings.Contains(dependedOnBy, "\"columnIds\": [0]"))

	// Upgrade to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`, v1.String())
	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{v1.String()}})

	// Assert the upgrade logic correctly updated the invalid column ID in the sequence
	// back reference.
	tdb.QueryRow(t, query).Scan(&dependedOnBy)
	require.True(t, strings.Contains(dependedOnBy, "\"columnIds\": [2]"))
}
