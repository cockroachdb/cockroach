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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// The hex for the descriptor to inject was created by running the following
// commands in a 22.1 binary.
//
// In 22.1 and prior, userfile creation had a bug that produced a foreign
// key constraint mutation without an associated mutation job.
//
//	   CREATE DATABASE to_backup;
//	   BACKUP DATABASE to_backup INTO 'userfile://defaultdb.test/data';
//
//	   SELECT encode(descriptor, 'hex')
//	   FROM system.descriptor
//	   WHERE id = (
//						SELECT id
//						FROM system.namespace
//						WHERE name = 'test_upload_payload'
//	   );
//	   SELECT encode(descriptor, 'hex')
//	   FROM system.descriptor
//	   WHERE id = (
//						SELECT id
//						FROM system.namespace
//						WHERE name = 'test_upload_files'
//	   );
//
// The files table is not broken, but should probably be inserted for completeness.
//
// These are shared with the tests for the upgrade preconditions.
const (
	brokenUserfilePayloadTable = "0ac4040a13746573745f75706c6f61645f7061796c6f6164186b206428033a00422d0a0766696c655f696410011a0d080e10001800300050861760002000300068007000780080010088010098010042300a0b627974655f6f666673657410021a0c08011040180030005014600020003000680070007800800100880100980100422c0a077061796c6f616410031a0c0808100018003000501160002001300068007000780080010088010098010048045290010a18746573745f75706c6f61645f7061796c6f61645f706b657910011801220766696c655f6964220b627974655f6f66667365742a077061796c6f616430013002400040004a10080010001a00200028003000380040005a0070037a0408002000800100880100900104980101a20106080012001800a80100b20100ba0100c00100c80188a5978797a6b68c17d0010160026a210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f74180272541801200128013800424a0801120a66696c655f69645f666b1a0c0a0012001800300038004003221e086b10011802206a2a0a66696c655f69645f666b3002380040004800700330003a0a08001a0020002a003003800102880103980100b201320a077072696d61727910001a0766696c655f69641a0b627974655f6f66667365741a077061796c6f61642001200220032803b80101c20100da010c080110818088ef93a5db8d0be80100f2010408001200f801008002009202009a020a0888a5978797a6b68c17b20200b80200c00265c80200e00200800300880304"
	brokenUserfileFilesTable   = "0a91060a11746573745f75706c6f61645f66696c6573186a206428033a00422d0a0866696c656e616d6510011a0c0807100018003000501960002000300068007000780080010088010098010042400a0766696c655f696410021a0d080e100018003000508617600020002a1167656e5f72616e646f6d5f7575696428293000680070007800800100880100980100422e0a0966696c655f73697a6510031a0c08011040180030005014600020003000680070007800800100880100980100422d0a08757365726e616d6510041a0c0807100018003000501960002000300068007000780080010088010098010042440a0b75706c6f61645f74696d6510051a0d080510001800300050da08600020012a116e6f7728293a3a3a54494d455354414d503000680070007800800100880100980100480652a6010a16746573745f75706c6f61645f66696c65735f706b657910011801220866696c656e616d652a0766696c655f69642a0966696c655f73697a652a08757365726e616d652a0b75706c6f61645f74696d65300140004a10080010001a00200028003000380040005a0070027003700470057a0408002000800100880100900104980101a20106080012001800a80100b20100ba0100c00100c80188a5978797a6b68c17d001025a7b0a1d746573745f75706c6f61645f66696c65735f66696c655f69645f6b657910021801220766696c655f69643002380140004a10080010001a00200028003000380040005a007a0408002000800100880100900103980100a20106080012001800a80100b20100ba0100c00100c80188a5978797a6b68c17d0010160036a210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f741802800101880103980100b2014c0a077072696d61727910001a0866696c656e616d651a0766696c655f69641a0966696c655f73697a651a08757365726e616d651a0b75706c6f61645f74696d65200120022003200420052800b80101c20100e80100f2010408001200f801008002009202009a020a0888a5978797a6b68c17b20200b80200c00265c80200e00200800300880303"
)

// TestFixUserfileRelatedDescriptorCorruptionUpgrade tests that we correctly repair
// a broken userfile table.
func TestFixUserfileRelatedDescriptorCorruptionUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.TODODelete_V22_2FixUserfileRelatedDescriptorCorruption - 1)
		v1 = clusterversion.ByKey(clusterversion.TODODelete_V22_2FixUserfileRelatedDescriptorCorruption)
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

	var parentID, parentSchemaID descpb.ID
	tdb.Exec(t, "CREATE TABLE temp_tbl()")
	tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
		Scan(&parentID, &parentSchemaID)

	decodeTableDescriptorAndInsert(t, ctx, sqlDB, brokenUserfileFilesTable, parentID, parentSchemaID)
	decodeTableDescriptorAndInsert(t, ctx, sqlDB, brokenUserfilePayloadTable, parentID, parentSchemaID)
	var count int
	err := sqlDB.QueryRow(`SELECT count(1) FROM "".crdb_internal.invalid_objects`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = sqlDB.Exec(`SET CLUSTER SETTING version = $1`, v1.String())
	require.NoError(t, err)
	err = sqlDB.QueryRow(`SELECT count(1) FROM "".crdb_internal.invalid_objects`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
	tdb.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v1.String()}})
}
