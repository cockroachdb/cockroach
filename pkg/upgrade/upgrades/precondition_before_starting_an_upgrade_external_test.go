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
	gosql "database/sql"
	"encoding/hex"
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

// TestPreconditionBeforeStartingAnUpgrade tests that all defined preconditions
// must be met before starting an upgrade.
func TestPreconditionBeforeStartingAnUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.ByKey(clusterversion.TODODelete_V22_2Start - 1)
		v1 = clusterversion.ByKey(clusterversion.TODODelete_V22_2Start)
	)

	ctx := context.Background()
	type testSetup struct {
		parentID       descpb.ID
		parentSchemaID descpb.ID
		sqlDB          *gosql.DB
		tdb            *sqlutils.SQLRunner
		cleanup        func()
	}
	setupTestCluster := func() testSetup {
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
		sqlDB := tc.ServerConn(0)
		tdb := sqlutils.MakeSQLRunner(sqlDB)
		var parentID, parentSchemaID descpb.ID
		tdb.Exec(t, "CREATE TABLE temp_tbl()")
		tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
			Scan(&parentID, &parentSchemaID)
		return testSetup{
			parentID:       parentID,
			parentSchemaID: parentSchemaID,
			sqlDB:          sqlDB,
			tdb:            tdb,
			cleanup:        func() { tc.Stopper().Stop(ctx) },
		}
	}

	// One subtest for each precondition we wish to test.
	t.Run("upgrade fails if there exists invalid descriptors", func(t *testing.T) {
		/*
				The hex for the descriptor to inject was created by running the following
				commands in a 21.2 binary.
				At the time this is written, `CREATE MATERIALIZED VIEW` has a bug that if
				it fails, it left dangling back-references in the table `t`. Injecting `t`
				and later attempting to upgrade will trigger the precondition check where
				we can expect a failure.

						CREATE TABLE t (i INT PRIMARY KEY);
						INSERT INTO t VALUES (1);
						CREATE MATERIALIZED VIEW v AS (SELECT i/0 FROM t);

						SELECT encode(descriptor, 'hex')
						FROM system.descriptor
						WHERE id = (
								SELECT id
								FROM system.namespace
								WHERE name = 't'
						);

			NB: As of 07/15/22 the injected descriptor has been edited to have an ID
			104 so that it does collide with system tables that are allowed IDs below
			100.
		*/
		ts := setupTestCluster()
		defer ts.cleanup()
		const tableDescriptorToInject = "0a85020a01741868203228023a0042260a016910011a0c080110401800300050146000200030006800700078008001008801009801004802524c0a077072696d61727910011801220169300140004a10080010001a00200028003000380040005a007a0408002000800100880100900104980101a20106080012001800a80100b20100ba010060026a1d0a090a0561646d696e10020a080a04726f6f7410021204726f6f741802800101880103980100b201120a077072696d61727910001a016920012800b80101c20100d201080835100018012000e80100f2010408001200f801008002009202009a020a08f084c3bfb1c1ccfe16b20200b80200c0021dc80200e00200f00200"

		// Decode and insert the table descriptor.
		decodeTableDescriptorAndInsert(t, ctx, ts.sqlDB, tableDescriptorToInject, ts.parentID, ts.parentSchemaID)

		// Attempt to upgrade the cluster version and expect to see a failure
		_, err := ts.sqlDB.Exec(`SET CLUSTER SETTING version = $1`, v1.String())
		require.Error(t, err, "upgrade should be refused because precondition is violated.")
		require.Equal(t, "pq: verifying precondition for version 22.1-2: "+
			"there exists invalid descriptors as listed below; fix these descriptors before attempting to upgrade again:\n"+
			"invalid descriptor: defaultdb.public.temp_tbl (104) because 'mismatched name \"t\" in relation descriptor'",
			strings.ReplaceAll(err.Error(), "1000022", "22"))
		// The cluster version should remain at `v0`.
		ts.tdb.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v0.String()}})
	})
	t.Run("upgrade correctly identifies broken userfiles", func(t *testing.T) {
		ts := setupTestCluster()
		defer ts.cleanup()

		decodeTableDescriptorAndInsert(t, ctx, ts.sqlDB, brokenUserfileFilesTable, ts.parentID, ts.parentSchemaID)
		decodeTableDescriptorAndInsert(t, ctx, ts.sqlDB, brokenUserfilePayloadTable, ts.parentID, ts.parentSchemaID)

		_, err := ts.sqlDB.Exec(`SET CLUSTER SETTING version = $1`, v1.String())
		require.NoError(t, err)
		ts.tdb.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v1.String()}})
	})
	// other preconditions to test here, one per `t.Run()`.
}

func decodeTableDescriptorAndInsert(
	t *testing.T,
	ctx context.Context,
	sqlDB *gosql.DB,
	hexEncodedDescriptor string,
	parentID, parentSchemaID descpb.ID,
) {
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
