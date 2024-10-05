// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc_test

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// This file complements "validate_test.go" file but is dedicated to testing
// validation logic added with a version gate. In general, adding new validation
// should almost always come with an accompanying version gate, so that the
// validation is disabled when the cluster is in a mixed version state. This
// helps avoid the disastrous situation where an existing corruption is caught
// by the newly added validation, and it starts to scream and block user
// workload.
//
// This test file therefore should contain tests where, if we added some
// validation logic and gated it behind a cluster version, then we ensure a
// mixed version cluster with such a corruption
//   1. can accept SQL reads/writes
//   2. can report this corruption from `invalid_objects` vtable
//   3. cannot upgrade the cluster version (because the precondition check on `invalid_objects` fails)

// TestIndexDoesNotStorePrimaryKeyColumnMixedVersion tests the validation that
// any active index does not store primary key column is properly gated behind
// V24.1.
func TestIndexDoesNotStorePrimaryKeyColumnMixedVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a test cluster whose cluster version is MinSupported and auto-upgrade is
	// disabled.
	v0 := clusterversion.MinSupported
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		v0.Version(),
		false, // initializeVersion
	)
	// Initialize the version to v0.
	require.NoError(t, clusterversion.Initialize(ctx, v0.Version(), &settings.SV))

	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v0.Version(),
			},
		},
	})
	defer ts.Stopper().Stop(ctx)

	sqlDB := ts.SQLConn(t, serverutils.DBName(catalogkeys.DefaultDatabaseName))
	defer sqlDB.Close()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	/*
		The hex for the descriptor to inject was created by running the following
		commands in a 23.2 binary, in which corruption #115214 could happen.

				CREATE TABLE t (i INT PRIMARY KEY, j INT NOT NULL, k INT NOT NULL);
				CREATE INDEX idx ON t(k) STORING (j);
				ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (j);

				SELECT encode(descriptor, 'hex')
				FROM system.descriptor
				WHERE id = (
						SELECT id
						FROM system.namespace
						WHERE name = 't'
				);
	*/

	// Deserialize and insert the corrupted descriptor to cluster.
	const corruptDescToInject = "0ade040a01741868206428133a0042260a016910011a0c0801104018003000501460002000300068007000780080010088010098010042260a016a10021a0c0801104018003000501460002000300068007000780080010088010098010042260a016b10031a0c080110401800300050146000200030006800700078008001008801009801004804526b0a06745f706b65791008180122016a2a01692a016b300240004a10080010001a00200028003000380040005a00700170037a0408002000800100880101900104980101a20106080012001800a80100b20100ba0100c00100c80100d00106e00100e90100000000000000005a6b0a036964781004180022016b2a016a300340004a10080010001a00200028003000380040005a0070027a0408002000800100880101900104980100a20106080012001800a80100b20100ba0100c00100c801f0bc93e0a5c788d017d00102e00100e90100000000000000005a6c0a07745f695f6b6579100618012201693001380240004a10080010001a00200028003000380040005a007a0408002000800100880101900104980100a20106080012001800a80100b20100ba0100c00100c801d8b39aa5a6c788d017d00104e00100e9010000000000000000600a6a210a0b0a0561646d696e100218020a0a0a04726f6f74100218021204726f6f741803800101880103980100b2011c0a077072696d61727910001a01691a016a1a016b2001200220032800b80101c20100e80100f2010408001200f801008002009202009a020a08d0d0d0febfc688d017b20200b80200c00265c80200e00200800300880308a80300b00300d00300"
	mustInsertDescToDB(ctx, t, sqlDB, corruptDescToInject)

	// Assert table is readable/writable.
	tdb.Exec(t, "INSERT INTO t VALUES (1,2,3);")
	require.Equal(t, [][]string{{"1", "2", "3"}}, tdb.QueryStr(t, "SELECT * FROM t;"))

	// Assert table corruption is reported by `invalid_objects`.
	require.Equal(t, [][]string{{"t", `relation "t" (104): index "idx" already contains column "j"`}},
		tdb.QueryStr(t, "SELECT obj_name, error FROM crdb_internal.invalid_objects;"))

	// Assert cluster version upgrade is blocked.
	require.Equal(t, [][]string{{"23.1"}}, tdb.QueryStr(t, "SHOW CLUSTER SETTING version;"))
	// disable AOST for upgrade precondition check to ensure it sees the injected, corrupt descriptor.
	defer upgrades.TestingSetFirstUpgradePreconditionAOST(false)()
	_, err := sqlDB.Exec(`SET CLUSTER SETTING version = $1`, clusterversion.Latest.String())
	require.ErrorContains(t, err, `verifying precondition for version 23.1-upgrading-to-23.2-step-002: "".crdb_internal.invalid_objects is not empty`)
}

// mustInsertDescToDB decode a table descriptor from a hex-encoded string and insert
// it into the database.
func mustInsertDescToDB(
	ctx context.Context, t *testing.T, db *gosql.DB, hexEncodedDescriptor string,
) {
	tdb := sqlutils.MakeSQLRunner(db)
	var parentID, parentSchemaID descpb.ID
	tdb.Exec(t, "CREATE TABLE temp_tbl()")
	tdb.QueryRow(t, `SELECT "parentID", "parentSchemaID" FROM system.namespace WHERE name = 'temp_tbl'`).
		Scan(&parentID, &parentSchemaID)
	tdb.Exec(t, `DROP TABLE temp_tbl;`)

	// Decode descriptor
	decodedDescriptor, err := hex.DecodeString(hexEncodedDescriptor)
	require.NoError(t, err)
	b, err := descbuilder.FromBytesAndMVCCTimestamp(decodedDescriptor, hlc.Timestamp{WallTime: 1})
	require.NoError(t, err)
	tableDesc := b.(tabledesc.TableDescriptorBuilder).BuildCreatedMutableTable()

	// Modify this descriptor's parentID and parentSchemaID as it could be
	// different from when the descriptor was serialized.
	tableDesc.ParentID = parentID
	tableDesc.UnexposedParentSchemaID = parentSchemaID

	// Insert the descriptor into test cluster.
	require.NoError(t, sqlutils.InjectDescriptors(
		ctx, db, []*descpb.Descriptor{tableDesc.DescriptorProto()}, true, /* force */
	))
}
