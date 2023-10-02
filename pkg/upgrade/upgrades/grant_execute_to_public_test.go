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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGrantExecuteToPublicOnAllFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.BinaryVersionKey)
	)
	ctx := context.Background()

	for numFuncs := range []int{0, 1, 100, 101} {
		settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
		require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
		testServer, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          v0,
				},
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		})
		defer testServer.Stopper().Stop(ctx)

		// Set up the test cluster schema.
		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, "CREATE DATABASE test")
		tdb.Exec(t, "USE test")
		for i := 0; i < numFuncs; i++ {
			tdb.Exec(t, "USE test")
			tdb.Exec(t, fmt.Sprintf("CREATE FUNCTION f%d() RETURNS INT LANGUAGE SQL AS 'SELECT 1'", i))
		}

		// Revoke the public execute privileges on the function descriptors.
		for i := 0; i < numFuncs; i++ {
			funcName := fmt.Sprintf("f%d", i)
			fn := desctestutils.TestingGetFunctionDescriptor(kvDB, keys.SystemSQLCodec, "test", "public", funcName)
			descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, fn.GetID())
			require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				mut := funcdesc.NewBuilder(fn.FuncDesc()).BuildExistingMutableFunction()
				err := mut.Privileges.Revoke(
					username.PublicRoleName(),
					privilege.List{privilege.EXECUTE},
					privilege.Routine,
					false, /* grantWithOption */
				)
				if err != nil {
					return err
				}
				return txn.Put(ctx, descKey, mut.DescriptorProto())
			}))
		}

		// Upgrade the cluster version.
		tdb.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

		for i := 0; i < numFuncs; i++ {
			funcName := fmt.Sprintf("f%d", i)
			fn := desctestutils.TestingGetFunctionDescriptor(kvDB, keys.SystemSQLCodec, "test", "public", funcName)
			descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, fn.GetID())
			require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				v, err := txn.Get(ctx, descKey)
				if err != nil {
					return err
				}
				b, err := descbuilder.FromSerializedValue(v.Value)
				if err != nil {
					return err
				}
				// The function descriptor protobuf should have execute privileges for the
				// public role.
				require.True(t, b.BuildImmutable().GetPrivileges().CheckPrivilege(username.PublicRoleName(), privilege.EXECUTE))
				return nil
			}))
		}
	}
}

func BenchmarkGrantExecuteToPublicOnAllFunctions(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.BinaryVersionKey)
	)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(v1, v0, false /* initializeVersion */)
	require.NoError(b, clusterversion.Initialize(ctx, v0, &settings.SV))
	testServer, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v0,
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer testServer.Stopper().Stop(ctx)

	// Set up the test cluster schema.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(b, "CREATE DATABASE test")
	tdb.Exec(b, "USE test")

	// Create many table descriptors.
	const numTables = 1000
	for i := 0; i < numTables; i++ {
		tdb.Exec(b, fmt.Sprintf("CREATE TABLE t%d (k INT PRIMARY KEY)", i))
	}

	// Create many function descriptors.
	const numFuncs = 1000
	for i := 0; i < numFuncs; i++ {
		tdb.Exec(b, fmt.Sprintf("CREATE FUNCTION f%d() RETURNS INT LANGUAGE SQL AS 'SELECT 1'", i))
	}

	b.ResetTimer()

	// Upgrade the cluster version.
	tdb.Exec(b, "SET CLUSTER SETTING version = crdb_internal.node_executable_version()")
}
