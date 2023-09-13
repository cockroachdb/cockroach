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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestGrantExecuteToPublicOnAllFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		v0 = clusterversion.TestingBinaryMinSupportedVersion
		v1 = clusterversion.ByKey(clusterversion.BinaryVersionKey)
	)

	ctx := context.Background()
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
	execStmts := func(t *testing.T, stmts ...string) {
		for _, stmt := range stmts {
			tdb.Exec(t, stmt)
		}
	}
	execStmts(t,
		"CREATE DATABASE test",
		"USE test",
		"CREATE FUNCTION foo() RETURNS INT LANGUAGE SQL AS 'SELECT 1'",
	)

	// Revoke the public execute privileges on the function descriptor.
	fn := desctestutils.TestingGetFunctionDescriptor(kvDB, keys.SystemSQLCodec, "test", "public", "foo")
	descKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, fn.GetID())
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		mut := funcdesc.NewBuilder(fn.FuncDesc()).BuildExistingMutableFunction()
		mut.Privileges.Revoke(
			username.PublicRoleName(),
			privilege.List{privilege.EXECUTE},
			privilege.Function,
			false, /* grantWithOption */
		)
		return txn.Put(ctx, descKey, mut.DescriptorProto())
	}))

	// Upgrade the cluster version.
	tdb.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version()")

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
