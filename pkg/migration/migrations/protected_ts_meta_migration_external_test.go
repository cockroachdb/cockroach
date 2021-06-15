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
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestProtectedTimestampMetaMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.ProtectedTsMetaPrivilegesMigration - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Replicate bad descriptor privilege bug.
	err := tc.Servers[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		id := systemschema.ProtectedTimestampsMetaTable.GetID()
		mut, err := catalogkv.MustGetMutableTableDescByID(ctx, txn, keys.SystemSQLCodec, id)
		if err != nil {
			return err
		}
		mut.Version = 1
		mut.Privileges = descpb.NewCustomSuperuserPrivilegeDescriptor(
			descpb.SystemAllowedPrivileges[keys.ReplicationStatsTableID], security.NodeUserName())
		b := txn.NewBatch()
		b.Put(catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, id), mut.DescriptorProto())
		return txn.Run(ctx, b)
	})
	require.NoError(t, err)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	checkPrivileges := func(expectedPrivileges int, expectedVersion int) {
		expectedStr := fmt.Sprintf(
			`{"ownerProto": "node", "users": [`+
				`{"privileges": %d, "userProto": "admin"}, `+
				`{"privileges": %d, "userProto": "root"}`+
				`], "version": 2}`,
			expectedPrivileges,
			expectedPrivileges,
		)
		var actualStr string
		tdb.QueryRow(t, `
			SELECT
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor,
					false
				)->'table'->>'privileges'
			FROM system.descriptor WHERE id = 31
		`).Scan(&actualStr)
		require.EqualValues(t, expectedStr, actualStr)
		var actualVersionStr string
		tdb.QueryRow(t, `
			SELECT
				crdb_internal.pb_to_json(
					'cockroach.sql.sqlbase.Descriptor',
					descriptor,
					false
				)->'table'->>'version'
			FROM system.descriptor WHERE id = 31
		`).Scan(&actualVersionStr)
		require.EqualValues(t, strconv.Itoa(expectedVersion), actualVersionStr)
	}

	checkPrivileges(496, 1)

	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.ProtectedTsMetaPrivilegesMigration).String())

	checkPrivileges(48, 2)
}
