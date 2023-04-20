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
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestJSONForwardingIndexes(t *testing.T) {
	var err error
	skip.UnderStressRace(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false,
	)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()

	// Setting an older cluster version and expecting an error when creating
	// forward indexes on JSON columns.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_1WebSessionsTableHasUserIDColumn).String())
	require.NoError(t, err)

	_, err = db.Exec(`CREATE DATABASE test`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE test.hello (
    key INT PRIMARY KEY,
    j JSONB
)`)
	require.NoError(t, err)

	// Creating an index on the JSON column should result in an error.
	_, err = db.Exec(`CREATE INDEX on test.hello (j)`)
	require.Error(t, err)

	// Creating an JSON expression index should result in an error.
	_, err = db.Exec(`CREATE INDEX on test.hello ((j->'a'))`)
	require.Error(t, err)

	// Creating a primary key on a JSON column should result in an error.
	_, err = db.Exec(`CREATE TABLE test.new (
	   key JSONB PRIMARY KEY
	)`)
	require.Error(t, err)

	// Updating the cluster version to allow creation of forward indexes
	// on JSON columns.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.V23_2_JSONForwardIndexes).String())
	require.NoError(t, err)

	_, err = db.Exec(`CREATE INDEX on test.hello (j)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test.hello VALUES (1, '[1, 2, 3]'::JSONB)`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE INDEX on test.hello ((j->'a'))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test.hello VALUES (2, '{"a": "b"}'::JSONB)`)
	require.NoError(t, err)

	// Creating a primary key on a JSON column.
	_, err = db.Exec(`CREATE TABLE test.new (
	   key JSONB PRIMARY KEY
	)`)
	require.NoError(t, err)
}
