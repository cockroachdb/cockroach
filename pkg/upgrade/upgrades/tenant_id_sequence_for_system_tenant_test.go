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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantIDSequenceForSystemTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.BinaryMinSupportedVersionKey),
					BootstrapVersionKeyOverride:    clusterversion.BinaryMinSupportedVersionKey,
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)
	require.True(t, s.ExecutorConfig().(sql.ExecutorConfig).Codec.ForSystemTenant())

	// Advance to the version immediately before the one that
	// interests us.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1_TenantIDSequence-1,
		nil,
		false,
	)

	// Check that the sequence does not exist.
	var seqVal int
	err := sqlDB.QueryRow(`SELECT last_value FROM system.public.tenant_id_seq`).Scan(&seqVal)
	require.Error(t, err)

	// Introduce the sequence.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1_TenantIDSequence,
		nil,
		false,
	)

	err = sqlDB.QueryRow(`SELECT last_value FROM system.public.tenant_id_seq`).Scan(&seqVal)
	require.NoError(t, err)
	require.Equal(t, 1, seqVal)
}
