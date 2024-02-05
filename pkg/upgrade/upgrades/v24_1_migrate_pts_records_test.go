// Copyright 2024 The Cockroach Authors.
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
	"slices"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMigrateOldStlePTSRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Enable changefeeds.
	defer ccl.TestingEnableEnterprise()()

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				ProtectedTS: &protectedts.TestingKnobs{
					WriteDeprecatedPTSRecords: true},
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc      = testcluster.StartTestCluster(t, 1, clusterArgs)
		s       = tc.Server(0)
		sqlDB   = tc.ServerConn(0)
		execCfg = s.ExecutorConfig().(sql.ExecutorConfig)
	)
	defer tc.Stopper().Stop(ctx)
	require.True(t, execCfg.Codec.ForSystemTenant())

	var err error

	// Rangefeeds are required for changefeeds.
	_, err = sqlDB.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	// Create changefeeds and store the protected descriptor ids.
	var allTargets [][]catid.DescID
	var descIDsArr []catid.DescID
	for i := 0; i < 10; i++ {
		_, err = sqlDB.Exec(fmt.Sprintf("create table table_%d (i int)", i))
		require.NoError(t, err)
		_, err = sqlDB.Exec(fmt.Sprintf("create changefeed for table_%d INTO 'null://'", i))
		require.NoError(t, err)

		tableDesc := desctestutils.TestingGetTableDescriptor(
			s.DB(), execCfg.Codec, "defaultdb", "public", fmt.Sprintf("table_%d", i),
		)
		allTargets = append(allTargets, []catid.DescID{keys.DescriptorTableID, tableDesc.GetID()})
		descIDsArr = append(descIDsArr, tableDesc.GetID())
	}
	_, err = sqlDB.Exec("create changefeed for table_0, table_1, table_2, table_3, table_4," +
		"table_5, table_6, table_7, table_8, table_9 INTO 'null://'")
	require.NoError(t, err)
	descIDsArr = append(descIDsArr, keys.DescriptorTableID)
	sort.Slice(descIDsArr, func(i int, j int) bool {
		return descIDsArr[i] < descIDsArr[j]
	})
	allTargets = append(allTargets, descIDsArr)

	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V24_1_MigrateOldStylePTSRecords,
		nil,
		false,
	)

	// Read the PTS state after the upgrade. Sort them so they
	// can be compared to the list of expected targets.
	var state ptpb.State
	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		state, err = execCfg.ProtectedTimestampProvider.WithTxn(txn).GetState(ctx)
		return err
	})
	require.NoError(t, err)
	var seenTargets [][]catid.DescID
	for _, s := range state.Records {
		require.Equal(t, 0, len(s.DeprecatedSpans))
		ids := s.Target.GetSchemaObjects().IDs
		sort.Slice(ids, func(i int, j int) bool {
			return ids[i] < ids[j]
		})
		seenTargets = append(seenTargets, ids)
	}
	sort.Slice(seenTargets, func(i int, j int) bool {
		a := seenTargets[i]
		b := seenTargets[j]
		if len(a) < len(b) {
			return true
		}
		if len(b) < len(a) {
			return false
		}
		return a[1] < b[1]
	})
	slices.SortFunc(seenTargets, func(a, b []catid.DescID) int {
		if len(a) < len(b) {
			return -1
		}
		if len(b) < len(a) {
			return 1
		}
		if a[1] < b[1] {
			return -1
		}
		return 1
	})

	t.Logf("%v", allTargets)
	t.Logf("%v", seenTargets)
	require.Equal(t, seenTargets, allTargets)
}
