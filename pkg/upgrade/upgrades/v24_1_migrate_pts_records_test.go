// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	// 10 changefeeds each protect 2 descriptors: data table n + the descs table.
	// An additional changefeed protects all 10 data tables + the descs table.`
	var allTargets [][]catid.DescID
	var descIDsArr []catid.DescID
	var allTables []string
	for i := 0; i < 10; i++ {
		tbl := fmt.Sprintf("table_%d", i)
		_, err = sqlDB.Exec(fmt.Sprintf("create table %s (i int)", tbl))
		require.NoError(t, err)
		_, err = sqlDB.Exec(fmt.Sprintf("create changefeed for %s INTO 'null://'", tbl))
		require.NoError(t, err)

		tableDesc := desctestutils.TestingGetTableDescriptor(
			s.DB(), execCfg.Codec, "defaultdb", "public", tbl,
		)
		allTargets = append(allTargets, []catid.DescID{
			keys.DescriptorTableID,
			keys.UsersTableID,
			keys.ZonesTableID,
			keys.RoleMembersTableID,
			keys.CommentsTableID,
			tableDesc.GetID()})
		descIDsArr = append(descIDsArr, tableDesc.GetID())
		allTables = append(allTables, tbl)
	}
	_, err = sqlDB.Exec(fmt.Sprintf("create changefeed for %s INTO 'null://'", strings.Join(allTables, ",")))
	require.NoError(t, err)
	descIDsArr = append(descIDsArr,
		keys.DescriptorTableID,
		keys.UsersTableID,
		keys.ZonesTableID,
		keys.RoleMembersTableID,
		keys.CommentsTableID)
	sort.Slice(descIDsArr, func(i int, j int) bool {
		return descIDsArr[i] < descIDsArr[j]
	})
	allTargets = append(allTargets, descIDsArr)

	// Ensure that old-style PTS records exist.
	var count int
	err = sqlDB.QueryRow("SELECT count(*) FROM system.protected_ts_records WHERE target is NULL").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 11, count)

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
		// Sort the changefeed which protects more than two descriptors last.
		if len(a) < len(b) {
			return true
		}
		if len(b) < len(a) {
			return false
		}
		// Since each slice was sorted, the data data should
		// be last. Sort on its descriptor ID.
		return a[len(a)-1] < b[len(b)-1]
	})

	require.ElementsMatch(t, allTargets, seenTargets)
}
