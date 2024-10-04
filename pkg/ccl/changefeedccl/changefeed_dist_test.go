// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDistSenderAllRangeSpans tests (*distserver).AllRangeSpans.
func TestDistSenderAllRangeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const nodes = 1
	args := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestTenantProbabilisticOnly,
		},
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, args)
	defer tc.Stopper().Stop(ctx)

	node := tc.Server(0).ApplicationLayer()
	systemLayer := tc.Server(0).SystemLayer()
	sqlDB := sqlutils.MakeSQLRunner(node.SQLConn(t))
	distSender := node.DistSenderI().(*kvcoord.DistSender)

	tenantPrefix := ""
	if tc.StartedDefaultTestTenant() {
		tenantPrefix = "/Tenant/10"
	}

	// Use manual merging/splitting only.
	tc.ToggleReplicateQueues(false)

	mergeRange := func(key interface{}) {
		err := systemLayer.DB().AdminMerge(ctx, key)
		if err != nil {
			if !strings.Contains(err.Error(), "cannot merge final range") {
				t.Fatal(err)
			}
		}
	}
	getTableSpan := func(tableName string) roachpb.Span {
		desc := desctestutils.TestingGetPublicTableDescriptor(
			node.DB(), node.Codec(), "defaultdb", "a")
		return desc.PrimaryIndexSpan(node.Codec())
	}
	getTableDesc := func(tableName string) catalog.TableDescriptor {
		return desctestutils.TestingGetPublicTableDescriptor(
			node.DB(), node.Codec(), "defaultdb", "a")
	}

	// Regression test for the issue in #117286.
	t.Run("returned range does not exceed input span boundaries", func(t *testing.T) {
		// Merge 3 tables into one range.
		sqlDB.Exec(t, "create table a (i int primary key)")
		sqlDB.Exec(t, "create table b (j int primary key)")
		sqlDB.Exec(t, "create table c (k int primary key)")
		mergeRange(getTableSpan("a").Key)
		mergeRange(getTableSpan("b").Key)

		bTableID := getTableDesc("b").GetID()
		rangeSpans, _, err := distSender.AllRangeSpans(ctx, []roachpb.Span{getTableSpan("b")})
		require.NoError(t, err)

		// Assert that the returned range is trimmed, so it only contains spans from "b" and not the entire
		// range containing "a" and "c".
		require.Equal(t, 1, len(rangeSpans), fmt.Sprintf("%v", rangeSpans))
		require.Equal(t, fmt.Sprintf("%s/Table/%d/{1-2}", tenantPrefix, bTableID), rangeSpans[0].String())
	})

}
