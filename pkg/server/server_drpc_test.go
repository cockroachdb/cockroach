// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDRPCSelectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		ctx, cancel := context.WithTimeout(context.Background(), testutils.SucceedsSoonDuration())
		defer cancel()

		st := cluster.MakeTestingClusterSettings()
		rpc.ExperimentalDRPCEnabled.Override(ctx, &st.SV, true)

		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
				Insecure: insecure,
			},
		})
		defer tc.Stopper().Stop(ctx)

		idx := rand.Intn(tc.NumServers())
		t.Logf("querying from node %d", idx+1)
		db := tc.ServerConn(idx)
		defer db.Close()

		rows, err := db.QueryContext(ctx, "SELECT count(*) FROM system.tenants")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var count int
			require.NoError(t, rows.Scan(&count))
			require.Equal(t, 1, count)
		}
		require.NoError(t, rows.Err())
	})
}
