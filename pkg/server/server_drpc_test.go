// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSelectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Run the test with both DRPC enabled and disabled.
	testutils.RunTrueAndFalse(t, "enableDRPC", func(t *testing.T, enableDRPC bool) {
		envutil.TestSetEnv(t, "COCKROACH_EXPERIMENTAL_DRPC_ENABLED", strconv.FormatBool(enableDRPC))
		ctx := context.Background()

		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Insecure: true,
			},
		})
		defer tc.Stopper().Stop(ctx)

		db := tc.ServerConn(1)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		rows, err := db.QueryContext(ctx, "SELECT COUNT(*) FROM system.tenants")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var count int
			require.NoError(t, rows.Scan(&count))
			require.Equal(t, 1, count)
		}
		require.NoError(t, rows.Close())
	})
}
