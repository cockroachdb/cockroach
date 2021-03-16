// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestWorkload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderStressRace(t, "times out")

	dir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			ExternalIODir: dir,
		},
	})

	defer tc.Stopper().Stop(ctx)
	m, err := workload.Get("schemachange")
	require.NoError(t, err)
	wl := m.New().(interface {
		workload.Opser
		workload.Flagser
	})
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	reg := histogram.NewRegistry(20 * time.Second)
	tdb.Exec(t, "CREATE USER testuser")
	tdb.Exec(t, "CREATE DATABASE schemachange")
	tdb.Exec(t, "GRANT admin TO testuser")
	tdb.Exec(t, "SET CLUSTER SETTING sql.trace.log_statement_execute = true")

	// Grab a backup and also print the namespace and descriptor tables upon
	// failure.
	// It's not clear how helpful this actually is but it doesn't hurt.
	printRows := func(rows *gosql.Rows) {
		t.Helper()
		mat, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		fmt.Printf("rows:\n%s", sqlutils.MatrixToStr(mat))
	}
	defer func() {
		if !t.Failed() {
			return
		}
		printRows(tdb.Query(t, "SELECT id, encode(descriptor, 'hex') FROM system.descriptor"))
		printRows(tdb.Query(t, "SELECT * FROM system.namespace"))
		tdb.Exec(t, "BACKUP DATABASE schemachange TO 'nodelocal://0/backup'")
		t.Logf("backup in %s", dir)
	}()

	pgURL, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(), t.Name(), url.User("testuser"))
	defer cleanup()

	const concurrency = 2
	require.NoError(t, wl.Flags().Parse([]string{
		"--concurrency", strconv.Itoa(concurrency),
		"--verbose", "2",
	}))

	ql, err := wl.Ops(ctx, []string{pgURL.String()}, reg)
	require.NoError(t, err)

	const N = 100
	workerFn := func(ctx context.Context, fn func(ctx context.Context) error) func() error {
		return func() error {
			for i := 0; i < N; i++ {
				if err := fn(ctx); err != nil || ctx.Err() != nil {
					return err
				}
			}
			return nil
		}
	}
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		g.Go(workerFn(gCtx, ql.WorkerFns[i]))
	}
	require.NoError(t, g.Wait())
}
