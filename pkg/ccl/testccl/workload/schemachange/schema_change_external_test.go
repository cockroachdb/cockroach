// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	_ "github.com/cockroachdb/cockroach/pkg/workload/schemachange"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestWorkload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ccl.TestingEnableEnterprise()()
	skip.UnderDeadlock(t, "test connections can be too slow under expensive configs")
	skip.UnderRace(t, "test connections can be too slow under expensive configs")

	scope := log.Scope(t)
	defer scope.Close(t)
	dir := scope.GetDirectory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // NOTE: Required to cleanup dnscache refresh Go routine

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t,
		3, /* numServers */
		base.TestingKnobs{},
		multiregionccltestutils.WithBaseDirectory(dir),
	)
	defer cleanup()

	m, err := workload.Get("schemachange")
	require.NoError(t, err)
	wl := m.New().(interface {
		workload.Opser
		workload.Flagser
	})
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	reg := histogram.NewRegistry(20*time.Second, m.Name)
	tdb.Exec(t, "CREATE USER testuser")
	tdb.Exec(t, "CREATE DATABASE schemachange")
	tdb.Exec(t, "GRANT admin TO testuser")
	tdb.Exec(t, "SET CLUSTER SETTING sql.log.all_statements.enabled = true")

	dumpRows := func(name string, rows *gosql.Rows) {
		t.Helper()
		mat, err := sqlutils.RowsToStrMatrix(rows)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(fmt.Sprintf("%s/%s.rows", dir, name), []byte(sqlutils.MatrixToStr(mat)), 0666))
	}

	// Grab a backup, dump the namespace and descriptor tables upon failure.
	defer func() {
		if !t.Failed() {
			return
		}
		// Dump namespace and descriptor in their raw format. This is useful for
		// processing results with some degree of scripting.
		dumpRows("namespace", tdb.Query(t, `SELECT * FROM system.namespace`))
		dumpRows("descriptor", tdb.Query(t, "SELECT id, encode(descriptor, 'hex') FROM system.descriptor"))
		// Dump out a more human readable version of the above as well to allow for
		// easy debugging by hand.
		// NB: A LEFT JOIN is used here because not all descriptors (looking at you
		// functions) have namespace entries.
		dumpRows("ns-desc-json", tdb.Query(t, `
			SELECT
				"parentID",
				"parentSchemaID",
				descriptor.id,
				name,
				crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor)
				FROM system.descriptor
				LEFT JOIN system.namespace ON namespace.id = descriptor.id
		`))
		tdb.Exec(t, "BACKUP DATABASE schemachange INTO 'nodelocal://1/backup'")
		t.Logf("backup, tracing data, and system table dumps in %s", dir)
	}()

	pgURL, cleanup := pgurlutils.PGUrl(t, tc.Server(0).AdvSQLAddr(), t.Name(), url.User("testuser"))
	defer cleanup()
	pgURL.Path = wl.Meta().Name

	const concurrency = 2
	require.NoError(t, wl.Flags().Parse([]string{
		"--concurrency", strconv.Itoa(concurrency),
		"--verbose", "2",
		"--trace-file", dir + "/schemachange.otlp.ndjson.gz",
	}))

	ql, err := wl.Ops(ctx, []string{pgURL.String()}, reg)
	require.NoError(t, err)

	// Defer ql.Close to ensure it gets called even in the case of panics.
	defer func() { require.NoError(t, ql.Close(ctx)) }()

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
