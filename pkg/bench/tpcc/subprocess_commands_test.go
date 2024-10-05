// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"context"
	"flag"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// databaseName is the name of the database used by this test.
const databaseName = "tpcc"

// Environment variables used to communicate configuration from the benchmark
// to the client subprocess.
const (
	internalTestEnvVar = "COCKROACH_INTERNAL_TEST"
	pgurlEnvVar        = "COCKROACH_PGURL"
	nEnvVar            = "COCKROACH_N"
	eventEnvVar        = "COCKROACH_STATUS_SERVER_ADDR"
	storeDirEnvVar     = "COCKROACH_STORE_DIR"
	srcEngineEnvVar    = "COCKROACH_SRC_ENGINE"
	dstEngineEnvVar    = "COCKROACH_DST_ENGINE"
)

var (
	benchmarkN = envutil.EnvOrDefaultInt(nEnvVar, -1)
)

func TestInternalCloneEngine(t *testing.T)      { internalCommand(t) }
func TestInternalRunClient(t *testing.T)        { internalCommand(t) }
func TestInternalGenerateStoreDir(t *testing.T) { internalCommand(t) }

var (
	commands         = map[string]*cmd{}
	generateStoreDir = registerCmd("GenerateStoreDir", func(t *testing.T) {
		ctx := context.Background()
		storeDir, ok := envutil.EnvString(storeDirEnvVar, 0)
		require.True(t, ok)

		defer log.Scope(t).Close(t)
		srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{Path: storeDir}},
		})
		defer srv.Stopper().Stop(ctx)

		// Make the generation faster.
		logstore.DisableSyncRaftLog.Override(context.Background(), &srv.SystemLayer().ClusterSettings().SV, true)

		tdb := sqlutils.MakeSQLRunner(db)
		tdb.Exec(t, "CREATE DATABASE "+databaseName)
		tdb.Exec(t, "USE "+databaseName)
		tpcc, err := workload.Get("tpcc")
		require.NoError(t, err)
		gen := tpcc.New().(interface {
			workload.Flagser
			workload.Hookser
			workload.Generator
		})
		require.NoError(t, gen.Flags().Parse([]string{
			"--db=" + databaseName,
		}))
		require.NoError(t, gen.Hooks().Validate())
		{
			var l workloadsql.InsertsDataLoader
			_, err := workloadsql.Setup(ctx, db, gen, l)
			require.NoError(t, err)
		}
	})
	cloneEngine = registerCmd("CloneEngine", func(t *testing.T) {
		src, ok := envutil.EnvString(srcEngineEnvVar, 0)
		require.True(t, ok)
		dst, ok := envutil.EnvString(dstEngineEnvVar, 0)
		require.True(t, ok)
		_, err := vfs.Clone(vfs.Default, vfs.Default, src, dst)
		require.NoError(t, err)
	})
	runClient = registerCmd("RunClient", func(t *testing.T) {
		require.Positive(t, benchmarkN)

		pgURL, ok := envutil.EnvString(pgurlEnvVar, 0)
		require.True(t, ok)
		ql := makeQueryLoad(t, pgURL)
		defer func() { _ = ql.Close(context.Background()) }()
		eventAddr, ok := envutil.EnvString(eventEnvVar, 0)
		require.True(t, ok)
		sendEvent(t, eventAddr, runStartEvent)
		for i := 0; i < benchmarkN; i++ {
			require.NoError(t, ql.WorkerFns[0](context.Background()))
		}
		sendEvent(t, eventAddr, runDoneEvent)
	})
)

func makeQueryLoad(t *testing.T, pgURL string) workload.QueryLoad {
	tpcc, err := workload.Get("tpcc")
	require.NoError(t, err)
	gen := tpcc.New()
	wl := gen.(interface {
		workload.Flagser
		workload.Hookser
		workload.Opser
	})
	ctx := context.Background()

	flags := append([]string{
		"--wait=0",
		"--workers=1",
		"--db=" + databaseName,
	}, flag.CommandLine.Args()...)
	require.NoError(t, wl.Flags().Parse(flags))

	require.NoError(t, wl.Hooks().Validate())

	reg := histogram.NewRegistry(time.Minute, "tpcc")
	ql, err := wl.Ops(ctx, []string{pgURL}, reg)
	require.NoError(t, err)
	return ql
}

// These event strings are used to synchronize the client process with the
// benchmark process.
const (
	runStartEvent = "run start"
	runDoneEvent  = "run done"
)

func sendEvent(t *testing.T, statusAddr, evName string) {
	resp, err := http.Post(statusAddr, "", strings.NewReader(evName))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, evName)
}
