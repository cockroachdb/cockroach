// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/stretchr/testify/require"
)

// databaseName is the name of the database used by this test.
const databaseName = "tpcc"

// These event strings are used to synchronize the client process with the
// benchmark process.
const (
	runStartEvent = "run start"
	runDoneEvent  = "run done"
)

// Environment variables used to communicate configuration from the benchmark
// to the client subprocess.
const (
	internalTestEnvVar = "COCKROACH_INTERNAL_TEST"
	pgurlEnvVar        = "COCKROACH_PGURL"
	nEnvVar            = "COCKROACH_N"
	eventEnvVar        = "COCKROACH_STATUS_SERVER_ADDR"
)

var (
	benchmarkN     = envutil.EnvOrDefaultInt(nEnvVar, -1)
	isInternalTest = envutil.EnvOrDefaultBool(internalTestEnvVar, false)
)

// TestInternalRunClient is run by BenchmarkTPCC as a subprocess to avoid
// capturing client overhead in the benchmark stats. The makeClientCommand
// function is used to construct the command arguments to invoke this test.
// It is never run on its own. The internalTestEnvVar is set to mark whether
// this test should be run.
func TestInternalRunClient(t *testing.T) {
	if !isInternalTest {
		skip.IgnoreLint(t)
	}
	require.Positive(t, benchmarkN)

	pgURL, ok := envutil.EnvString(pgurlEnvVar, 0)
	require.True(t, ok)
	ql := makeQueryLoad(t, pgURL)
	eventAddr, ok := envutil.EnvString(eventEnvVar, 0)
	require.True(t, ok)
	sendEvent(t, eventAddr, runStartEvent)
	for i := 0; i < benchmarkN; i++ {
		require.NoError(t, ql.WorkerFns[0](context.Background()))
	}
	sendEvent(t, eventAddr, runDoneEvent)
	defer ql.Close(context.Background())
}

func makeClientCommand(n int, pgURL, eventURL string, workloadFlags []string) *exec.Cmd {
	cmd := exec.Command(os.Args[0],
		append([]string{
			"--test.run=^TestInternalRunClient$",
			"--test.v",
			"--",
		}, workloadFlags...)...)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env,
		fmt.Sprintf("%s=t", internalTestEnvVar),
		fmt.Sprintf("%s=%d", nEnvVar, n),
		fmt.Sprintf("%s=%s", pgurlEnvVar, pgURL),
		fmt.Sprintf("%s=%s", eventEnvVar, eventURL),
	)
	return cmd
}

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

func sendEvent(t *testing.T, statusAddr, evName string) {
	resp, err := http.Post(statusAddr, "", strings.NewReader(evName))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, evName)
}
