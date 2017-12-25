// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"context"
	"flag"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Running these tests is best done using build/teamcity-nightly-
// acceptance.sh. See instructions therein.
//
// These tests run for the duration specified by the testing package's timeout
// flag, minus some time required for the orderly teardown of resources
// created by the test. Because of the time required to create and destroy a
// test cluster (presently up to 10 minutes), you should use a timeout of at
// least a few hours.

// continuousLoadTest generates continuous load against a remote test cluster
// created specifically for the test.
type continuousLoadTest struct {
	// Prefix is prepended to all resources created by Terraform. We also add the
	// duration of the test to the prefix.
	Prefix string
	// BenchmarkPrefix is the prefix that's prepended to the benchmark name
	// outputted upon termination of the load generators.
	BenchmarkPrefix string
	// NumNodes is the number of nodes in the test cluster.
	NumNodes int
	// Process must be one of the processes (e.g. block_writer) that's
	// downloaded by the Terraform configuration.
	Process string
}

const longWaitTime = 2 * time.Minute

// queryCount returns the total SQL queries executed by the cluster.
func (cl continuousLoadTest) queryCount(f *terrafarm.Farmer) (float64, error) {
	var client http.Client
	var resp status.NodeStatus
	host := f.Hostname(0)
	if err := httputil.GetJSON(client, "http://"+host+":8080/_status/nodes/local", &resp); err != nil {
		return 0, err
	}
	count, ok := resp.Metrics["sql.query.count"]
	if !ok {
		return 0, errors.New("couldn't find SQL query count metric")
	}
	return count, nil
}

// Run performs a continuous load test with the given parameters, checking the
// health of the cluster regularly. Any failure of a CockroachDB node or load
// generator constitutes a test failure. The test runs for the duration given
// by the `test.timeout` flag, minus the time it takes to reliably tear down
// the test cluster.
func (cl continuousLoadTest) Run(ctx context.Context, t testing.TB) {
	s := log.Scope(t)
	defer s.Close(t)

	f := MakeFarmer(t, cl.Prefix+cl.shortTestTimeout(), stopper)
	// If the timeout flag was set, calculate an appropriate lower timeout by
	// subtracting expected cluster creation and teardown times to allow for
	// proper shutdown at the end of the test.
	if fl := flag.Lookup("test.timeout"); fl != nil {
		timeout, err := time.ParseDuration(fl.Value.String())
		if err != nil {
			t.Fatal(err)
		}
		// We've empirically observed 6-7 minute teardown times. We set aside a
		// larger duration to account for outliers and setup time.
		if setupTeardownDuration := 10 * time.Minute; timeout <= setupTeardownDuration {
			t.Fatalf("test.timeout must be greater than create/destroy interval %s", setupTeardownDuration)
		} else {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout-setupTeardownDuration)
			defer cancel()
		}
	}
	if deadline, ok := ctx.Deadline(); ok {
		log.Infof(ctx, "load test will end at %s", deadline)
	} else {
		log.Infof(ctx, "load test will run indefinitely")
	}

	// Start cluster and load.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("recovered from panic to destroy cluster: %v", r)
		}
		f.MustDestroy(t)
	}()
	f.BenchmarkName = cl.BenchmarkPrefix + cl.shortTestTimeout()

	if err := f.Resize(cl.NumNodes); err != nil {
		t.Fatal(err)
	}
	if err := CheckGossip(ctx, f, longWaitTime, HasPeers(cl.NumNodes)); err != nil {
		t.Fatal(err)
	}
	start := timeutil.Now()
	if err := f.StartLoad(ctx, cl.Process); err != nil {
		t.Fatal(err)
	}
	f.Assert(ctx, t)

	// Run load, checking the health of the cluster periodically.
	const healthCheckInterval = 2 * time.Minute
	var healthCheckTimer timeutil.Timer
	healthCheckTimer.Reset(healthCheckInterval)
	lastQueryCount := 0.0
	lastHealthCheck := timeutil.Now()
	for {
		select {
		case <-healthCheckTimer.C:
			// Check that all nodes are up and that queries are being processed.
			healthCheckTimer.Read = true
			f.Assert(ctx, t)
			queryCount, err := cl.queryCount(f)
			if err != nil {
				t.Fatal(err)
			}
			if lastQueryCount == queryCount {
				t.Fatalf("no queries in the last %s", healthCheckInterval)
			}
			qps := (queryCount - lastQueryCount) / timeutil.Now().Sub(lastHealthCheck).Seconds()
			if qps < *flagCLTMinQPS {
				t.Fatalf("qps (%.1f) < min (%.1f)", qps, *flagCLTMinQPS)
			}
			lastQueryCount = queryCount
			lastHealthCheck = timeutil.Now()
			healthCheckTimer.Reset(healthCheckInterval)
			log.Infof(ctx, "health check ok %s (%.1f qps)", timeutil.Now().Sub(start), qps)
		case <-ctx.Done():
			log.Infof(ctx, "load test finished")
			f.Assert(ctx, t)
			if err := f.Stop(ctx, 0, cl.Process); err != nil {
				t.Error(err)
			}
			return
		case <-stopper.ShouldStop():
			t.Fatal("interrupted")
		}
	}
}

// shortTestTimeout returns the string form of a time.Duration stripped of
// trailing time units that have 0 values. For example, 6*time.Hour normally
// stringifies as "6h0m0s". This regex converts it into a more readable "6h".
func (cl continuousLoadTest) shortTestTimeout() string {
	fl := flag.Lookup("test.timeout")
	if fl == nil {
		return ""
	}
	timeout, err := time.ParseDuration(fl.Value.String())
	if err != nil {
		log.Errorf(context.Background(), "couldn't parse test timeout %s", fl.Value.String())
		return ""
	}
	return regexp.MustCompile(`([a-z])0[0a-z]+`).ReplaceAllString(timeout.String(), `$1`)
}

func TestContinuousLoad_BlockWriter(t *testing.T) {
	ctx := context.Background()
	continuousLoadTest{
		Prefix:          "bwriter",
		BenchmarkPrefix: "BenchmarkBlockWriter",
		NumNodes:        *flagNodes,
		Process:         "block_writer",
	}.Run(ctx, t)
}

func TestContinuousLoad_Photos(t *testing.T) {
	ctx := context.Background()
	continuousLoadTest{
		Prefix:          "photos",
		BenchmarkPrefix: "BenchmarkPhotos",
		NumNodes:        *flagNodes,
		Process:         "photos",
	}.Run(ctx, t)
}
