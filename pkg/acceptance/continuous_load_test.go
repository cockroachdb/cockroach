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
	"flag"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// Run tests in this file as follows:
//
// make test \
//   PKG=./acceptance \
//   TESTTIMEOUT=6h \
//   TESTS=ContinuousLoad_BlockWriter \
//   TESTFLAGS='-v -remote -key-name google_compute_engine -cwd terraform -nodes 4 -tf.keep-cluster=failed'
//
// Load is generated for the duration specified by TESTTIMEOUT, minus some time
// required for the orderly teardown of resources created by the test.  Because
// of the time required to create and destroy a test cluster (presently up to
// 10 minutes), you should use a TESTTIMEOUT that's at least a few hours.
//
// Refer to the file-level comments in acceptance/allocator_test.go for more
// tips on running these tests.

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
	// CockroachDiskSizeGB is the size, in gigabytes, of the disks allocated
	// for CockroachDB nodes. Leaving this as 0 accepts the default in the
	// Terraform configs. This must be in GB, because Terraform only accepts
	// disk size for GCE in GB.
	CockroachDiskSizeGB int
}

// queryCount returns the total SQL queries executed by the cluster.
func (cl continuousLoadTest) queryCount(f *terrafarm.Farmer) (float64, error) {
	var client http.Client
	var resp status.NodeStatus
	host := f.Nodes()[0]
	if err := util.GetJSON(client, "http://"+host+":8080/_status/nodes/local", &resp); err != nil {
		return 0, err
	}
	count, ok := resp.Metrics["sql.query.count"]
	if !ok {
		return 0, errors.New("couldn't find SQL query count metric")
	}
	return count, nil
}

func (cl continuousLoadTest) startLoad(f *terrafarm.Farmer) error {
	if *flagCLTWriters > len(f.Nodes()) {
		return errors.Errorf("writers (%d) > nodes (%d)", *flagCLTWriters, len(f.Nodes()))
	}

	// We may have to retry restarting the load generators, because CockroachDB
	// might have been started too recently to start accepting connections.
	started := make(map[int]bool)
	return util.RetryForDuration(10*time.Second, func() error {
		for i := 0; i < *flagCLTWriters; i++ {
			if !started[i] {
				if err := f.Start(i, cl.Process); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// Run performs a continuous load test with the given parameters, checking the
// health of the cluster regularly. Any failure of a CockroachDB node or load
// generator constitutes a test failure. The test runs for the duration given
// by the `test.timeout` flag, minus the time it takes to reliably tear down
// the test cluster.
func (cl continuousLoadTest) Run(t *testing.T) {
	f := farmer(t, cl.Prefix+cl.shortTestTimeout())
	ctx, err := WithClusterTimeout(context.Background())
	if err != nil {
		t.Fatal(err)
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
	f.AddVars["benchmark_name"] = cl.BenchmarkPrefix + cl.shortTestTimeout()
	if cl.CockroachDiskSizeGB != 0 {
		f.AddVars["cockroach_disk_size"] = strconv.Itoa(cl.CockroachDiskSizeGB)
	}
	if err := f.Resize(cl.NumNodes); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, f, longWaitTime, hasPeers(cl.NumNodes))
	start := timeutil.Now()
	if err := cl.startLoad(f); err != nil {
		t.Fatal(err)
	}
	cl.assert(t, f)

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
			cl.assert(t, f)
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
			cl.assert(t, f)
			if err := f.Stop(0, cl.Process); err != nil {
				t.Error(err)
			}
			return
		case <-stopper:
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

// assert fails the test if CockroachDB or the load generators are down.
func (cl continuousLoadTest) assert(t *testing.T, f *terrafarm.Farmer) {
	f.Assert(t)
	for _, host := range f.Nodes()[0:*flagCLTWriters] {
		f.AssertState(t, host, cl.Process, "RUNNING")
	}
}

func TestContinuousLoad_BlockWriter(t *testing.T) {
	continuousLoadTest{
		Prefix:              "bwriter",
		BenchmarkPrefix:     "BenchmarkBlockWriter",
		NumNodes:            *flagNodes,
		Process:             "block_writer",
		CockroachDiskSizeGB: 200,
	}.Run(t)
}

func TestContinuousLoad_Photos(t *testing.T) {
	continuousLoadTest{
		Prefix:              "photos",
		BenchmarkPrefix:     "BenchmarkPhotos",
		NumNodes:            *flagNodes,
		Process:             "photos",
		CockroachDiskSizeGB: 200,
	}.Run(t)
}
