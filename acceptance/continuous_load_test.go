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
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"golang.org/x/net/context"
)

// Run tests in this file as follows:
//
// make test \
//   PKG=./acceptance \
//   TESTTIMEOUT=6h \
//   TESTS=ContinuousLoad_BlockWriter \
//   TESTFLAGS='-v -remote -key-name google_compute_engine -cwd allocator_terraform -nodes 4 -tf.keep-cluster=failed'
//
// Load is generated for the duration specified by TESTTIMEOUT, minus some time
// required for the orderly teardown of resources created by the test.  Because
// of the time required to create and destroy a test cluster (presently up to
// 10 minutes), you should use a TESTTIMEOUT that's at least a few hours.
//
// The SSH key you use for GCE must be in ~/.ssh/google_compute_engine. Also,
// note the additional time in TESTTIMEOUT to allow time for the cluster to be
// torn down.

// continuousLoadTest generates continuous load against a remote test cluster
// created specifically for the test.
type continuousLoadTest struct {
	// Prefix is prepended to all resources created by Terraform. We also add the
	// duration of the test to the prefix.
	Prefix string
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
	if e := "GOOGLE_PROJECT"; os.Getenv(e) == "" {
		t.Fatalf("%s environment variable must be set for Terraform", e)
	}
	if deadline, ok := ctx.Deadline(); ok {
		log.Infof(ctx, "load test will end at %s", deadline)
	} else {
		log.Infof(ctx, "load test will run indefinitely")
	}

	// Start cluster and load generator.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("recovered from panic to destroy cluster: %v", r)
		}
		f.MustDestroy(t)
	}()
	f.AddVars["benchmark_name"] = "BenchmarkBlockWriter" + cl.shortTestTimeout()
	if cl.CockroachDiskSizeGB != 0 {
		f.AddVars["cockroach_disk_size"] = strconv.Itoa(cl.CockroachDiskSizeGB)
	}
	if err := f.Resize(cl.NumNodes, 1 /* writer */); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, f, longWaitTime, hasPeers(cl.NumNodes))
	start := timeutil.Now()
	if err := f.StartWriter(0, cl.Process); err != nil {
		t.Fatal(err)
	}
	f.Assert(t)

	// Run load, checking the health of the cluster periodically.
	const healthCheckInterval = 1 * time.Minute
	var healthCheckTimer timeutil.Timer
	healthCheckTimer.Reset(healthCheckInterval)
	for {
		select {
		case <-healthCheckTimer.C:
			healthCheckTimer.Read = true
			f.Assert(t)
			healthCheckTimer.Reset(healthCheckInterval)
			log.Infof(ctx, "health check ok: %s", timeutil.Now().Sub(start))
		case <-ctx.Done():
			log.Infof(ctx, "load test finished")
			f.Assert(t)
			if err := f.StopWriter(0, cl.Process); err != nil {
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
		log.Errorf(context.TODO(), "couldn't parse test timeout %s", fl.Value.String())
		return ""
	}
	return regexp.MustCompile(`([a-z])0[0a-z]+`).ReplaceAllString(timeout.String(), `$1`)
}

func TestContinuousLoad_BlockWriter(t *testing.T) {
	continuousLoadTest{
		Prefix:              "bwriter",
		NumNodes:            *flagNodes,
		Process:             "block_writer",
		CockroachDiskSizeGB: 200,
	}.Run(t)
}
