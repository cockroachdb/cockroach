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
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// Run tests in this file as follows:
//
// make test \
//   PKG=./acceptance \
//   TESTTIMEOUT=6h15m \
//   TESTS=BlockWriter6Hours \
//   TESTFLAGS='-v -remote -key-name google_compute_engine -cwd allocator_terraform'
//
// The SSH key you use for GCE must be in ~/.ssh/google_compute_engine. Also,
// note the additional time in TESTTIMEOUT to allow time for the cluster to be
// torn down.

// continuousLoadTest generates continuous load against a remote test cluster
// created specifically for the test.
type continuousLoadTest struct {
	// Prefix is prepended to all resources created by Terraform.
	Prefix string
	// NumNodes is the number of nodes in the test cluster.
	NumNodes int
	// Duration is the duration that the test should run for.
	Duration time.Duration
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
// generator constitutes a test failure.
func (cl continuousLoadTest) Run(t *testing.T) {
	if e := "GOOGLE_PROJECT"; os.Getenv(e) == "" {
		t.Fatalf("%s environment variable must be set for Terraform", e)
	}

	// Ensure that the test timeout allows the load test to run and tear down
	// properly, to avoid Terraform resource leaks.
	const destroyTime = 10 * time.Minute
	fl := flag.Lookup("test.timeout")
	if fl != nil {
		testTimeout, err := time.ParseDuration(fl.Value.String())
		if err != nil {
			t.Fatal(err)
		}
		if testTimeout-cl.Duration < destroyTime {
			t.Fatalf("test timeout must be at least %s", cl.Duration+destroyTime)
		}
	}

	// Start cluster and load generator.
	f := farmer(t, "bwriter-6h")
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("recovered from panic to destroy cluster: %v", r)
		}
		f.MustDestroy(t)
	}()
	f.AddVars["load_duration"] = cl.Duration.String()
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
	const healthCheckInterval = 5 * time.Minute
	var healthCheckTimer timeutil.Timer
	healthCheckTimer.Reset(healthCheckInterval)
	// Add some extra time for the load generator to output benchmark stats to its
	// logs.
	loadTimer := time.NewTimer(cl.Duration + 10*time.Second)
	scheduledEnd := start.Add(cl.Duration)
	for {
		select {
		case <-healthCheckTimer.C:
			// There could be a race between timers when cl.Duration divides evenly by
			// healthCheckInterval. So, if we've already exhausted the allotted time,
			// we wait until the final post-load generation health check happens
			// below.
			healthCheckTimer.Read = true
			if timeutil.Now().Before(scheduledEnd) {
				f.Assert(t)
				healthCheckTimer.Reset(healthCheckInterval)
				log.Infof("health check ok: %s / %s", timeutil.Now().Sub(start), cl.Duration)
			}
		case <-loadTimer.C:
			log.Infof("load test finished")
			f.AssertStates(t, map[string]string{
				"cockroach":    "RUNNING",
				"block_writer": "EXITED",
			})
			return
		case <-stopper:
			t.Fatal("interrupted")
		}
	}
}

func TestBlockWriter6Hours(t *testing.T) {
	continuousLoadTest{
		Prefix:              "bwriter-6h",
		NumNodes:            3,
		Duration:            6 * time.Hour,
		Process:             "block_writer",
		CockroachDiskSizeGB: 200,
	}.Run(t)
}
