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

// All tests in this file are remote tests that use Terrafarm to manage
// and run tests against dedicated test clusters.
//
// Required setup:
// 1. Have a GCE account.
// 2. Have someone grant permissions (for new *and* existing objects) for the
//    GCS bucket referenced in `archivedStoreURL`. You'll want permissions
//    granted to the following email addresses:
//      a. The email address you use to log into Google Cloud Console.
//      b. Your default Google Compute Engine service account (it'll look like
//         111111111111-compute@developer.gserviceaccount.com)
// 3. Set the environment variable GOOGLE_PROJECT to the name of the Google
//    Project you want Terraform to use.
//
// Example use:
//
// make acceptance \
//   TESTFLAGS="-v --remote -key-name google_compute_engine -cwd=allocator_terraform" \
//   TESTS="TestUpreplicate_1To3Small" \
//   TESTTIMEOUT="24h"
//
// Things to note:
// - Your SSH key (-key-name) for Google Cloud Platform must be in
//    ~/.ssh/google_compute_engine
// - These tests rely on a specific Terraform config that's specified using the
//   -cwd test flag.
// - You *must* set the TESTTIMEOUT high enough for any of these tests to
//   finish. To be safe, specify a timeout of at least 24 hours.
// - The Google credentials you're using for Terraform must have access to the
//   Google Cloud Storage bucket referenced by archivedStoreURL.
// - Your Google Cloud credentials must be accessible by Terraform, as described
//   here:
//   https://www.terraform.io/docs/providers/google/
// - There are various flags that start with `tf.` and `at.` that control the
//   of Terrafarm and allocator tests, respectively. For example, you can add
//   "-at.cockroach-binary" to TESTFLAGS to specify a custom Linux CockroachDB
//   binary. If omitted, your test will use the latest CircleCI Linux build.

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/montanaflynn/stats"

	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	archivedStoreURL = "gs://cockroach-test/allocatortest"
	StableInterval   = 3 * time.Minute
)

type allocatorTest struct {
	// StartNodes is the starting number of nodes this cluster will have.
	StartNodes int
	// EndNodes is the final number of nodes this cluster will have.
	EndNodes int
	// StoreURL is the Google Cloud Storage URL from which the test will download
	// stores.
	StoreURL string
	// Prefix is the prefix that will be prepended to all resources created by
	// Terraform.
	Prefix string
	// CockroachDiskSizeGB is the size, in gigabytes, of the disks allocated
	// for CockroachDB nodes. Leaving this as 0 accepts the default in the
	// Terraform configs. This must be in GB, because Terraform only accepts
	// disk size for GCE in GB.
	CockroachDiskSizeGB int

	f *terrafarm.Farmer
}

func (at *allocatorTest) Run(t *testing.T) {
	at.f = farmer(t, at.Prefix)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("recovered from panic to destroy cluster: %v", r)
		}
		if t.Failed() && at.f.KeepClusterAfterFail {
			t.Log("test has failed, not destroying")
			return
		}
		at.f.MustDestroy()
	}()

	if e := "GOOGLE_PROJECT"; os.Getenv(e) == "" {
		t.Fatalf("%s environment variable must be set for Terraform", e)
	}

	// Pass on overrides to Terraform input variables.
	if *flagATCockroachBinary != "" {
		at.f.AddVars["cockroach_binary"] = *flagATCockroachBinary
	}
	if *flagATCockroachFlags != "" {
		at.f.AddVars["cockroach_flags"] = *flagATCockroachFlags
	}
	if *flagATCockroachEnv != "" {
		at.f.AddVars["cockroach_env"] = *flagATCockroachEnv
	}
	if at.CockroachDiskSizeGB != 0 {
		at.f.AddVars["cockroach_disk_size"] = strconv.Itoa(at.CockroachDiskSizeGB)
	}

	log.Infof("creating cluster with %d node(s)", at.StartNodes)
	if err := at.f.Resize(at.StartNodes, 0 /*writers*/); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, at.f, longWaitTime, hasPeers(at.StartNodes))
	at.f.Assert(t)
	log.Info("initial cluster is up")

	log.Info("downloading archived stores from Google Cloud Storage in parallel")
	errors := make(chan error, at.f.NumNodes())
	for i := 0; i < at.f.NumNodes(); i++ {
		go func(nodeNum int) {
			errors <- at.f.Exec(nodeNum, "./nodectl download "+at.StoreURL)
		}(i)
	}
	for i := 0; i < at.f.NumNodes(); i++ {
		if err := <-errors; err != nil {
			t.Fatalf("error downloading store %d: %s", i, err)
		}
	}

	log.Info("restarting cluster with archived store(s)")
	for i := 0; i < at.f.NumNodes(); i++ {
		if err := at.f.Restart(i); err != nil {
			t.Fatalf("error restarting node %d: %s", i, err)
		}
	}
	at.f.Assert(t)

	log.Infof("resizing cluster to %d nodes", at.EndNodes)
	if err := at.f.Resize(at.EndNodes, 0 /*writers*/); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, at.f, longWaitTime, hasPeers(at.EndNodes))
	at.f.Assert(t)

	log.Info("waiting for rebalance to finish")
	if err := at.WaitForRebalance(); err != nil {
		t.Fatal(err)
	}

	at.f.Assert(t)
}

// printStats prints the time it took for rebalancing to finish and the final
// standard deviation of replica counts across stores.
func (at *allocatorTest) printRebalanceStats(db *gosql.DB, host string, adminPort int) error {
	// TODO(cuongdo): Output these in a machine-friendly way and graph.

	// Output time it took to rebalance.
	{
		var rebalanceIntervalStr string
		var rebalanceInterval time.Duration
		q := `SELECT (SELECT MAX(timestamp) FROM rangelog) - ` +
			`(select MAX(timestamp) FROM eventlog WHERE eventType='` + string(sql.EventLogNodeJoin) + `')`
		if err := db.QueryRow(q).Scan(&rebalanceIntervalStr); err != nil {
			return err
		}
		rebalanceInterval, err := time.ParseDuration(rebalanceIntervalStr)
		if err != nil {
			return err
		}
		if rebalanceInterval < 0 {
			// This can happen with single-node clusters.
			rebalanceInterval = time.Duration(0)
		}
		log.Infof("cluster took %s to rebalance", rebalanceInterval)
	}

	// Output # of range events that occurred. All other things being equal,
	// larger numbers are worse and potentially indicate thrashing.
	{
		var rangeEvents int64
		q := `SELECT COUNT(*) from rangelog`
		if err := db.QueryRow(q).Scan(&rangeEvents); err != nil {
			return err
		}
		log.Infof("%d range events", rangeEvents)
	}

	// Output standard deviation of the replica counts for all stores.
	{
		var client http.Client
		var nodesResp serverpb.NodesResponse
		url := fmt.Sprintf("http://%s:%d/_status/nodes", host, adminPort)
		if err := util.GetJSON(client, url, &nodesResp); err != nil {
			return err
		}
		var replicaCounts stats.Float64Data
		for _, node := range nodesResp.Nodes {
			for _, ss := range node.StoreStatuses {
				replicaCounts = append(replicaCounts, float64(ss.Metrics["replicas"]))
			}
		}
		stddev, err := stats.StdDevP(replicaCounts)
		if err != nil {
			return err
		}
		log.Infof("stddev(replica count) = %.2f", stddev)
	}

	return nil
}

// checkAllocatorStable returns whether the replica distribution within the
// cluster has been stable for at least `StableInterval`. Only unrecoverable
// errors are returned.
func (at *allocatorTest) checkAllocatorStable(db *gosql.DB) (bool, error) {
	q := `SELECT NOW()-timestamp, rangeID, storeID, eventType FROM rangelog WHERE ` +
		`timestamp=(SELECT MAX(timestamp) FROM rangelog WHERE eventType IN ($1, $2, $3))`
	eventTypes := []interface{}{
		string(storage.RangeEventLogSplit),
		string(storage.RangeEventLogAdd),
		string(storage.RangeEventLogRemove),
	}
	var elapsedStr string
	var rangeID int64
	var storeID int64
	var eventType string

	row := db.QueryRow(q, eventTypes...)
	if row == nil {
		log.Errorf("couldn't find any range events")
		return false, nil
	}
	if err := row.Scan(&elapsedStr, &rangeID, &storeID, &eventType); err != nil {
		// Log but don't return errors, to increase resilience against transient
		// errors.
		log.Errorf("error checking rebalancer: %s", err)
		return false, nil
	}
	elapsedSinceLastRangeEvent, err := time.ParseDuration(elapsedStr)
	if err != nil {
		return false, err
	}

	log.Infof("last range event: %s for range %d/store %d (%s ago)",
		eventType, rangeID, storeID, elapsedSinceLastRangeEvent)
	return elapsedSinceLastRangeEvent >= StableInterval, nil
}

// WaitForRebalance waits until there's been no recent range adds, removes, and
// splits. We wait until the cluster is balanced or until `StableInterval`
// elapses, whichever comes first. Then, it prints stats about the rebalancing
// process.
//
// This method is crude but necessary. If we were to wait until range counts
// were just about even, we'd miss potential post-rebalance thrashing.
func (at *allocatorTest) WaitForRebalance() error {
	db, err := gosql.Open("postgres", at.f.PGUrl(1))
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	var timer timeutil.Timer
	defer timer.Stop()
	timer.Reset(0)
	for {
		select {
		case <-timer.C:
			timer.Read = true
			stable, err := at.checkAllocatorStable(db)
			if err != nil {
				return err
			}
			if stable {
				host := at.f.Nodes()[0]
				if err := at.printRebalanceStats(db, host, 8080); err != nil {
					return err
				}
				return nil
			}
		case <-stopper:
			return errors.New("interrupted")
		}

		timer.Reset(10 * time.Second)
	}
}

// TestUpreplicate_1To3Small tests up-replication, starting with 1 node
// containing 10 GiB of data and growing to 3 nodes.
func TestUpreplicate_1To3Small(t *testing.T) {
	at := allocatorTest{
		StartNodes: 1,
		EndNodes:   3,
		StoreURL:   archivedStoreURL + "/1node-10g-262ranges",
		Prefix:     "u1to3s",
	}
	at.Run(t)
}

// TestRebalance3to5Small tests rebalancing, starting with 3 nodes (each
// containing 10 GiB of data) and growing to 5 nodes.
func TestRebalance_3To5Small(t *testing.T) {
	at := allocatorTest{
		StartNodes: 3,
		EndNodes:   5,
		StoreURL:   archivedStoreURL + "/3nodes-10g-262ranges",
		Prefix:     "r3to5s",
	}
	at.Run(t)
}

// TestUpreplicate_1To3Medium tests up-replication, starting with 1 node
// containing 108 GiB of data and growing to 3 nodes.
func TestUpreplicate_1To3Medium(t *testing.T) {
	at := allocatorTest{
		StartNodes:          1,
		EndNodes:            3,
		StoreURL:            archivedStoreURL + "/1node-2065replicas-108G",
		Prefix:              "u1to3m",
		CockroachDiskSizeGB: 250, // GB
	}
	at.Run(t)
}
