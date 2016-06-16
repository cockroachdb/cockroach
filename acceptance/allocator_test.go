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
// Example use:
//
// make acceptance \
//   TESTFLAGS="-v --remote -key-name google_compute_engine -cwd=allocator_terraform" \
//   TESTS="TestUpreplicate1To3Medium" \
//   TESTTIMEOUT="1h"
//
// Things to note:
// - Your SSH key (-key-name) for Google Cloud Platform must be in
//    ~/.ssh/google_compute_engine
// - These tests rely on a specific Terraform config that's specified using the
//   -cwd test flag.
// - You *must* set the TESTTIMEOUT high enough for any of these tests to
//   finish. To be safe, specify a timeout of at least an hour.
// - The Google credentials you're using for Terraform must have access to the
//   Google Cloud Storage bucket referenced by archivedStoreURL.
// - Your Google Cloud credentials must be accessible by Terraform, as described
//   here:
//   https://www.terraform.io/docs/providers/google/
// - Add "-cockroach-binary" to TESTFLAGS to specify a custom Linux CockroachDB
//   binary. If omitted, your test will use the latest CircleCI Linux build.

import (
	gosql "database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"testing"
	"time"

	"github.com/montanaflynn/stats"

	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

const (
	archivedStoreURL = "gs://cockroach-test/allocatortest"
	stableInterval   = 3 * time.Minute
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
	// RebalanceTimeout is the max time we'll wait for the cluster to finish
	// rebalancing.
	RebalanceTimeout time.Duration
	// CockroachDiskSizeGB is the size, in gigabytes, of the disks allocated
	// for CockroachDB nodes. Leaving this as 0 accepts the default in the
	// Terraform configs.
	CockroachDiskSizeGB int

	f *terrafarm.Farmer
}

func (at *allocatorTest) Run(t *testing.T) {
	at.f = farmer(t, at.Prefix)
	defer func() {
		at.f.MustDestroy()
	}()

	// Pass on overrides to Terraform input variables.
	if *flagCockroachBinary != "" {
		at.f.AddVars["cockroach_binary"] = *flagCockroachBinary
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

	log.Info("stopping CockroachDB")
	if err := at.f.StopCockroach(); err != nil {
		t.Fatal(err)
	}

	// TODO(cuongdo): Make this parallel, which will greatly reduce test run
	// times with larger data sets and clusters.
	log.Info("downloading archived stores from Google Cloud Storage")
	if err := at.f.SSHCockroachNodes("./nodectl download " + at.StoreURL); err != nil {
		t.Fatal(err)
	}

	log.Info("bringing cluster back up")
	if err := at.f.StartCockroach(); err != nil {
		t.Fatal(err)
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

// checkAllocatorStable returns whether the replica distribution within the cluster has
// been stable for at least `stableInterval`.
func (at *allocatorTest) checkAllocatorStable(db *gosql.DB) (bool, error) {
	q := `SELECT NOW() - MAX(timestamp) FROM rangelog`
	var elapsedStr string
	if err := db.QueryRow(q).Scan(&elapsedStr); err != nil {
		// Log but don't return errors, to increase resilience against transient
		// errors.
		log.Errorf("error checking rebalancer: %s", err)
		return false, nil
	}
	elapsedSinceLastRangeEvent, err := time.ParseDuration(elapsedStr)
	if err != nil {
		return false, err
	}

	var status string
	stable := elapsedSinceLastRangeEvent >= stableInterval
	if stable {
		status = fmt.Sprintf("allocator is stable (idle for %s)", stableInterval)
	} else {
		status = "waiting for idle allocator"
	}
	log.Infof("last range event was %s ago: %s", elapsedSinceLastRangeEvent, status)
	return stable, nil
}

// WaitForRebalance waits until there's been no recent range adds, removes, and
// splits. We wait until the cluster is balanced or until `maxWait` elapses,
// whichever comes first. Then, it prints stats about the rebalancing process.
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

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	timeoutTimer := time.After(at.RebalanceTimeout)
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
		case <-timeoutTimer:
			return errors.New("timeout expired")
		case <-c:
			return errors.New("interrupted")
		}

		timer.Reset(10 * time.Second)
	}
}

// TestUpreplicate1To3Small tests up-replication, starting with 1 node
// containing 10 GiB of data and growing to 3 nodes.
func TestUpreplicate1To3Small(t *testing.T) {
	at := allocatorTest{
		StartNodes:       1,
		EndNodes:         3,
		StoreURL:         archivedStoreURL + "/1node-10g-262ranges",
		Prefix:           "uprep-1to3-small",
		RebalanceTimeout: time.Hour,
	}
	at.Run(t)
}

// TestRebalance3to5Small tests rebalancing, starting with 3 nodes (each
// containing 10 GiB of data) and growing to 5 nodes.
func TestRebalance3to5Small(t *testing.T) {
	at := allocatorTest{
		StartNodes:       3,
		EndNodes:         5,
		StoreURL:         archivedStoreURL + "/3nodes-10g-262ranges",
		Prefix:           "rebal-3to5-small",
		RebalanceTimeout: time.Hour,
	}
	at.Run(t)
}

// TestUpreplicate1To3Medium tests up-replication, starting with 1 node
// containing 108 GiB of data and growing to 3 nodes.
func TestUpreplicate1To3Medium(t *testing.T) {
	at := allocatorTest{
		StartNodes:          1,
		EndNodes:            3,
		StoreURL:            archivedStoreURL + "/1node-2065replicas-108G",
		Prefix:              "uprep-1to3-med",
		RebalanceTimeout:    4 * time.Hour,
		CockroachDiskSizeGB: 250, // GiB
	}
	at.Run(t)
}
