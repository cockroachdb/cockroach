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
// make test \
//	 TESTTIMEOUT=48h \
//	 PKG=./acceptance \
//	 TESTS=Rebalance_3To5Small \
//	 TESTFLAGS='-v -remote -key-name google_compute_engine -cwd terraform -tf.keep-cluster=failed'
//
// Things to note:
// - You must use an SSH key without a passphrase. It is recommended that you
//   create a neww key for this purpose named google_compute_engine so that
//   gcloud and related tools can use it too. Create the key with:
//     ssh-keygen -f ~/.ssh/google_compute_engine
// - Your SSH key (-key-name) for Google Cloud Platform must be in
//   ~/.ssh/google_compute_engine
// - If you want to manually fiddle with a test cluster, start the test with
//   `-tf.keep-cluster=failed". After the cluster has been created, press
//   Control-C and the cluster will remain up.
// - These tests rely on a specific Terraform config that's specified using the
//   -cwd test flag.
// - You *must* set the TESTTIMEOUT high enough for any of these tests to
//   finish. To be safe, specify a timeout of at least 24 hours.
// - Your Google Cloud credentials must be accessible by Terraform, as described
//   here:
//   https://www.terraform.io/docs/providers/google/
// - There are various flags that start with `tf.` and `at.` that control the
//   of Terrafarm and allocator tests, respectively. For example, you can add
//   "-at.cockroach-binary" to TESTFLAGS to specify a custom Linux CockroachDB
//   binary. If omitted, your test will use the latest CircleCI Linux build.
//   Note that the location has to be specified relative to the terraform
//   working directory, so typically `-at.cockroach-binary=../../cockroach`.
//
// Troubleshooting:
// - The minimum recommended version of Terraform is 0.6.16. If you see strange
//   Terraform errors, upgrade your install of Terraform.
// - Adding `-tf.keep-cluster=always` to your TESTFLAGS allows the cluster to
//   stay around after the test completes.

import (
	gosql "database/sql"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/base"
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
	adminPort        = base.DefaultHTTPPort
)

const (
	urlStore1s = archivedStoreURL + "/1node-10g-262ranges"
	urlStore1m = archivedStoreURL + "/1node-2065replicas-108G"
	urlStore3s = archivedStoreURL + "/3nodes-10g-262ranges"
	urlStore6m = archivedStoreURL + "/6nodes-1038replicas-56G"
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

func (at *allocatorTest) Cleanup(t *testing.T) {
	if r := recover(); r != nil {
		t.Errorf("recovered from panic to destroy cluster: %v", r)
	}
	if at.f != nil {
		at.f.MustDestroy(t)
	}
}

func (at *allocatorTest) Run(t *testing.T) {
	at.f = farmer(t, at.Prefix)

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
	at.f.AddVars["cockroach_disk_type"] = *flagATDiskType

	log.Infof(context.Background(), "creating cluster with %d node(s)", at.StartNodes)
	if err := at.f.Resize(at.StartNodes); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, at.f, longWaitTime, hasPeers(at.StartNodes))
	at.f.Assert(t)
	log.Info(context.Background(), "initial cluster is up")

	// We must stop the cluster because a) `nodectl` pokes at the data directory
	// and, more importantly, b) we don't want the cluster above and the cluster
	// below to ever talk to each other (see #7224).
	log.Info(context.Background(), "stopping cluster")
	for i := 0; i < at.f.NumNodes(); i++ {
		if err := at.f.Kill(i); err != nil {
			t.Fatalf("error stopping node %d: %s", i, err)
		}
	}

	log.Info(context.Background(), "downloading archived stores from Google Cloud Storage in parallel")
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

	log.Info(context.Background(), "restarting cluster with archived store(s)")
	for i := 0; i < at.f.NumNodes(); i++ {
		if err := at.f.Restart(i); err != nil {
			t.Fatalf("error restarting node %d: %s", i, err)
		}
	}
	at.f.Assert(t)

	log.Infof(context.Background(), "resizing cluster to %d nodes", at.EndNodes)
	if err := at.f.Resize(at.EndNodes); err != nil {
		t.Fatal(err)
	}
	checkGossip(t, at.f, longWaitTime, hasPeers(at.EndNodes))
	at.f.Assert(t)

	log.Info(context.Background(), "waiting for rebalance to finish")
	if err := at.WaitForRebalance(t); err != nil {
		t.Fatal(err)
	}
	at.f.Assert(t)
}

func (at *allocatorTest) RunAndCleanup(t *testing.T) {
	defer at.Cleanup(t)
	at.Run(t)
}

func (at *allocatorTest) stdDev() (float64, error) {
	host := at.f.Nodes()[0]
	var client http.Client
	var nodesResp serverpb.NodesResponse
	url := fmt.Sprintf("http://%s:%s/_status/nodes", host, adminPort)
	if err := util.GetJSON(client, url, &nodesResp); err != nil {
		return 0, err
	}
	var replicaCounts stats.Float64Data
	for _, node := range nodesResp.Nodes {
		for _, ss := range node.StoreStatuses {
			replicaCounts = append(replicaCounts, float64(ss.Metrics["replicas"]))
		}
	}
	stdDev, err := stats.StdDevP(replicaCounts)
	if err != nil {
		return 0, err
	}
	return stdDev, nil
}

// printStats prints the time it took for rebalancing to finish and the final
// standard deviation of replica counts across stores.
func (at *allocatorTest) printRebalanceStats(
	db *gosql.DB,
	host string,
) error {
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
		log.Infof(context.Background(), "cluster took %s to rebalance", rebalanceInterval)
	}

	// Output # of range events that occurred. All other things being equal,
	// larger numbers are worse and potentially indicate thrashing.
	{
		var rangeEvents int64
		q := `SELECT COUNT(*) from rangelog`
		if err := db.QueryRow(q).Scan(&rangeEvents); err != nil {
			return err
		}
		log.Infof(context.Background(), "%d range events", rangeEvents)
	}

	// Output standard deviation of the replica counts for all stores.
	stdDev, err := at.stdDev()
	if err != nil {
		return err
	}
	log.Infof(context.Background(), "stddev(replica count) = %.2f", stdDev)

	return nil
}

type replicationStats struct {
	ElapsedSinceLastEvent time.Duration
	EventType             string
	RangeID               int64
	StoreID               int64
	ReplicaCountStdDev    float64
}

func (s replicationStats) String() string {
	return fmt.Sprintf("last range event: %s for range %d/store %d (%s ago)",
		s.EventType, s.RangeID, s.StoreID, s.ElapsedSinceLastEvent)
}

// checkAllocatorStable returns the duration of stability (i.e. no replication
// changes) and the standard deviation in replica counts. Only unrecoverable
// errors are returned.
func (at *allocatorTest) allocatorStats(db *gosql.DB) (s replicationStats, err error) {
	defer func() {
		if err != nil {
			s.ReplicaCountStdDev = math.MaxFloat64
		}
	}()

	q := `SELECT NOW()-timestamp, rangeID, storeID, eventType FROM rangelog WHERE ` +
		`timestamp=(SELECT MAX(timestamp) FROM rangelog WHERE eventType IN ($1, $2, $3))`
	eventTypes := []interface{}{
		string(storage.RangeEventLogSplit),
		string(storage.RangeEventLogAdd),
		string(storage.RangeEventLogRemove),
	}

	var elapsedStr string

	row := db.QueryRow(q, eventTypes...)
	if row == nil {
		// This should never happen, because the archived store we're starting with
		// will always have some range events.
		return replicationStats{}, errors.New("couldn't find any range events")
	}
	if err := row.Scan(&elapsedStr, &s.RangeID, &s.StoreID, &s.EventType); err != nil {
		return replicationStats{}, err
	}

	s.ElapsedSinceLastEvent, err = time.ParseDuration(elapsedStr)
	if err != nil {
		return replicationStats{}, err
	}

	s.ReplicaCountStdDev, err = at.stdDev()
	if err != nil {
		return replicationStats{}, err
	}

	return s, nil
}

// WaitForRebalance waits until there's been no recent range adds, removes, and
// splits. We wait until the cluster is balanced or until `StableInterval`
// elapses, whichever comes first. Then, it prints stats about the rebalancing
// process. If the replica count appears unbalanced, an error is returned.
//
// This method is crude but necessary. If we were to wait until range counts
// were just about even, we'd miss potential post-rebalance thrashing.
func (at *allocatorTest) WaitForRebalance(t *testing.T) error {
	const statsInterval = 20 * time.Second

	db, err := gosql.Open("postgres", at.f.PGUrl(0))
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	var statsTimer timeutil.Timer
	var assertTimer timeutil.Timer
	defer statsTimer.Stop()
	defer assertTimer.Stop()
	statsTimer.Reset(statsInterval)
	assertTimer.Reset(0)
	for {
		select {
		case <-statsTimer.C:
			statsTimer.Read = true
			stats, err := at.allocatorStats(db)
			if err != nil {
				return err
			}

			log.Info(context.Background(), stats)
			if StableInterval <= stats.ElapsedSinceLastEvent {
				host := at.f.Nodes()[0]
				log.Infof(context.Background(), "replica count = %f, max = %f", stats.ReplicaCountStdDev, *flagATMaxStdDev)
				if stats.ReplicaCountStdDev > *flagATMaxStdDev {
					_ = at.printRebalanceStats(db, host)
					return errors.Errorf(
						"%s elapsed without changes, but replica count standard "+
							"deviation is %.2f (>%.2f)", stats.ElapsedSinceLastEvent,
						stats.ReplicaCountStdDev, *flagATMaxStdDev)
				}
				return at.printRebalanceStats(db, host)
			}
			statsTimer.Reset(statsInterval)
		case <-assertTimer.C:
			assertTimer.Read = true
			at.f.Assert(t)
			assertTimer.Reset(time.Minute)
		case <-stopper:
			return errors.New("interrupted")
		}
	}
}

// TestUpreplicate_1To3Small tests up-replication, starting with 1 node
// containing 10 GiB of data and growing to 3 nodes.
func TestUpreplicate_1To3Small(t *testing.T) {
	at := allocatorTest{
		StartNodes: 1,
		EndNodes:   3,
		StoreURL:   urlStore1s,
		Prefix:     "uprep-1to3s",
	}
	at.RunAndCleanup(t)
}

// TestRebalance3to5Small tests rebalancing, starting with 3 nodes (each
// containing 10 GiB of data) and growing to 5 nodes.
func TestRebalance_3To5Small(t *testing.T) {
	at := allocatorTest{
		StartNodes: 3,
		EndNodes:   5,
		StoreURL:   urlStore3s,
		Prefix:     "rebal-3to5s",
	}
	at.RunAndCleanup(t)
}

// TestUpreplicate_1To3Medium tests up-replication, starting with 1 node
// containing 108 GiB of data and growing to 3 nodes.
func TestUpreplicate_1To3Medium(t *testing.T) {
	at := allocatorTest{
		StartNodes:          1,
		EndNodes:            3,
		StoreURL:            urlStore1m,
		Prefix:              "uprep-1to3m",
		CockroachDiskSizeGB: 250, // GB
	}
	at.RunAndCleanup(t)
}

// TestUpreplicate_1To6Medium tests up-replication (and, to a lesser extent,
// rebalancing), starting with 1 node containing 108 GiB of data and growing to
// 6 nodes.
func TestUpreplicate_1To6Medium(t *testing.T) {
	at := allocatorTest{
		StartNodes:          1,
		EndNodes:            6,
		StoreURL:            urlStore1m,
		Prefix:              "uprep-1to6m",
		CockroachDiskSizeGB: 250, // GB
	}
	at.RunAndCleanup(t)
}

// TestSteady_6Medium is useful for creating a medium-size balanced cluster
// (when used with the -tf.keep-cluster flag).
// TODO(tschottdorf): use for tests which run schema changes or drop large
// amounts of data.
func TestSteady_6Medium(t *testing.T) {
	at := allocatorTest{
		StartNodes:          6,
		EndNodes:            6,
		StoreURL:            urlStore6m,
		Prefix:              "steady-6m",
		CockroachDiskSizeGB: 250, // GB
	}
	at.RunAndCleanup(t)
}
