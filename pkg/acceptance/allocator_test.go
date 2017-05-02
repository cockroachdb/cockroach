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
// 1. Have an Azure account.
// 2. Passphrase-less SSH key in ~/.ssh/{azure,azure.pub}.
// 3. Set the ARM_SUBSCRIPTION_ID, ARM_CLIENT_ID, ARM_CLIENT_SECRET, and
//    ARM_TENANT_ID variables as documented here:
//    https://www.terraform.io/docs/providers/azurerm/#argument-reference
//
// Example use:
//
// make test \
//	 TESTTIMEOUT=48h \
//	 PKG=./pkg/acceptance \
//	 TESTS='^TestRebalance_3To5Small$$' \
//	 TESTFLAGS='-v -remote -key-name azure -cwd terraform/azure -tf.keep-cluster=failed'
//
// Things to note:
// - You must use an SSH key without a passphrase. This is a Terraform
//   requirement.
// - If you want to manually fiddle with a test cluster, start the test with
//   `-tf.keep-cluster=failed". After the cluster has been created, press
//   Control-C and the cluster will remain up.
// - These tests rely on a specific Terraform config that's specified using the
//   -cwd test flag.
// - You *must* set the TESTTIMEOUT high enough for any of these tests to
//   finish. To be really safe, specify a timeout of at least 24 hours.
// - There are various flags that start with `tf.` that control the
//   of Terrafarm and allocator tests, respectively. For example, you can add
//   "-tf.cockroach-binary" to TESTFLAGS to specify a custom Linux CockroachDB
//   binary. If omitted, your test will use the latest CircleCI Linux build.
//   Note that the location has to be specified relative to the terraform
//   working directory, so typically `-tf.cockroach-binary=../../cockroach`.
//   This must be a linux binary; on a mac you must build with
//   `build/builder.sh make build`.
//
// Troubleshooting:
// - The minimum recommended version of Terraform is 0.7.2. If you see strange
//   Terraform errors, upgrade your install of Terraform. Terraform 0.8.x or
//   later might not work because of breaking changes to Terraform.
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

	"github.com/cockroachdb/cockroach/pkg/acceptance/terrafarm"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	// Run some schema changes during the rebalancing.
	RunSchemaChanges bool

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

func (at *allocatorTest) Run(ctx context.Context, t *testing.T) {
	at.f = MakeFarmer(t, at.Prefix, stopper)

	if at.CockroachDiskSizeGB != 0 {
		at.f.AddVars["cockroach_disk_size"] = strconv.Itoa(at.CockroachDiskSizeGB)
	}

	log.Infof(ctx, "creating cluster with %d node(s)", at.StartNodes)
	if err := at.f.Resize(at.StartNodes); err != nil {
		t.Fatal(err)
	}
	CheckGossip(ctx, t, at.f, longWaitTime, HasPeers(at.StartNodes))
	at.f.Assert(ctx, t)
	log.Info(ctx, "initial cluster is up")

	// We must stop the cluster because a) `nodectl` pokes at the data directory
	// and, more importantly, b) we don't want the cluster above and the cluster
	// below to ever talk to each other (see #7224).
	log.Info(ctx, "stopping cluster")
	for i := 0; i < at.f.NumNodes(); i++ {
		if err := at.f.Kill(ctx, i); err != nil {
			t.Fatalf("error stopping node %d: %s", i, err)
		}
	}

	log.Info(ctx, "downloading archived stores from Google Cloud Storage in parallel")
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

	log.Info(ctx, "restarting cluster with archived store(s)")
	for i := 0; i < at.f.NumNodes(); i++ {
		if err := at.f.Restart(ctx, i); err != nil {
			t.Fatalf("error restarting node %d: %s", i, err)
		}
	}
	at.f.Assert(ctx, t)

	log.Infof(ctx, "resizing cluster to %d nodes", at.EndNodes)
	if err := at.f.Resize(at.EndNodes); err != nil {
		t.Fatal(err)
	}

	CheckGossip(ctx, t, at.f, longWaitTime, HasPeers(at.EndNodes))
	at.f.Assert(ctx, t)

	log.Infof(ctx, "starting load on cluster")
	if err := at.f.StartLoad(ctx, "block_writer", *flagCLTWriters); err != nil {
		t.Fatal(err)
	}
	if err := at.f.StartLoad(ctx, "photos", *flagCLTWriters); err != nil {
		t.Fatal(err)
	}

	if at.RunSchemaChanges {
		log.Info(ctx, "running schema changes while cluster is rebalancing")
		if err := at.runSchemaChanges(ctx, t); err != nil {
			t.Fatal(err)
		}
	}

	log.Info(ctx, "waiting for rebalance to finish")
	if err := at.WaitForRebalance(ctx, t); err != nil {
		t.Fatal(err)
	}

	at.f.Assert(ctx, t)
}

func (at *allocatorTest) RunAndCleanup(ctx context.Context, t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	defer at.Cleanup(t)
	at.Run(ctx, t)
}

func (at *allocatorTest) runSchemaChanges(ctx context.Context, t *testing.T) error {
	db, err := gosql.Open("postgres", at.f.PGUrl(ctx, 0))
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	const tableName = "datablocks.blocks"
	schemaChanges := []string{
		"ALTER TABLE %s ADD COLUMN newcol DECIMAL DEFAULT (DECIMAL '1.4')",
		"CREATE INDEX foo ON %s (block_id)",
	}

	errChan := make(chan error)
	for i := range schemaChanges {
		go func(i int) {
			start := timeutil.Now()
			cmd := fmt.Sprintf(schemaChanges[i], tableName)
			log.Infof(ctx, "starting schema change: %s", cmd)
			if _, err := db.Exec(cmd); err != nil {
				errChan <- errors.Errorf("hit schema change error: %s, for %s, in %s", err, cmd, timeutil.Since(start))
				return
			}
			log.Infof(ctx, "completed schema change: %s, in %s", cmd, timeutil.Since(start))
			errChan <- nil
			// TODO(vivek): Monitor progress of schema changes and log progress.
		}(i)
	}

	for range schemaChanges {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}

	log.Info(ctx, "validate applied schema changes")
	if err := at.ValidateSchemaChanges(ctx, t); err != nil {
		t.Fatal(err)
	}
	return nil
}

func (at *allocatorTest) ValidateSchemaChanges(ctx context.Context, t *testing.T) error {
	db, err := gosql.Open("postgres", at.f.PGUrl(ctx, 0))
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	const tableName = "datablocks.blocks"
	var now string
	if err := db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	var eCount int64
	q := fmt.Sprintf(`SELECT COUNT(*) FROM %s AS OF SYSTEM TIME %s`, tableName, now)
	if err := db.QueryRow(q).Scan(&eCount); err != nil {
		return err
	}
	log.Infof(ctx, "%s: %d rows", q, eCount)

	// Validate the different schema changes
	validationQueries := []string{
		"SELECT COUNT(newcol) FROM %s AS OF SYSTEM TIME %s",
		"SELECT COUNT(block_id) FROM %s@foo AS OF SYSTEM TIME %s",
	}
	for i := range validationQueries {
		var count int64
		q := fmt.Sprintf(validationQueries[i], tableName, now)
		if err := db.QueryRow(q).Scan(&count); err != nil {
			return err
		}
		log.Infof(ctx, "query: %s, found %d rows", q, count)
		if count != eCount {
			t.Fatalf("%s: %d rows found, expected %d rows", q, count, eCount)
		}
	}
	return nil
}

func (at *allocatorTest) stdDev() (float64, error) {
	host := at.f.Hostname(0)
	var client http.Client
	var nodesResp serverpb.NodesResponse
	url := fmt.Sprintf("http://%s:%s/_status/nodes", host, adminPort)
	if err := httputil.GetJSON(client, url, &nodesResp); err != nil {
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
func (at *allocatorTest) printRebalanceStats(db *gosql.DB, host string) error {
	// TODO(cuongdo): Output these in a machine-friendly way and graph.

	// Output time it took to rebalance.
	{
		var rebalanceIntervalStr string
		if err := db.QueryRow(
			`SELECT (SELECT MAX(timestamp) FROM rangelog) - (SELECT MAX(timestamp) FROM eventlog WHERE eventType=$1)`,
			sql.EventLogNodeJoin,
		).Scan(&rebalanceIntervalStr); err != nil {
			return err
		}
		rebalanceInterval, err := parser.ParseDInterval(rebalanceIntervalStr)
		if err != nil {
			return err
		}
		if rebalanceInterval.Duration.Compare(duration.Duration{}) < 0 {
			log.Warningf(context.Background(), "test finished, but clock moved backward")
		} else {
			log.Infof(context.Background(), "cluster took %s to rebalance", rebalanceInterval)
		}
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
	log.Infof(context.Background(), "stdDev(replica count) = %.2f", stdDev)

	return nil
}

type replicationStats struct {
	ElapsedSinceLastEvent duration.Duration
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

	eventTypes := []interface{}{
		string(storage.RangeEventLogSplit),
		string(storage.RangeEventLogAdd),
		string(storage.RangeEventLogRemove),
	}

	q := `SELECT NOW()-timestamp, rangeID, storeID, eventType FROM rangelog WHERE ` +
		`timestamp=(SELECT MAX(timestamp) FROM rangelog WHERE eventType IN ($1, $2, $3))`

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
	elapsedSinceLastEvent, err := parser.ParseDInterval(elapsedStr)
	if err != nil {
		return replicationStats{}, err
	}
	s.ElapsedSinceLastEvent = elapsedSinceLastEvent.Duration

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
func (at *allocatorTest) WaitForRebalance(ctx context.Context, t *testing.T) error {
	const statsInterval = 20 * time.Second

	db, err := gosql.Open("postgres", at.f.PGUrl(ctx, 0))
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

			log.Info(ctx, stats)
			stableDuration := duration.Duration{Nanos: StableInterval.Nanoseconds()}
			if stableDuration.Compare(stats.ElapsedSinceLastEvent) <= 0 {
				host := at.f.Hostname(0)
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
			at.f.Assert(ctx, t)
			assertTimer.Reset(time.Minute)
		case <-stopper.ShouldStop():
			return errors.New("interrupted")
		}
	}
}

// TestUpreplicate_1To3Small tests up-replication, starting with 1 node
// containing 10 GiB of data and growing to 3 nodes.
func TestUpreplicate_1To3Small(t *testing.T) {
	ctx := context.Background()
	at := allocatorTest{
		StartNodes: 1,
		EndNodes:   3,
		StoreURL:   urlStore1s,
		Prefix:     "uprep-1to3s",
	}
	at.RunAndCleanup(ctx, t)
}

// TestRebalance3to5Small_WithSchemaChanges tests rebalancing in
// the midst of schema changes, starting with 3 nodes (each
// containing 10 GiB of data) and growing to 5 nodes.
func TestRebalance_3To5Small_WithSchemaChanges(t *testing.T) {
	ctx := context.Background()
	at := allocatorTest{
		StartNodes:       3,
		EndNodes:         5,
		StoreURL:         urlStore3s,
		Prefix:           "rebal-3to5s",
		RunSchemaChanges: true,
	}
	at.RunAndCleanup(ctx, t)
}

// TestRebalance3to5Small tests rebalancing, starting with 3 nodes (each
// containing 10 GiB of data) and growing to 5 nodes.
func TestRebalance_3To5Small(t *testing.T) {
	ctx := context.Background()
	at := allocatorTest{
		StartNodes: 3,
		EndNodes:   5,
		StoreURL:   urlStore3s,
		Prefix:     "rebal-3to5s",
	}
	at.RunAndCleanup(ctx, t)
}

// TestUpreplicate_1To3Medium tests up-replication, starting with 1 node
// containing 108 GiB of data and growing to 3 nodes.
func TestUpreplicate_1To3Medium(t *testing.T) {
	ctx := context.Background()
	at := allocatorTest{
		StartNodes:          1,
		EndNodes:            3,
		StoreURL:            urlStore1m,
		Prefix:              "uprep-1to3m",
		CockroachDiskSizeGB: 250, // GB
	}
	at.RunAndCleanup(ctx, t)
}

// TestUpreplicate_1To6Medium tests up-replication (and, to a lesser extent,
// rebalancing), starting with 1 node containing 108 GiB of data and growing to
// 6 nodes.
func TestUpreplicate_1To6Medium(t *testing.T) {
	ctx := context.Background()
	at := allocatorTest{
		StartNodes:          1,
		EndNodes:            6,
		StoreURL:            urlStore1m,
		Prefix:              "uprep-1to6m",
		CockroachDiskSizeGB: 250, // GB
	}
	at.RunAndCleanup(ctx, t)
}

// TestSteady_6Medium is useful for creating a medium-size balanced cluster
// (when used with the -tf.keep-cluster flag).
// TODO(vivek): use for tests which drop large amounts of data.
func TestSteady_6Medium(t *testing.T) {
	ctx := context.Background()
	at := allocatorTest{
		StartNodes:          6,
		EndNodes:            6,
		StoreURL:            urlStore6m,
		Prefix:              "steady-6m",
		CockroachDiskSizeGB: 250, // GB
		RunSchemaChanges:    true,
	}
	at.RunAndCleanup(ctx, t)
}
