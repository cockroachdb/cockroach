// Copyright 2015 The Cockroach Authors.
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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// TestDecommission starts up an >3 node cluster and decomissions and
// recommissions nodes in various ways.
func TestDecommission(t *testing.T) {
	RunLocal(t, func(t *testing.T) {
		s := log.Scope(t)
		defer s.Close(t)

		runTestWithCluster(t, testDecommissionInner)
	})
}

func decommission(
	ctx context.Context,
	c cluster.Cluster,
	runNode int,
	targetNodes []roachpb.NodeID,
	verbs ...string,
) (string, string, error) {
	args := append([]string{"node"}, verbs...)
	for _, target := range targetNodes {
		args = append(args, strconv.Itoa(int(target)))
	}
	o, e, err := c.ExecCLI(ctx, runNode, args)
	return o, e, err
}

func matchCSV(csvStr string, matchColRow [][]string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Errorf("csv input:\n%v\nexpected:\n%s\nerrors:%s", csvStr, pretty.Sprint(matchColRow), err)
		}
	}()

	reader := csv.NewReader(strings.NewReader(csvStr))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	lr, lm := len(records), len(matchColRow)
	if lr < lm {
		return errors.Errorf("csv has %d rows, but expected at least %d", lr, lm)
	}

	// Compare only the last len(matchColRow) records. That is, if we want to
	// match 4 rows and we have 100 records, we only really compare
	// records[96:], that is, the last four rows.
	records = records[lr-lm:]

	for i := range records {
		if lr, lm := len(records[i]), len(matchColRow[i]); lr != lm {
			return errors.Errorf("row #%d: csv has %d columns, but expected %d", i+1, lr, lm)
		}
		for j := range records[i] {
			pat, str := matchColRow[i][j], records[i][j]
			re := regexp.MustCompile(pat)
			if !re.MatchString(str) {
				err = errors.Errorf("%v\nrow #%d, col #%d: found %q which does not match %q", err, i+1, j+1, str, pat)
			}
		}
	}
	return err
}

func testDecommissionInner(
	ctx context.Context, t *testing.T, c cluster.Cluster, cfg cluster.TestConfig,
) {
	if c.NumNodes() < 4 {
		// TODO(tschottdorf): or we invent a way to change the ZoneConfig in
		// this test and test less ambitiously (or split up the tests).
		t.Skip("need at least four nodes")
	}

	withDB := func(n int, stmt string) {
		db, err := gosql.Open("postgres", c.PGUrl(ctx, n))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	withDB(1, "SET CLUSTER SETTING server.remote_debugging.mode = 'any';")

	// Get the ids for each node.
	idMap := make(map[int]roachpb.NodeID)
	for i := 0; i < c.NumNodes(); i++ {
		var details serverpb.DetailsResponse
		if err := httputil.GetJSON(cluster.HTTPClient, c.URL(ctx, i)+"/_status/details/local", &details); err != nil {
			t.Fatal(err)
		}
		idMap[i] = details.NodeID
	}

	decommissionHeader := []string{"id", "is_live", "replicas", "is_decommissioning", "is_draining"}
	decommissionFooter := []string{"No more data reported on target nodes. Please verify cluster health before removing the nodes."}
	waitLiveDeprecated := "--wait=live is deprecated and is treated as --wait=all"

	statusHeader := []string{"id", "address", "build", "started_at", "updated_at", "is_live"}

	log.Info(ctx, "decommissioning first node from the second, polling the status manually")
	retryOpts := retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     1,
		MaxRetries:     20,
	}
	for r := retry.Start(retryOpts); r.Next(); {
		o, _, err := decommission(ctx, c, 1, []roachpb.NodeID{idMap[0]}, "decommission", "--wait", "none", "--format", "csv")
		if err != nil {
			t.Fatal(err)
		}

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(int(idMap[0])), "true", "0", "true", "false"},
			decommissionFooter,
		}
		log.Infof(ctx, o)

		if err := matchCSV(o, exp); err != nil {
			continue
		}
		break
	}

	// Check that even though the node is decommissioned, we still see it (since
	// it remains live) in `node ls`.
	{
		o, _, err := c.ExecCLI(ctx, 2, []string{"node", "ls", "--format", "csv"})
		if err != nil {
			t.Fatal(err)
		}
		exp := [][]string{
			{"id"},
			{"1"},
			{"2"},
			{"3"},
			{"4"},
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}
	// Ditto `node status`.
	{
		o, _, err := c.ExecCLI(ctx, 2, []string{"node", "status", "--format", "csv"})
		if err != nil {
			t.Fatal(err)
		}
		exp := [][]string{
			statusHeader,
			{`1`, `.*`, `.*`, `.*`, `.*`, `.*`},
			{`2`, `.*`, `.*`, `.*`, `.*`, `.*`},
			{`3`, `.*`, `.*`, `.*`, `.*`, `.*`},
			{`4`, `.*`, `.*`, `.*`, `.*`, `.*`},
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	log.Info(ctx, "recommissioning first node (from third node)")
	{
		o, _, err := decommission(ctx, c, 2, []roachpb.NodeID{idMap[0]}, "recommission")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
	}

	log.Info(ctx, "decommissioning second node from third, using --wait=all")
	{
		target := idMap[1]
		o, _, err := decommission(ctx, c, 2, []roachpb.NodeID{target}, "decommission", "--wait", "all", "--format", "csv")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(int(target)), "true", "0", "true", "false"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	log.Info(ctx, "recommissioning second node from itself")
	{
		o, _, err := decommission(ctx, c, 1, []roachpb.NodeID{idMap[1]}, "recommission")
		if err != nil {
			t.Fatalf("could no recommission: %s\n%s", err, o)
		}
		log.Infof(ctx, o)
	}

	log.Info(ctx, "decommissioning third node via `quit --decommission`")
	{
		// This should not take longer than five minutes, and if it does, it's
		// likely stuck forever and we want to see the output.
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		o, e, err := c.ExecCLI(timeoutCtx, 2, []string{"quit", "--decommission"})
		if err != nil {
			if timeoutCtx.Err() != nil {
				t.Fatalf("quit --decommission failed: %s\nstdout:\n%s\nstderr:\n%s", err, o, e)
			}
			// TODO(tschottdorf): grep the process output for the string announcing success?
			log.Warningf(ctx, "ignoring error on quit --decommission: %s", err)
		} else {
			log.Infof(ctx, o, e)
		}

		// Kill the node to generate the expected event (Kill is idempotent, so this works).
		if err := c.Kill(ctx, 2); err != nil {
			log.Warning(ctx, err)
		}
	}

	// Now that the third node is down and decommissioned, decommissioning it
	// again should be a no-op. We do it from node one but as always it doesn't
	// matter.
	log.Info(ctx, "checking that other nodes see node three as successfully decommissioned")
	{
		target := idMap[2]
		o, _, err := decommission(ctx, c, 1, []roachpb.NodeID{target}, "decommission", "--format", "csv") // wait=all is implied
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)

		exp := [][]string{
			decommissionHeader,
			// Expect the same as usual, except this time the node should be draining
			// because it shut down cleanly (thanks to `quit --decommission`).
			{strconv.Itoa(int(target)), "true", "0", "true", "true"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}

		// Bring the node back up. It's still decommissioned, so it won't be of much use.
		if err := c.Restart(ctx, 2); err != nil {
			t.Fatal(err)
		}

		// Recommission. Welcome back!
		o, _, err = decommission(ctx, c, 1, []roachpb.NodeID{target}, "recommission")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
	}

	// Kill the first node and verify that we can decommission it while it's down,
	// bringing it back up to verify that its replicas still get removed.
	log.Info(ctx, "intentionally killing first node")
	if err := c.Kill(ctx, 0); err != nil {
		t.Fatal(err)
	}
	log.Info(ctx, "decommission first node, starting with it down but restarting it for verification")
	{
		target := idMap[0]
		o, e, err := decommission(ctx, c, 2, []roachpb.NodeID{target}, "decommission", "--wait", "live")
		if err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, o)
		if strings.Split(e, "\n")[1] != waitLiveDeprecated {
			t.Fatal("missing deprecate message for --wait=live")
		}
		if err := c.Restart(ctx, 0); err != nil {
			t.Fatal(err)
		}
		// Run a second time to wait until the replicas have all been GC'ed.
		// Note that we specify "all" because even though the first node is
		// now running, it may not be live by the time the command runs.
		o, _, err = decommission(ctx, c, 2, []roachpb.NodeID{target}, "decommission", "--wait", "all", "--format", "csv")
		if err != nil {
			t.Fatal(err)
		}

		log.Info(ctx, o)

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(int(target)), "true", "0", "true", "false"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	// Now we want to test decommissioning a truly dead node. Make sure we don't
	// waste too much time waiting for the node to be recognized as dead. Note that
	// we don't want to set this number too low or everything will seem dead to the
	// allocator at all times, so nothing will ever happen.
	withDB(1, "SET CLUSTER SETTING server.time_until_store_dead = '1m15s'")

	log.Info(ctx, "intentionally killing first node")
	if err := c.Kill(ctx, 0); err != nil {
		t.Fatal(err)
	}
	// It is being decommissioned in absentia, meaning that its replicas are
	// being removed due to deadness. We can't see that reflected in the output
	// since the current mechanism gets its replica counts from what the node
	// reports about itself, so our assertion here is somewhat weak.
	log.Info(ctx, "decommission first node in absentia using --wait=live")
	{
		target := idMap[0]
		o, e, err := decommission(ctx, c, 2, []roachpb.NodeID{target}, "decommission", "--wait", "live", "--format", "csv")
		if err != nil {
			t.Fatal(err)
		}

		log.Infof(ctx, o)

		// Note we don't check precisely zero replicas (which the node would write
		// itself, but it's dead). We do check that the node isn't live, though, which
		// is essentially what `--wait=live` waits for.
		// Note that the target node may still be "live" when it's marked as
		// decommissioned, as its replica count may drop to zero faster than
		// liveness times out.
		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(int(target)), `true|false`, "0", `true`, `false`},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
		if strings.Split(e, "\n")[1] != waitLiveDeprecated {
			t.Fatal("missing deprecate message for --wait=live")
		}
	}

	// Check that (at least after a bit) the node disappears from `node ls`
	// because it is decommissioned and not live.
	for {
		o, _, err := c.ExecCLI(ctx, 2, []string{"node", "ls", "--format", "csv"})
		if err != nil {
			t.Fatal(err)
		}

		log.Info(ctx, o)

		exp := [][]string{
			{"id"},
			{"2"},
			{"3"},
			{"4"},
		}

		if err := matchCSV(o, exp); err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	for {
		o, _, err := c.ExecCLI(ctx, 2, []string{"node", "status", "--format", "csv"})
		if err != nil {
			t.Fatal(err)
		}

		log.Info(ctx, o)

		exp := [][]string{
			statusHeader,
			{`2`, `.*`, `.*`, `.*`, `.*`, `.*`},
			{`3`, `.*`, `.*`, `.*`, `.*`, `.*`},
			{`4`, `.*`, `.*`, `.*`, `.*`, `.*`},
		}
		if err := matchCSV(o, exp); err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	var rows *gosql.Rows
	if err := retry.ForDuration(time.Minute, func() error {
		// Verify the event log has recorded exactly one decommissioned or
		// recommissioned event for each commissioning operation.
		//
		// Spurious errors appear to be possible since we might be trying to
		// send RPCs to the (relatively recently) down node:
		//
		// pq: rpc error: code = Unavailable desc = grpc: the connection is
		// unavailable
		//
		// Seen in https://teamcity.cockroachdb.com/viewLog.html?buildId=344802.
		db, err := gosql.Open("postgres", c.PGUrl(ctx, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		rows, err = db.Query(`
		SELECT "eventType", "targetID" FROM system.eventlog
		WHERE "eventType" IN ($1, $2) ORDER BY timestamp`,
			sql.EventLogNodeDecommissioned, sql.EventLogNodeRecommissioned,
		)
		if err != nil {
			log.Warning(ctx, errors.Wrap(err, "retrying after"))
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	matrix, err := sqlutils.RowsToStrMatrix(rows)
	if err != nil {
		t.Fatal(err)
	}
	expMatrix := [][]string{
		{string(sql.EventLogNodeDecommissioned), idMap[0].String()},
		{string(sql.EventLogNodeRecommissioned), idMap[0].String()},
		{string(sql.EventLogNodeDecommissioned), idMap[1].String()},
		{string(sql.EventLogNodeRecommissioned), idMap[1].String()},
		{string(sql.EventLogNodeDecommissioned), idMap[2].String()},
		{string(sql.EventLogNodeRecommissioned), idMap[2].String()},
		{string(sql.EventLogNodeDecommissioned), idMap[0].String()},
	}

	if !reflect.DeepEqual(matrix, expMatrix) {
		t.Fatalf("unexpected diff(matrix, expMatrix):\n%s", pretty.Diff(matrix, expMatrix))
	}

	// Last, verify that the operator can't shoot themselves in the foot by
	// accidentally decommissioning all nodes.
	var allNodeIDs []roachpb.NodeID
	for _, nodeID := range idMap {
		allNodeIDs = append(allNodeIDs, nodeID)
	}

	// Specify wait=none because the command would block forever (the replicas have
	// nowhere to go).
	if _, _, err := decommission(
		ctx, c, 1, allNodeIDs, "decommission", "--wait", "none",
	); err != nil {
		t.Fatal(err)
	}

	// Check that we can still do stuff. Creating a database should be good enough.
	db, err := gosql.Open("postgres", c.PGUrl(ctx, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`CREATE DATABASE still_working;`); err != nil {
		t.Fatal(err)
	}

	// Recommission all nodes.
	if _, _, err := decommission(
		ctx, c, 1, allNodeIDs, "recommission",
	); err != nil {
		t.Fatal(err)
	}

	// To verify that all nodes are actually accepting replicas again, decommission
	// the first nodes (blocking until it's done). This proves that the other nodes
	// absorb the first one's replicas.
	if _, _, err := decommission(
		ctx, c, 1, []roachpb.NodeID{idMap[0]}, "decommission",
	); err != nil {
		t.Fatal(err)
	}
}
