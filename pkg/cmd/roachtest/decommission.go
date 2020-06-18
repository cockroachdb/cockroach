// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

// TODO(tschottdorf): verify that the logs don't contain the messages
// that would spam the log before #23605. I wonder if we should really
// start grepping the logs. An alternative is to introduce a metric
// that would have signaled this and check that instead.
func runDecommission(t *test, c *cluster, nodes int, duration time.Duration) {
	ctx := context.Background()

	const defaultReplicationFactor = 3
	// The number of nodes we're going to cycle through. Since we're sometimes
	// killing the nodes and then removing them, this means having to be careful
	// with loss of quorum. So only ever touch a fixed minority of nodes and
	// swap them out for as long as the test runs. The math boils down to `1`,
	// but conceivably we'll want to run a test with replication factor five
	// at some point.
	numDecom := (defaultReplicationFactor - 1) / 2

	c.Put(ctx, workload, "./workload", c.Node(nodes))
	c.Put(ctx, cockroach, "./cockroach", c.All())

	for i := 1; i <= numDecom; i++ {
		c.Start(ctx, t, c.Node(i), startArgs(fmt.Sprintf("-a=--attrs=node%d", i)))
	}

	c.Start(ctx, t, c.Range(numDecom+1, nodes))
	c.Run(ctx, c.Node(nodes), `./workload init kv --drop`)

	waitReplicatedAwayFrom := func(downNodeID string) error {
		db := c.Conn(ctx, nodes)
		defer func() {
			_ = db.Close()
		}()

		for {
			var count int
			if err := db.QueryRow(
				// Check if the down node has any replicas.
				"SELECT count(*) FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NOT NULL",
				downNodeID,
			).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				fullReplicated := false
				if err := db.QueryRow(
					// Check if all ranges are fully replicated.
					"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
				).Scan(&fullReplicated); err != nil {
					return err
				}
				if fullReplicated {
					break
				}
			}
			time.Sleep(time.Second)
		}
		return nil
	}

	waitUpReplicated := func(targetNodeID string) error {
		db := c.Conn(ctx, nodes)
		defer func() {
			_ = db.Close()
		}()

		for ok := false; !ok; {
			stmtReplicaCount := fmt.Sprintf(
				`SELECT count(*) = 0 FROM crdb_internal.ranges WHERE array_position(replicas, %s) IS NULL and database_name = 'kv';`, targetNodeID)
			t.Status(stmtReplicaCount)
			if err := db.QueryRow(stmtReplicaCount).Scan(&ok); err != nil {
				return err
			}
			time.Sleep(time.Second)
		}
		return nil
	}

	if err := waitReplicatedAwayFrom("0" /* no down node */); err != nil {
		t.Fatal(err)
	}

	loadDuration := " --duration=" + duration.String()

	workloads := []string{
		// TODO(tschottdorf): in remote mode, the ui shows that we consistently write
		// at 330 qps (despite asking for 500 below). Locally we get 500qps (and a lot
		// more without rate limiting). Check what's up with that.
		"./workload run kv --max-rate 500 --tolerate-errors" + loadDuration + " {pgurl:1-%d}",
	}

	run := func(stmtStr string) {
		db := c.Conn(ctx, nodes)
		defer db.Close()
		stmt := fmt.Sprintf(stmtStr, "", "=")
		// We are removing the EXPERIMENTAL keyword in 2.1. For compatibility
		// with 2.0 clusters we still need to try with it if the
		// syntax without EXPERIMENTAL fails.
		// TODO(knz): Remove this in 2.2.
		t.Status(stmt)
		_, err := db.ExecContext(ctx, stmt)
		if err != nil && strings.Contains(err.Error(), "syntax error") {
			stmt = fmt.Sprintf(stmtStr, "EXPERIMENTAL", "")
			t.Status(stmt)
			_, err = db.ExecContext(ctx, stmt)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.l.Printf("run: %s\n", stmt)
	}

	var m *errgroup.Group // see comment in version.go
	m, ctx = errgroup.WithContext(ctx)
	for _, cmd := range workloads {
		cmd := cmd // copy is important for goroutine

		cmd = fmt.Sprintf(cmd, nodes)
		m.Go(func() error {
			return c.RunE(ctx, c.Node(nodes), cmd)
		})
	}

	m.Go(func() error {
		nodeID := func(node int) (string, error) {
			dbNode := c.Conn(ctx, node)
			defer dbNode.Close()
			var nodeID string
			if err := dbNode.QueryRow(`SELECT node_id FROM crdb_internal.node_runtime_info LIMIT 1`).Scan(&nodeID); err != nil {
				return "", err
			}
			return nodeID, nil
		}

		stop := func(node int) error {
			port := fmt.Sprintf("{pgport:%d}", node)
			defer time.Sleep(time.Second) // work around quit returning too early
			return c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --host=:"+port)
		}

		decom := func(id string) error {
			port := fmt.Sprintf("{pgport:%d}", nodes) // always use last node
			t.Status("decommissioning node", id)
			return c.RunE(ctx, c.Node(nodes), "./cockroach node decommission --insecure --wait=all --host=:"+port+" "+id)
		}

		for tBegin, whileDown, node := timeutil.Now(), true, 1; timeutil.Since(tBegin) <= duration; whileDown, node = !whileDown, (node%numDecom)+1 {
			t.Status(fmt.Sprintf("decommissioning %d (down=%t)", node, whileDown))
			id, err := nodeID(node)
			if err != nil {
				return err
			}
			run(fmt.Sprintf(`ALTER RANGE default %%[1]s CONFIGURE ZONE %%[2]s 'constraints: {"+node%d"}'`, node))

			if err := waitUpReplicated(id); err != nil {
				return err
			}

			if whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}

			run(fmt.Sprintf(`ALTER RANGE default %%[1]s CONFIGURE ZONE %%[2]s 'constraints: {"-node%d"}'`, node))

			if err := decom(id); err != nil {
				return err
			}

			if err := waitReplicatedAwayFrom(id); err != nil {
				return err
			}

			if !whileDown {
				if err := stop(node); err != nil {
					return err
				}
			}

			if err := c.RunE(ctx, c.Node(node), "rm -rf {store-dir}"); err != nil {
				return err
			}

			db := c.Conn(ctx, 1)
			defer db.Close()

			c.Start(ctx, t, c.Node(node), startArgs(fmt.Sprintf("-a=--join %s --attrs=node%d",
				c.InternalAddr(ctx, c.Node(nodes))[0], node)))
		}
		// TODO(tschottdorf): run some ui sanity checks about decommissioned nodes
		// having disappeared. Verify that the workloads don't dip their qps or
		// show spikes in latencies.
		return nil
	})
	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
}

func registerDecommission(r *testRegistry) {
	{
		numNodes := 4
		duration := time.Hour

		r.Add(testSpec{
			Name:    fmt.Sprintf("decommission/nodes=%d/duration=%s", numNodes, duration),
			Owner:   OwnerKV,
			Cluster: makeClusterSpec(4),
			Run: func(ctx context.Context, t *test, c *cluster) {
				if local {
					duration = 3 * time.Minute
					t.l.Printf("running with duration=%s in local mode\n", duration)
				}
				runDecommission(t, c, numNodes, duration)
			},
		})
	}
	{
		numNodes := 9

		r.Add(testSpec{
			// This test was originally an acceptance test, we preserve its name.
			Name:    "acceptance/decommission",
			Owner:   OwnerKV,
			Timeout: 10 * time.Minute,
			Cluster: makeClusterSpec(numNodes),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runDecommissionAcceptance(ctx, t, c)
			},
		})
	}

}

func execCLI(
	ctx context.Context, t *test, c *cluster, runNode int, extraArgs ...string,
) (string, error) {
	args := []string{"./cockroach"}
	args = append(args, extraArgs...)
	args = append(args, "--insecure")
	args = append(args, fmt.Sprintf("--port={pgport:%d}", runNode))
	buf, err := c.RunWithBuffer(ctx, t.l, c.Node(runNode), args...)
	t.l.Printf("%s\n", buf)
	return string(buf), err
}

// runDecommissionAcceptance tests a bunch of node
// decommissioning/recommissioning procedures, all the while checking for
// replica movement and appropriate commission status detection behavior.
func runDecommissionAcceptance(ctx context.Context, t *test, c *cluster) {
	args := startArgs("--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t, args)

	commission := func(
		ctx context.Context,
		runNode int,
		targetNodes nodeListOption,
		verbs ...string,
	) (string, error) {
		args := []string{"node"}
		args = append(args, verbs...)
		for _, target := range targetNodes {
			args = append(args, strconv.Itoa(target))
		}
		return execCLI(ctx, t, c, runNode, args...)
	}

	getCsvNumCols := func(csvStr string) (cols int, err error) {
		reader := csv.NewReader(strings.NewReader(csvStr))
		records, err := reader.Read()
		if err != nil {
			return 0, errors.Errorf("error reading csv input: \n %v\n errors:%s", csvStr, err)
		}
		return len(records), nil
	}

	matchCSV := func(csvStr string, matchColRow [][]string) (err error) {
		defer func() {
			if err != nil {
				err = errors.Errorf("csv input:\n%v\nexpected:\n%s\nerrors:%s",
					csvStr, pretty.Sprint(matchColRow), err)
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
					err = errors.Errorf("%v\nrow #%d, col #%d: found %q which does not match %q",
						err, i+1, j+1, str, pat)
				}
			}
		}
		return err
	}

	decommissionHeader := []string{
		"id", "is_live", "replicas", "is_decommissioning", "commission_status", "is_draining",
	}
	decommissionFooter := []string{
		"No more data reported on target nodes. " +
			"Please verify cluster health before removing the nodes.",
	}

	// Different output here to be backwards compatible with earlier
	// versions of cockroach (versions pre commit 888813c, which
	// extends the node status command to include locality information).
	statusHeaderWithLocality := []string{
		"id", "address", "sql_address", "build", "started_at", "updated_at", "locality", "is_available", "is_live",
	}
	statusHeaderNoLocality := []string{
		"id", "address", "sql_address", "build", "started_at", "updated_at", "is_available", "is_live",
	}
	statusHeaderNoLocalityNoSQLAddress := []string{
		"id", "address", "build", "started_at", "updated_at", "is_available", "is_live",
	}
	statusHeaderWithLocalityCommissionStatus := []string{
		"id", "address", "sql_address", "build", "started_at", "updated_at", "locality", "is_available", "is_live",
		"gossiped_replicas", "is_decommissioning", "commission_status", "is_draining",
	}
	// Index of `commission_status` column in statusHeaderWithLocalityCommissionStatus
	const statusHeaderCommissionStatusColumn = 11

	getStatusCsvOutput := func(ids []string, numCols int) [][]string {
		var res [][]string
		switch numCols {
		case len(statusHeaderNoLocality):
			res = append(res, statusHeaderNoLocality)
		case len(statusHeaderWithLocality):
			res = append(res, statusHeaderWithLocality)
		case len(statusHeaderNoLocalityNoSQLAddress):
			res = append(res, statusHeaderNoLocalityNoSQLAddress)
		case len(statusHeaderWithLocalityCommissionStatus):
			res = append(res, statusHeaderWithLocalityCommissionStatus)
		default:
			t.Fatalf(
				"Expected status output numCols to be one of {%d, %d, %d or %d}, found %d",
				len(statusHeaderNoLocalityNoSQLAddress),
				len(statusHeaderNoLocality),
				len(statusHeaderWithLocality),
				len(statusHeaderWithLocalityCommissionStatus),
				numCols,
			)
		}
		for _, id := range ids {
			build := []string{id}
			for i := 0; i < numCols-1; i++ {
				build = append(build, `.*`)
			}
			res = append(res, build)
		}
		return res
	}
	matchColumn := func(column int, columnRegex []string, numRows, numCols int) [][]string {
		var res [][]string
		for r := 0; r < numRows; r++ {
			build := []string{}
			for c := 0; c < numCols; c++ {
				if c == column {
					build = append(build, columnRegex[r])
				} else {
					build = append(build, `.*`)
				}
			}
			res = append(res, build)
		}
		return res
	}

	retryOpts := retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}

	t.l.Printf("fully decommissioning first node from the second, polling the status manually\n")
	if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
		o, err := commission(ctx, 2, c.Node(1),
			"decommission", "--wait=none", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{"1", "true", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}

		return matchCSV(o, exp)
	}); err != nil {
		t.Fatal(err)
	}

	// Check that even though the node is decommissioned, we still see it (since
	// it remains live) in `node ls`.
	{
		o, err := execCLI(ctx, t, c, 2, "node", "ls", "--format=csv")
		if err != nil {
			t.Fatalf("node-ls failed: %v", err)
		}
		exp := [][]string{
			{"id"},
			{"1"},
			{"2"},
			{"3"},
			{"4"},
			{"5"},
			{"6"},
			{"7"},
			{"8"},
			{"9"},
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}
	// Ditto `node status`.
	{
		o, err := execCLI(ctx, t, c, 2, "node", "status", "--format=csv")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}
		exp := getStatusCsvOutput([]string{`1`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`}, numCols)
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	// We expect this to fail, seeing as how it's attempting to recommission a
	// fully decommissioned node.
	t.l.Printf("recommissioning first node (from third node), expecting to fail\n")
	if _, err := commission(ctx, 3, c.Node(1), "recommission"); err == nil {
		t.Fatal("expected recommission to fail")
	}

	t.l.Printf("partially decommissioning second node from the third\n")
	if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
		o, err := commission(ctx, 3, c.Node(2),
			"decommission", "--wait=none", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{"2", "true", `\d+`, "true", "decommissioning", "false"},
		}

		return matchCSV(o, exp)
	}); err != nil {
		t.Fatal(err)
	}

	// Check that `node status` reflects an ongoing decommissioning status for
	// the second node.
	{
		o, err := execCLI(ctx, t, c, 2, "node", "status", "--format=csv", "--decommission")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}

		exp := matchColumn(statusHeaderCommissionStatusColumn,
			[]string{
				`.*`,
				`decommissioning`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
			},
			9, numCols)
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	// Cancel in-flight decommissioning process of second node.
	t.l.Printf("recommissioning second node (from third)\n")
	if _, err := commission(ctx, 3, c.Node(2), "recommission"); err != nil {
		t.Fatalf("recommission failed: %v", err)
	}

	// Check that `node status` now reflects a 'commissioned' status for the
	// second node.
	{
		o, err := execCLI(ctx, t, c, 2, "node", "status", "--format=csv", "--decommission")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}
		exp := matchColumn(statusHeaderCommissionStatusColumn,
			[]string{
				`.*`,
				`commissioned`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
				`.*`,
			},
			9, numCols)
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	t.l.Printf("fully decommissioning second node from third, using --wait=all\n")
	{
		o, err := commission(ctx, 3, c.Node(2),
			"decommission", "--wait=all", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{"2", "true", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	t.l.Printf("recommissioning second node from itself, expecting to fail\n")
	if _, err := commission(ctx, 2, c.Node(2), "recommission"); err == nil {
		t.Fatal("expected recommission to failed")
	}

	t.l.Printf("fully decommissioning third node (from itself)\n")
	func() {
		// This should not take longer than five minutes, and if it does, it's
		// likely stuck forever and we want to see the output.
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		o, err := commission(timeoutCtx, 3, c.Node(3),
			"decommission", "--wait=all", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{"3", "true", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}()

	// Now that the third node is down and decommissioned, decommissioning it
	// again should be a no-op. We do it from node one but as always it doesn't
	// matter.
	t.l.Printf("checking that other nodes see node three as successfully decommissioned\n")
	{
		o, err := commission(ctx, 2, c.Node(3),
			"decommission", "--format=csv") // wait=all is implied
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{"3", "true", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}

		// Bring the node back up. It's still decommissioned, so it won't be of much use.
		c.Stop(ctx, c.Node(3))
		c.Start(ctx, t, c.Node(3), args)

		// Recommission. Expecting it to fail.
		if _, err = commission(ctx, 2, c.Node(3), "recommission"); err == nil {
			t.Fatalf("expected recommission to fail")
		}
	}

	// Kill the fourth node and verify that we can decommission it while it's down,
	// bringing it back up to verify that its replicas still get removed.
	t.l.Printf("intentionally killing fourth node\n")
	c.Stop(ctx, c.Node(4))
	t.l.Printf("decommission fourth node, starting with it down but restarting it for verification\n")
	{
		_, err := commission(ctx, 2, c.Node(4), "decommission", "--wait=all")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}
		c.Start(ctx, t, c.Node(4), args)

		// Run a second time to wait until the replicas have all been GC'ed.
		// Note that we specify "all" because even though the fourth node is
		// now running, it may not be live by the time the command runs.
		o, err := commission(ctx, 2, c.Node(4),
			"decommission", "--wait=all", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{"4", "true|false", "0", "true", "decommissioned", "false"},
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
	func() {
		db := c.Conn(ctx, 2)
		defer db.Close()
		const stmt = "SET CLUSTER SETTING server.time_until_store_dead = '1m15s'"
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}()

	t.l.Printf("intentionally killing fifth node\n")
	c.Stop(ctx, c.Node(5))
	// It is being decommissioned in absentia, meaning that its replicas are
	// being removed due to deadness. We can't see that reflected in the output
	// since the current mechanism gets its replica counts from what the node
	// reports about itself, so our assertion here is somewhat weak.
	t.l.Printf("decommission fifth node in absentia using --wait=all\n")
	{
		o, err := commission(ctx, 6, c.Node(5), "decommission", "--wait=all", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		// Note we don't check precisely zero replicas (which the node would write
		// itself, but it's dead). Also note that the target node may still be
		// "live" when it's marked as decommissioned, as its replica count may
		// drop to zero faster than liveness times out.
		exp := [][]string{
			decommissionHeader,
			{"5", `true|false`, "0", `true`, `decommissioned`, `false`},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	// Check that (at least after a bit) the node disappears from `node ls`
	// because it is decommissioned and not live.
	for {
		o, err := execCLI(ctx, t, c, 6, "node", "ls", "--format=csv")
		if err != nil {
			t.Fatalf("node-ls failed: %v", err)
		}

		exp := [][]string{
			{"id"},
			{"1"},
			{"2"},
			{"3"},
			{"4"},
			{"6"},
			{"7"},
			{"8"},
			{"9"},
		}

		if err := matchCSV(o, exp); err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	for {
		o, err := execCLI(ctx, t, c, 2, "node", "status", "--format=csv")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}
		exp := getStatusCsvOutput([]string{`1`, `2`, `3`, `4`, `6`, `7`, `8`, `9`}, numCols)
		if err := matchCSV(o, exp); err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	// Wipe data of node 5 and start it as a new node. It will join the cluster
	// with a node id of 10.
	//
	// This is done to verify that node status works when a new node is started
	// with an address belonging to an old decommissioned node.
	t.l.Printf("wiping fifth node and adding it back to the cluster as a new node\n")
	{
		c.Wipe(ctx, c.Node(5))
		c.Start(ctx, t, c.Node(5), startArgs(fmt.Sprintf("-a=--join %s",
			c.InternalAddr(ctx, c.Node(6))[0])))
	}

	if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
		o, err := execCLI(ctx, t, c, 6, "node", "status", "--format=csv")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}
		exp := getStatusCsvOutput([]string{`1`, `2`, `3`, `4`, `6`, `7`, `8`, `9`, `10`}, numCols)
		return matchCSV(o, exp)
	}); err != nil {
		t.Fatal(err)
	}

	t.l.Printf("verifying events posted to system.eventlog\n")
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
		db := c.Conn(ctx, 6)
		defer db.Close()

		rows, err := db.Query(`
	SELECT "eventType", "targetID" FROM system.eventlog
	WHERE "eventType" IN ($1, $2, $3) ORDER BY timestamp`,
			"node_decommissioned", "node_decommissioning", "node_recommissioned",
		)
		if err != nil {
			t.l.Printf("retrying: %v\n", err)
			return err
		}
		defer rows.Close()

		matrix, err := sqlutils.RowsToStrMatrix(rows)
		if err != nil {
			return err
		}

		expMatrix := [][]string{
			{"node_decommissioning", "1"},
			{"node_decommissioned", "1"},
			{"node_decommissioning", "2"},
			{"node_recommissioned", "2"},
			{"node_decommissioning", "2"},
			{"node_decommissioned", "2"},
			{"node_decommissioning", "3"},
			{"node_decommissioned", "3"},
			{"node_decommissioning", "4"},
			{"node_decommissioned", "4"},
			{"node_decommissioning", "5"},
			{"node_decommissioned", "5"},
		}

		if !reflect.DeepEqual(matrix, expMatrix) {
			t.Fatalf("unexpected diff(matrix, expMatrix):\n%s", pretty.Diff(matrix, expMatrix))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Last, verify that the operator can't shoot themselves in the foot by
	// accidentally decommissioning all nodes.
	//
	// Specify wait=none because the command would block forever (the replicas have
	// nowhere to go).
	t.l.Printf("verifying it's not possible to decommission into unavailability\n")
	if _, err := commission(ctx, 6, c.All(), "decommission", "--wait=none"); err != nil {
		t.Fatalf("decommission failed: %v", err)
	}

	// Check for the in-progress decommissioning status nodes 6-9.
	{
		o, err := execCLI(ctx, t, c, 2, "node", "status", "--format=csv", "--decommission")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}
		exp := matchColumn(statusHeaderCommissionStatusColumn,
			[]string{
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`decommissioning`,
				`decommissioning`,
				`decommissioning`,
				`decommissioning`,
				`commissioned`,
			},
			10, numCols)
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}

	// Check that we can still do stuff. Creating a database should be good enough.
	db := c.Conn(ctx, 2)
	defer db.Close()

	if _, err := db.Exec(`CREATE DATABASE still_working;`); err != nil {
		t.Fatal(err)
	}

	// Recommission all nodes.
	t.l.Printf("verifying cancellation of cluster wide decommissioning process\n")
	if _, err := commission(ctx, 6, c.Range(6, 9), "recommission"); err != nil {
		t.Fatalf("recommission failed: %v", err)
	}

	// To verify that all nodes are actually accepting replicas again, decommission
	// the sixth node (blocking until it's done). This proves that the other nodes
	// absorb its replicas.
	if _, err := commission(ctx, 2, c.Node(6), "decommission"); err != nil {
		t.Fatalf("decommission failed: %v", err)
	}

	// Check final commissioning status for the cluster.
	{
		o, err := execCLI(ctx, t, c, 2, "node", "status", "--format=csv", "--decommission")
		if err != nil {
			t.Fatalf("node-status failed: %v", err)
		}
		numCols, err := getCsvNumCols(o)
		if err != nil {
			t.Fatal(err)
		}
		exp := matchColumn(statusHeaderCommissionStatusColumn,
			[]string{
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`decommissioned`,
				`commissioned`,
				`commissioned`,
				`commissioned`,
				`commissioned`,
			},
			10, numCols)

		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}
	}
}
