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
	"math/rand"
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
		numNodes := 6
		r.Add(testSpec{
			Name:       "decommission-recommission",
			Owner:      OwnerKV,
			MinVersion: "v20.2.0",
			Timeout:    10 * time.Minute,
			Cluster:    makeClusterSpec(numNodes),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runDecommissionRecommission(ctx, t, c)
			},
		})
	}
}

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

// runDecommissionRecommission tests a bunch of node
// decommissioning/recommissioning procedures, all the while checking for
// replica movement and appropriate membership status detection behavior. We go
// through partial decommissioning of random nodes, ensuring we're able to undo
// those operations. We then fully decommission nodes, verifying it's an
// irreversible operation.
func runDecommissionRecommission(ctx context.Context, t *test, c *cluster) {
	args := startArgs("--env=COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, cockroach, "./cockroach")
	c.Start(ctx, t, args)

	// We use a few helpers for random node ID access.
	var nodeIDs []int
	for i := 1; i <= c.spec.NodeCount; i++ {
		nodeIDs = append(nodeIDs, i)
	}
	getRandNode := func() int {
		return nodeIDs[rand.Intn(len(nodeIDs))]
	}
	getRandNodeOtherThan := func(ids ...int) int {
		for {
			cur := nodeIDs[rand.Intn(len(nodeIDs))]
			inBlockList := false
			for _, id := range ids {
				if cur == id {
					inBlockList = true
				}
			}
			if inBlockList {
				continue
			}
			return cur
		}
	}

	// We define a few short hands to {d,r}ecommission target nodes, through a
	// given node.
	decommission := func(
		ctx context.Context,
		targetNodes nodeListOption,
		runNode int,
		verbs ...string,
	) (string, error) {
		args := []string{"node", "decommission"}
		args = append(args, verbs...)

		if len(targetNodes) == 1 && targetNodes[0] == runNode {
			args = append(args, "--self")
		} else {
			for _, target := range targetNodes {
				args = append(args, strconv.Itoa(target))
			}
		}
		return execCLI(ctx, t, c, runNode, args...)
	}
	recommission := func(
		ctx context.Context,
		targetNodes nodeListOption,
		runNode int,
		verbs ...string,
	) (string, error) {
		args := []string{"node", "recommission"}
		args = append(args, verbs...)

		if len(targetNodes) == 1 && targetNodes[0] == runNode {
			args = append(args, "--self")
		} else {
			for _, target := range targetNodes {
				args = append(args, strconv.Itoa(target))
			}
		}
		return execCLI(ctx, t, c, runNode, args...)
	}

	// getCsvNumCols returns the number of columns in the given csv string.
	getCsvNumCols := func(csvStr string) (cols int) {
		reader := csv.NewReader(strings.NewReader(csvStr))
		records, err := reader.Read()
		if err != nil {
			t.Fatal(errors.Errorf("error reading csv input: \n %v\n errors:%s", csvStr, err))
		}
		return len(records)
	}

	// matchCsv matches a multi-line csv string with the provided regex
	// (matchColRow[i][j] will be matched against the i-th line, j-th column).
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

	// Header from the output of `cockroach node decommission`.
	decommissionHeader := []string{
		"id", "is_live", "replicas", "is_decommissioning", "membership", "is_draining",
	}
	// Footer from the output of `cockroach node decommission`, after successful
	// decommission.
	decommissionFooter := []string{
		"No more data reported on target nodes. " +
			"Please verify cluster health before removing the nodes.",
	}

	// Header from the output of `cockroach node status`.
	statusHeader := []string{
		"id", "address", "sql_address", "build", "started_at", "updated_at", "locality", "is_available", "is_live",
	}
	// Header from the output of `cockroach node status --decommission`.
	statusHeaderWithDecommission := []string{
		"id", "address", "sql_address", "build", "started_at", "updated_at", "locality", "is_available", "is_live",
		"gossiped_replicas", "is_decommissioning", "membership", "is_draining",
	}
	// Index of `membership` column in statusHeaderWithDecommission
	const statusHeaderMembershipColumnIdx = 11

	// expectIDsInStatusOut constructs a matching regex for output of `cockroach
	// node status`. It matches against the `id` column in the output generated
	// with and without the `--decommission` flag.
	expectIDsInStatusOut := func(ids []string, numCols int) [][]string {
		var res [][]string
		switch numCols {
		case len(statusHeader):
			res = append(res, statusHeader)
		case len(statusHeaderWithDecommission):
			res = append(res, statusHeaderWithDecommission)
		default:
			t.Fatalf(
				"Expected status output numCols to be one of %d or %d, found %d",
				len(statusHeader),
				len(statusHeaderWithDecommission),
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

	// expectColumn constructs a matching regex for a given column (identified
	// by its column index).
	expectColumn := func(column int, columnRegex []string, numRows, numCols int) [][]string {
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
	// expectCell constructs a matching regex for a given cell (identified by
	// its row and column indexes).
	expectCell := func(row, column int, regex string, numRows, numCols int) [][]string {
		var res [][]string
		for r := 0; r < numRows; r++ {
			build := []string{}
			for c := 0; c < numCols; c++ {
				if r == row && c == column {
					build = append(build, regex)
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

	// Phew, now for the actual test..

	// Partially decommission then recommission a random node, from another
	// random node. Run a couple of status checks while doing so.
	{
		targetNode, runNode := getRandNode(), getRandNode()
		t.l.Printf("partially decommissioning n%d from n%d\n", targetNode, runNode)
		o, err := decommission(ctx, c.Node(targetNode), runNode,
			"--wait=none", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(targetNode), "true", `\d+`, "true", "decommissioning", "false"},
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}

		// Check that `node status` reflects an ongoing decommissioning status
		// for the second node.
		{
			runNode = getRandNode()
			t.l.Printf("checking that `node status` (from n%d) shows n%d as decommissioning\n",
				runNode, targetNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols := getCsvNumCols(o)
			exp := expectCell(targetNode-1, /* node IDs are 1-indexed */
				statusHeaderMembershipColumnIdx, `decommissioning`, c.spec.NodeCount, numCols)
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// Recommission the target node, cancel the in-flight decommissioning
		// process.
		{
			runNode = getRandNode()
			t.l.Printf("recommissioning n%d (from n%d)\n", targetNode, runNode)
			if _, err := recommission(ctx, c.Node(targetNode), runNode); err != nil {
				t.Fatalf("recommission failed: %v", err)
			}
		}

		// Check that `node status` now reflects a 'active' status for the
		// target node.
		{
			runNode = getRandNode()
			t.l.Printf("checking that `node status` (from n%d) shows n%d as active\n",
				targetNode, runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols := getCsvNumCols(o)
			exp := expectCell(targetNode-1, /* node IDs are 1-indexed */
				statusHeaderMembershipColumnIdx, `active`, c.spec.NodeCount, numCols)
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Check to see that operators aren't able to decommission into
	// availability. We'll undo the attempted decommissioning event by
	// recommissioning the targeted nodes.
	{
		runNode := getRandNode()
		t.l.Printf("attempting to decommission all nodes from n%d\n", runNode)
		if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
			o, err := decommission(ctx, c.All(), runNode,
				"--wait=none", "--format=csv")
			if err != nil {
				t.Fatalf("decommission failed: %v", err)
			}

			exp := [][]string{decommissionHeader}
			for i := 1; i <= c.spec.NodeCount; i++ {
				rowRegex := []string{strconv.Itoa(i), "true", `\d+`, "true", "decommissioning", "false"}
				exp = append(exp, rowRegex)
			}
			return matchCSV(o, exp)
		}); err != nil {
			t.Fatal(err)
		}

		// Check that `node status` reflects an ongoing decommissioning status for
		// all nodes.
		{
			runNode = getRandNode()
			t.l.Printf("checking that `node status` (from n%d) shows all nodes as decommissioning\n",
				runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols := getCsvNumCols(o)
			var colRegex []string
			for i := 1; i <= c.spec.NodeCount; i++ {
				colRegex = append(colRegex, `decommissioning`)
			}
			exp := expectColumn(statusHeaderMembershipColumnIdx, colRegex, c.spec.NodeCount, numCols)
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// Check that we can still do stuff, creating a database should be good
		// enough.
		{
			t.l.Printf("checking that we're able to create a database\n", runNode)
			db := c.Conn(ctx, runNode)
			defer db.Close()

			if _, err := db.Exec(`create database still_working;`); err != nil {
				t.Fatal(err)
			}
		}

		// Cancel in-flight decommissioning process of target node.
		{
			t.l.Printf("recommissioning all nodes (from n%d)\n", runNode)
			if _, err := recommission(ctx, c.All(), runNode); err != nil {
				t.Fatalf("recommission failed: %v", err)
			}
		}

		// Check that `node status` now reflects an 'active' status for all
		// nodes.
		{
			runNode = getRandNode()
			t.l.Printf("checking that `node status` (from n%d) shows all nodes as active\n",
				runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv", "--decommission")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols := getCsvNumCols(o)
			var colRegex []string
			for i := 1; i <= c.spec.NodeCount; i++ {
				colRegex = append(colRegex, `active`)
			}
			exp := expectColumn(statusHeaderMembershipColumnIdx, colRegex, c.spec.NodeCount, numCols)
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Fully recommission two random nodes, from a random node, randomly choosing
	// between using --wait={all,none}. We pin these two nodes to not re-use
	// them for the block after, as they will have been fully decommissioned and
	// by definition, non-operational.
	decommissionedNodeA := getRandNode()
	decommissionedNodeB := getRandNodeOtherThan(decommissionedNodeA)
	{
		targetNodeA, targetNodeB := decommissionedNodeA, decommissionedNodeB
		if targetNodeB < targetNodeA {
			targetNodeB, targetNodeA = targetNodeA, targetNodeB
		}

		runNode := getRandNode()
		waitStrategy := "all" // Blocking decommission.
		if i := rand.Intn(2); i == 0 {
			waitStrategy = "none" // Polling decommission.
		}

		t.l.Printf("fully decommissioning [n%d,n%d] from n%d, using --wait=%s\n",
			targetNodeA, targetNodeB, runNode, waitStrategy)

		// When using --wait=none, we poll the decommission status.
		maxAttempts := 50
		if waitStrategy == "all" {
			// --wait=all is a one shot attempt at decommissioning, that polls
			// internally.
			maxAttempts = 1
		}

		if err := retry.WithMaxAttempts(ctx, retryOpts, maxAttempts, func() error {
			o, err := decommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode,
				fmt.Sprintf("--wait=%s", waitStrategy), "--format=csv")
			if err != nil {
				t.Fatalf("decommission failed: %v", err)
			}

			exp := [][]string{
				decommissionHeader,
				{strconv.Itoa(targetNodeA), "true", "0", "true", "decommissioned", "false"},
				{strconv.Itoa(targetNodeB), "true", "0", "true", "decommissioned", "false"},
				decommissionFooter,
			}
			return matchCSV(o, exp)
		}); err != nil {
			t.Fatal(err)
		}

		// Check that even though the node is decommissioned, we still see it (since
		// it remains live) in `node ls`.
		{
			runNode = getRandNode()
			t.l.Printf("checking that `node ls` (from n%d) shows all nodes\n", runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "ls", "--format=csv")
			if err != nil {
				t.Fatalf("node-ls failed: %v", err)
			}
			exp := [][]string{{"id"}}
			for i := 1; i <= c.spec.NodeCount; i++ {
				exp = append(exp, []string{strconv.Itoa(i)})
			}
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// Ditto for `node status`.
		{
			runNode = getRandNode()
			t.l.Printf("checking that `node status` (from n%d) shows all nodes\n", runNode)
			o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}

			numCols := getCsvNumCols(o)
			colRegex := []string{}
			for i := 1; i <= c.spec.NodeCount; i++ {
				colRegex = append(colRegex, strconv.Itoa(i))
			}
			exp := expectIDsInStatusOut(colRegex, numCols)
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// We expect this to fail, seeing as how it's attempting to recommission
		// fully decommissioned nodes.
		{
			runNode = getRandNode()
			t.l.Printf("expected to fail: recommissioning [n%d,n%d] (from n%d)\n",
				targetNodeA, targetNodeB, runNode)
			if _, err := recommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode); err == nil {
				t.Fatal("expected recommission to failed")
			}
		}

		// Decommissioning the same nodes again should be a no-op. We do it from
		// a random node.
		{
			runNode = getRandNode()
			t.l.Printf("checking that decommissioning [n%d,n%d] (from n%d) is a no-op\n",
				runNode, targetNodeA, targetNodeB)
			o, err := decommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode,
				"--wait=all", "--format=csv")
			if err != nil {
				t.Fatalf("decommission failed: %v", err)
			}

			exp := [][]string{
				decommissionHeader,
				{strconv.Itoa(targetNodeA), "true", "0", "true", "decommissioned", "false"},
				{strconv.Itoa(targetNodeB), "true", "0", "true", "decommissioned", "false"},
				decommissionFooter,
			}
			if err := matchCSV(o, exp); err != nil {
				t.Fatal(err)
			}
		}

		// We restart the nodes and attempt to recommission (should still fail).
		{
			runNode = getRandNode()
			t.l.Printf("expected to fail: restarting [n%d,n%d] and attempting to recommission through n%d\n",
				targetNodeA, targetNodeB, runNode)
			c.Stop(ctx, c.Nodes(targetNodeA, targetNodeB))
			c.Start(ctx, t, c.Nodes(targetNodeA, targetNodeB), args)

			if _, err := recommission(ctx, c.Nodes(targetNodeA, targetNodeB), runNode); err == nil {
				t.Fatalf("expected recommission to fail")
			}
		}
	}

	// Decommission a downed node (random selected), randomly choosing between
	// bringing the node back to life or leaving it permanently dead.
	//
	// TODO(irfansharif): We could pull merge this "deadness" check into the
	// previous block, when fully decommissioning multiple nodes, to reduce the
	// total number of nodes needed in the cluster.
	{
		restartDownedNode := false
		if i := rand.Intn(2); i == 0 {
			restartDownedNode = true
		}

		if !restartDownedNode {
			// We want to test decommissioning a truly dead node. Make sure we
			// don't waste too much time waiting for the node to be recognized
			// as dead. Note that we don't want to set this number too low or
			// everything will seem dead to the allocator at all times, so
			// nothing will ever happen.
			func() {
				db := c.Conn(ctx, 1)
				defer db.Close()
				const stmt = "SET CLUSTER SETTING server.time_until_store_dead = '1m15s'"
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					t.Fatal(err)
				}
			}()
		}

		targetNode := getRandNodeOtherThan(decommissionedNodeA, decommissionedNodeB)
		t.l.Printf("intentionally killing n%d to later decommission it when down\n", targetNode)
		c.Stop(ctx, c.Node(targetNode))

		runNode := getRandNodeOtherThan(targetNode)
		t.l.Printf("decommissioning n%d (from n%d) in absentia\n", targetNode, runNode)
		if _, err := decommission(ctx, c.Node(targetNode), runNode,
			"--wait=all", "--format=csv"); err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		if restartDownedNode {
			t.l.Printf("restarting n%d for verification\n", targetNode)

			// Bring targetNode it back up to verify that its replicas still get
			// removed.
			c.Start(ctx, t, c.Node(targetNode), args)
		}

		// Run decommission a second time to wait until the replicas have
		// all been GC'ed. Note that we specify "all" because even though
		// the target node is now running, it may not be live by the time
		// the command runs.
		o, err := decommission(ctx, c.Node(targetNode), runNode,
			"--wait=all", "--format=csv")
		if err != nil {
			t.Fatalf("decommission failed: %v", err)
		}

		exp := [][]string{
			decommissionHeader,
			{strconv.Itoa(targetNode), "true|false", "0", "true", "decommissioned", "false"},
			decommissionFooter,
		}
		if err := matchCSV(o, exp); err != nil {
			t.Fatal(err)
		}

		if !restartDownedNode {
			// Check that (at least after a bit) the node disappears from `node
			// ls` because it is decommissioned and not live.
			if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
				runNode := getRandNodeOtherThan(targetNode)
				o, err := execCLI(ctx, t, c, runNode, "node", "ls", "--format=csv")
				if err != nil {
					t.Fatalf("node-ls failed: %v", err)
				}

				var exp [][]string
				for i := 1; i <= c.spec.NodeCount; i++ {
					exp = append(exp, []string{fmt.Sprintf("[^%d]", targetNode)})
				}

				return matchCSV(o, exp)
			}); err != nil {
				t.Fatal(err)
			}

			// Ditto for `node status`
			if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
				runNode := getRandNodeOtherThan(targetNode)
				o, err := execCLI(ctx, t, c, runNode, "node", "status", "--format=csv")
				if err != nil {
					t.Fatalf("node-status failed: %v", err)
				}

				numCols := getCsvNumCols(o)
				var expC []string
				// We're checking for n-1 rows, where n is the node count.
				for i := 1; i < c.spec.NodeCount; i++ {
					expC = append(expC, fmt.Sprintf("[^%d].*", targetNode))
				}
				exp := expectIDsInStatusOut(expC, numCols)
				return matchCSV(o, exp)
			}); err != nil {
				t.Fatal(err)
			}
		}

		{
			t.l.Printf("wiping n%d and adding it back to the cluster as a new node\n", targetNode)

			c.Stop(ctx, c.Node(targetNode))
			c.Wipe(ctx, c.Node(targetNode))

			joinNode := targetNode%c.spec.NodeCount + 1
			joinAddr := c.InternalAddr(ctx, c.Node(joinNode))[0]
			c.Start(ctx, t, c.Node(targetNode), startArgs(
				fmt.Sprintf("-a=--join %s", joinAddr),
			))
		}

		if err := retry.WithMaxAttempts(ctx, retryOpts, 50, func() error {
			o, err := execCLI(ctx, t, c, getRandNode(), "node", "status", "--format=csv")
			if err != nil {
				t.Fatalf("node-status failed: %v", err)
			}
			numCols := getCsvNumCols(o)
			var expC []string
			for i := 1; i <= c.spec.NodeCount; i++ {
				expC = append(expC, fmt.Sprintf("[^%d].*", targetNode))
			}
			exp := expectIDsInStatusOut(expC, numCols)
			return matchCSV(o, exp)
		}); err != nil {
			t.Fatal(err)
		}
	}

	// We'll verify the set of events, in order, we expect to get posted to
	// system.eventlog.
	if err := retry.ForDuration(time.Minute, func() error {
		// Verify the event log has recorded exactly one decommissioned or
		// recommissioned event for each membership operation.
		db := c.Conn(ctx, 1)
		defer db.Close()

		rows, err := db.Query(`
			SELECT "eventType" FROM system.eventlog WHERE "eventType" IN ($1, $2, $3) ORDER BY timestamp
			`, "node_decommissioned", "node_decommissioning", "node_recommissioned",
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
			// Partial decommission attempt of a single node.
			{"node_decommissioning"},
			{"node_recommissioned"},

			// Cluster wide decommissioning attempt.
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioning"},

			// Cluster wide recommissioning, to undo previous decommissioning attempt.
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},
			{"node_recommissioned"},

			// Full decommission of two nodes.
			{"node_decommissioning"},
			{"node_decommissioning"},
			{"node_decommissioned"},
			{"node_decommissioned"},

			// Full decommission of a single node.
			{"node_decommissioning"},
			{"node_decommissioned"},
		}

		if !reflect.DeepEqual(matrix, expMatrix) {
			t.Fatalf("unexpected diff(matrix, expMatrix):\n%s\n%s\nvs.\n%s", pretty.Diff(matrix, expMatrix), matrix, expMatrix)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
