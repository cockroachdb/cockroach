// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	localTimeFormat = "2006-01-02 15:04:05.999999-07:00"
)

var lsNodesColumnHeaders = []string{
	"id",
}

var lsNodesCmd = &cobra.Command{
	Use:   "ls",
	Short: "lists the IDs of all nodes in the cluster",
	Long: `
Display the node IDs for all active (that is, running and not decommissioned) members of the cluster.
To retrieve the IDs for inactive members, see 'node status --decommission'.
	`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runLsNodes),
}

func runLsNodes(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach node ls", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	if cliCtx.cmdTimeout != 0 {
		if err := conn.Exec(fmt.Sprintf("SET statement_timeout=%d", cliCtx.cmdTimeout), nil); err != nil {
			return err
		}
	}

	_, rows, err := runQuery(
		conn,
		makeQuery(`SELECT node_id FROM crdb_internal.gossip_liveness
               WHERE membership = 'active' OR split_part(expiration,',',1)::decimal > now()::decimal`),
		false,
	)

	if err != nil {
		return err
	}

	return PrintQueryOutput(os.Stdout, lsNodesColumnHeaders, NewRowSliceIter(rows, "r"))
}

var baseNodeColumnHeaders = []string{
	"id",
	"address",
	"sql_address",
	"build",
	"started_at",
	"updated_at",
	"locality",
	"is_available",
	"is_live",
}

var statusNodesColumnHeadersForRanges = []string{
	"replicas_leaders",
	"replicas_leaseholders",
	"ranges",
	"ranges_unavailable",
	"ranges_underreplicated",
}

var statusNodesColumnHeadersForStats = []string{
	"live_bytes",
	"key_bytes",
	"value_bytes",
	"intent_bytes",
	"system_bytes",
}

var statusNodesColumnHeadersForDecommission = []string{
	"gossiped_replicas",
	"is_decommissioning",
	"membership",
	"is_draining",
}

var statusNodeCmd = &cobra.Command{
	Use:   "status [<node id>]",
	Short: "shows the status of a node or all nodes",
	Long: `
If a node ID is specified, this will show the status for the corresponding node. If no node ID
is specified, this will display the status for all nodes in the cluster.
	`,
	Args: cobra.MaximumNArgs(1),
	RunE: MaybeDecorateGRPCError(runStatusNode),
}

func runStatusNode(cmd *cobra.Command, args []string) error {
	_, rows, err := runStatusNodeInner(nodeCtx.statusShowDecommission || nodeCtx.statusShowAll, args)
	if err != nil {
		return err
	}

	sliceIter := NewRowSliceIter(rows, getStatusNodeAlignment())
	return PrintQueryOutput(os.Stdout, getStatusNodeHeaders(), sliceIter)
}

func runStatusNodeInner(showDecommissioned bool, args []string) ([]string, [][]string, error) {
	joinUsingID := func(queries []string) (query string) {
		for i, q := range queries {
			if i == 0 {
				query = q
				continue
			}
			query = "(" + query + ") LEFT JOIN (" + q + ") USING (id)"
		}
		return
	}

	maybeAddActiveNodesFilter := func(query string) string {
		if !showDecommissioned {
			query += " WHERE membership = 'active' OR split_part(expiration,',',1)::decimal > now()::decimal"
		}
		return query
	}

	baseQuery := maybeAddActiveNodesFilter(
		`SELECT node_id AS id,
            address,
            sql_address,
            build_tag AS build,
            started_at,
			updated_at,
			locality,
            CASE WHEN split_part(expiration,',',1)::decimal > now()::decimal
                 THEN true
                 ELSE false
                 END AS is_available,
			ifnull(is_live, false)
     FROM crdb_internal.gossip_liveness LEFT JOIN crdb_internal.gossip_nodes USING (node_id)`,
	)

	const rangesQuery = `
SELECT node_id AS id,
       sum((metrics->>'replicas.leaders')::DECIMAL)::INT AS replicas_leaders,
       sum((metrics->>'replicas.leaseholders')::DECIMAL)::INT AS replicas_leaseholders,
       sum((metrics->>'replicas')::DECIMAL)::INT AS ranges,
       sum((metrics->>'ranges.unavailable')::DECIMAL)::INT AS ranges_unavailable,
       sum((metrics->>'ranges.underreplicated')::DECIMAL)::INT AS ranges_underreplicated
FROM crdb_internal.kv_store_status
GROUP BY node_id`

	const statsQuery = `
SELECT node_id AS id,
       sum((metrics->>'livebytes')::DECIMAL)::INT AS live_bytes,
       sum((metrics->>'keybytes')::DECIMAL)::INT AS key_bytes,
       sum((metrics->>'valbytes')::DECIMAL)::INT AS value_bytes,
       sum((metrics->>'intentbytes')::DECIMAL)::INT AS intent_bytes,
       sum((metrics->>'sysbytes')::DECIMAL)::INT AS system_bytes
FROM crdb_internal.kv_store_status
GROUP BY node_id`

	// TODO(irfansharif): Remove the `is_decommissioning` column in v20.2.
	const decommissionQuery = `
SELECT node_id AS id,
       ranges AS gossiped_replicas,
       membership != 'active' as is_decommissioning,
       membership AS membership,
       draining AS is_draining
FROM crdb_internal.gossip_liveness LEFT JOIN crdb_internal.gossip_nodes USING (node_id)`

	conn, err := makeSQLClient("cockroach node status", useSystemDb)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	queriesToJoin := []string{baseQuery}

	if nodeCtx.statusShowAll || nodeCtx.statusShowRanges {
		queriesToJoin = append(queriesToJoin, rangesQuery)
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowStats {
		queriesToJoin = append(queriesToJoin, statsQuery)
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowDecommission {
		queriesToJoin = append(queriesToJoin, decommissionQuery)
	}

	if cliCtx.cmdTimeout != 0 {
		if err := conn.Exec(fmt.Sprintf("SET statement_timeout=%d", cliCtx.cmdTimeout), nil); err != nil {
			return nil, nil, err
		}
	}

	queryString := "SELECT * FROM (" + joinUsingID(queriesToJoin) + ")"

	switch len(args) {
	case 0:
		query := makeQuery(queryString + " ORDER BY id")
		return runQuery(conn, query, false)
	case 1:
		nodeID, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, nil, errors.Errorf("could not parse node_id %s", args[0])
		}
		query := makeQuery(queryString+" WHERE id = $1", nodeID)
		headers, rows, err := runQuery(conn, query, false)
		if err != nil {
			return nil, nil, err
		}
		if len(rows) == 0 {
			return nil, nil, errors.Errorf("node %d doesn't exist", nodeID)
		}
		return headers, rows, nil
	default:
		return nil, nil, errors.Errorf("expected no arguments or a single node ID")
	}
}

func getStatusNodeHeaders() []string {
	headers := baseNodeColumnHeaders

	if nodeCtx.statusShowAll || nodeCtx.statusShowRanges {
		headers = append(headers, statusNodesColumnHeadersForRanges...)
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowStats {
		headers = append(headers, statusNodesColumnHeadersForStats...)
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowDecommission {
		headers = append(headers, statusNodesColumnHeadersForDecommission...)
	}
	return headers
}

func getStatusNodeAlignment() string {
	align := "rlllll"
	if nodeCtx.statusShowAll || nodeCtx.statusShowRanges {
		align += "rrrrrr"
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowStats {
		align += "rrrrrr"
	}
	if nodeCtx.statusShowAll || nodeCtx.statusShowDecommission {
		align += decommissionResponseAlignment()
	}
	return align
}

var decommissionNodesColumnHeaders = []string{
	"id",
	"is_live",
	"replicas",
	"is_decommissioning",
	"membership",
	"is_draining",
}

var decommissionNodeCmd = &cobra.Command{
	Use:   "decommission { --self | <node id 1> [<node id 2> ...] }",
	Short: "decommissions the node(s)",
	Long: `
Marks the nodes with the supplied IDs as decommissioning.
This will cause leases and replicas to be removed from these nodes.`,
	Args: cobra.MinimumNArgs(0),
	RunE: MaybeDecorateGRPCError(runDecommissionNode),
}

func parseNodeIDs(strNodeIDs []string) ([]roachpb.NodeID, error) {
	nodeIDs := make([]roachpb.NodeID, 0, len(strNodeIDs))
	for _, str := range strNodeIDs {
		i, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, errors.Errorf("unable to parse %s: %s", str, err)
		}
		nodeIDs = append(nodeIDs, roachpb.NodeID(i))
	}
	return nodeIDs, nil
}

func runDecommissionNode(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !nodeCtx.nodeDecommissionSelf && len(args) == 0 {
		return errors.New("no node ID specified; use --self to target the node specified with --host")
	}

	nodeIDs, err := parseNodeIDs(args)
	if err != nil {
		return err
	}

	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()

	s := serverpb.NewStatusClient(conn)

	nodeIDs, err = handleNodeDecommissionSelf(ctx, s, nodeIDs, "decommissioning")
	if err != nil {
		return err
	}

	if err := expectNodesDecommissioned(ctx, s, nodeIDs, false /* expDecommissioned */); err != nil {
		return err
	}

	c := serverpb.NewAdminClient(conn)
	if err := runDecommissionNodeImpl(ctx, c, nodeCtx.nodeDecommissionWait, nodeIDs); err != nil {
		cause := errors.UnwrapAll(err)
		if s, ok := status.FromError(cause); ok && s.Code() == codes.NotFound {
			// Are we trying to decommision a node that does not
			// exist? See Server.Decommission for where this specific grpc error
			// code is generated.
			return errors.New("node does not exist")
		}
		return err
	}
	return nil
}

func handleNodeDecommissionSelf(
	ctx context.Context, s serverpb.StatusClient, nodeIDs []roachpb.NodeID, command string,
) ([]roachpb.NodeID, error) {
	if !nodeCtx.nodeDecommissionSelf {
		// --self not passed; nothing to do.
		return nodeIDs, nil
	}

	if len(nodeIDs) > 0 {
		return nil, errors.Newf("cannot use --%s with an explicit list of node IDs",
			cliflags.NodeDecommissionSelf.Name)
	}

	// What's this node's ID?
	resp, err := s.Node(ctx, &serverpb.NodeRequest{NodeId: "local"})
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "%s node %d", log.Safe(command), resp.Desc.NodeID)
	return []roachpb.NodeID{resp.Desc.NodeID}, nil
}

func expectNodesDecommissioned(
	ctx context.Context, s serverpb.StatusClient, nodeIDs []roachpb.NodeID, expDecommissioned bool,
) error {
	resp, err := s.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		liveness, ok := resp.LivenessByNodeID[nodeID]
		if !ok {
			fmt.Fprintln(stderr, "warning: cannot find status of node", nodeID)
			continue
		}
		if !expDecommissioned {
			// The user is expecting the node to not be
			// decommissioned/decommissioning already.
			switch liveness {
			case livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				livenesspb.NodeLivenessStatus_DECOMMISSIONED:
				fmt.Fprintln(stderr, "warning: node", nodeID, "is already decommissioning or decommissioned")
			default:
				// It's always possible to decommission a node that's either live
				// or dead.
			}
		} else {
			// The user is expecting the node to be recommissionable.
			switch liveness {
			case livenesspb.NodeLivenessStatus_DECOMMISSIONING,
				livenesspb.NodeLivenessStatus_DECOMMISSIONED:
				// ok.
			case livenesspb.NodeLivenessStatus_LIVE:
				fmt.Fprintln(stderr, "warning: node", nodeID, "is not decommissioned")
			default: // dead, unavailable, etc
				fmt.Fprintln(stderr, "warning: node", nodeID, "is in unexpected state", liveness)
			}
		}
	}
	return nil
}

func runDecommissionNodeImpl(
	ctx context.Context,
	c serverpb.AdminClient,
	wait nodeDecommissionWaitType,
	nodeIDs []roachpb.NodeID,
) error {
	minReplicaCount := int64(math.MaxInt64)
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     20 * time.Second,
	}

	// Marking a node as fully decommissioned is driven by a two-step process.
	// We start off by marking each node as 'decommissioning'. In doing so,
	// replicas are slowly moved off of these nodes. It's only after when we're
	// made aware that the replica counts have all hit zero, and that all nodes
	// have been successfully marked as 'decommissioning', that we then go and
	// mark each node as 'decommissioned'.
	prevResponse := serverpb.DecommissionStatusResponse{}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		req := &serverpb.DecommissionRequest{
			NodeIDs:          nodeIDs,
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
		}
		resp, err := c.Decommission(ctx, req)
		if err != nil {
			fmt.Fprintln(stderr)
			return errors.Wrap(err, "while trying to mark as decommissioning")
		}

		if !reflect.DeepEqual(&prevResponse, resp) {
			fmt.Fprintln(stderr)
			if err := printDecommissionStatus(*resp); err != nil {
				return err
			}
			prevResponse = *resp
		} else {
			fmt.Fprintf(stderr, ".")
		}

		anyActive := false
		var replicaCount int64
		for _, status := range resp.Status {
			anyActive = anyActive || status.Membership.Active()
			replicaCount += status.ReplicaCount
		}

		if !anyActive && replicaCount == 0 {
			// We now mark the nodes as fully decommissioned.
			req := &serverpb.DecommissionRequest{
				NodeIDs:          nodeIDs,
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
			}
			_, err = c.Decommission(ctx, req)
			if err != nil {
				fmt.Fprintln(stderr)
				return errors.Wrap(err, "while trying to mark as decommissioned")
			}

			fmt.Fprintln(os.Stdout, "\nNo more data reported on target nodes. "+
				"Please verify cluster health before removing the nodes.")
			return nil
		}

		if wait == nodeDecommissionWaitNone {
			// The intent behind --wait=none is for it to be used when polling
			// manually from an external system. We'll only mark nodes as
			// fully decommissioned once the replica count hits zero and they're
			// all marked as decommissioning.
			return nil
		}
		if replicaCount < minReplicaCount {
			minReplicaCount = replicaCount
			r.Reset()
		}
	}
	return errors.New("maximum number of retries exceeded")
}

func decommissionResponseAlignment() string {
	return "rcrccc"
}

// decommissionResponseValueToRows converts DecommissionStatusResponse_Status to
// SQL-like result rows, so that we can pretty-print them.
func decommissionResponseValueToRows(
	statuses []serverpb.DecommissionStatusResponse_Status,
) [][]string {
	// Create results that are like the results for SQL results, so that we can pretty-print them.
	var rows [][]string
	for _, node := range statuses {
		rows = append(rows, []string{
			strconv.FormatInt(int64(node.NodeID), 10),
			strconv.FormatBool(node.IsLive),
			strconv.FormatInt(node.ReplicaCount, 10),
			strconv.FormatBool(!node.Membership.Active()),
			node.Membership.String(),
			strconv.FormatBool(node.Draining),
		})
	}
	return rows
}

var recommissionNodeCmd = &cobra.Command{
	Use:   "recommission { --self | <node id 1> [<node id 2> ...] }",
	Short: "recommissions the node(s)",
	Long: `
For the nodes with the supplied IDs, resets the decommissioning states,
signaling the affected nodes to participate in the cluster again.
	`,
	Args: cobra.MinimumNArgs(0),
	RunE: MaybeDecorateGRPCError(runRecommissionNode),
}

func printDecommissionStatus(resp serverpb.DecommissionStatusResponse) error {
	return PrintQueryOutput(os.Stdout, decommissionNodesColumnHeaders,
		NewRowSliceIter(decommissionResponseValueToRows(resp.Status), decommissionResponseAlignment()))
}

func runRecommissionNode(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !nodeCtx.nodeDecommissionSelf && len(args) == 0 {
		return errors.New("no node ID specified; use --self to target the node specified with --host")
	}

	nodeIDs, err := parseNodeIDs(args)
	if err != nil {
		return err
	}

	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()

	s := serverpb.NewStatusClient(conn)

	nodeIDs, err = handleNodeDecommissionSelf(ctx, s, nodeIDs, "recommissioning")
	if err != nil {
		return err
	}

	if err := expectNodesDecommissioned(ctx, s, nodeIDs, true /* expDecommissioned */); err != nil {
		return err
	}

	c := serverpb.NewAdminClient(conn)
	req := &serverpb.DecommissionRequest{
		NodeIDs:          nodeIDs,
		TargetMembership: livenesspb.MembershipStatus_ACTIVE,
	}
	resp, err := c.Decommission(ctx, req)
	if err != nil {
		cause := errors.UnwrapAll(err)
		// If it's a specific illegal membership transition error, we try to
		// surface a more readable message to the user. See ValidateTransition
		// in pkg/liveness/livenesspb for where this error is generated.
		if s, ok := status.FromError(cause); ok && s.Code() == codes.FailedPrecondition {
			return errors.Newf("%s", s.Message())
		}
		if s, ok := status.FromError(cause); ok && s.Code() == codes.NotFound {
			// Are we trying to recommission node that does not
			// exist? See Server.Decommission for where this specific grpc error
			// code is generated.
			fmt.Fprintln(stderr)
			return errors.New("node does not exist")
		}
		return err
	}
	return printDecommissionStatus(*resp)
}

var drainNodeCmd = &cobra.Command{
	Use:   "drain",
	Short: "drain a node without shutting it down",
	Long: `
Prepare a server so it becomes ready to be shut down safely.
This causes the server to stop accepting client connections, stop
extant connections, and finally push range leases onto other
nodes, subject to various timeout parameters configurable via
cluster settings.

After a successful drain, the server process is still running;
use a service manager or orchestrator to terminate the process
gracefully using e.g. a unix signal.`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runDrain),
}

// runDrain calls the Drain RPC without the flag to stop the
// server process.
func runDrain(cmd *cobra.Command, args []string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// At the end, we'll report "ok" if there was no error.
	defer func() {
		if err == nil {
			fmt.Println("ok")
		}
	}()

	// Establish a RPC connection.
	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	_, _, err = doDrain(ctx, c)
	return err
}

// Sub-commands for node command.
var nodeCmds = []*cobra.Command{
	lsNodesCmd,
	statusNodeCmd,
	decommissionNodeCmd,
	recommissionNodeCmd,
	drainNodeCmd,
}

var nodeCmd = &cobra.Command{
	Use:   "node [command]",
	Short: "list, inspect, drain or remove nodes\n",
	Long:  "List, inspect, drain or remove nodes.",
	RunE:  usageAndErr,
}

func init() {
	nodeCmd.AddCommand(nodeCmds...)
}
