// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	RunE: clierrorplus.MaybeDecorateError(runLsNodes),
}

func runLsNodes(cmd *cobra.Command, args []string) (resErr error) {
	ctx := context.Background()
	// TODO(ssd): We can potentially make this work against
	// secondary tenants using sql_instances.
	conn, err := makeTenantSQLClient(ctx, "cockroach node ls", useSystemDb, catconstants.SystemTenantName)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	// TODO(knz): This can use a context deadline instead, now that
	// query cancellation is supported.
	if cliCtx.cmdTimeout != 0 {
		if err := conn.Exec(ctx,
			"SET statement_timeout = $1", cliCtx.cmdTimeout.String()); err != nil {
			return err
		}
	}

	_, rows, err := sqlExecCtx.RunQuery(
		ctx,
		conn,
		clisqlclient.MakeQuery(`SELECT node_id FROM crdb_internal.gossip_liveness
               WHERE membership = 'active' OR split_part(expiration,',',1)::decimal > now()::decimal`),
		false, /* showMoreChars */
	)

	if err != nil {
		return err
	}

	return sqlExecCtx.PrintQueryOutput(os.Stdout, stderr, lsNodesColumnHeaders,
		clisqlexec.NewRowSliceIter(rows, "r"))
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
	"range_key_bytes",
	"range_value_bytes",
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
	RunE: clierrorplus.MaybeDecorateError(runStatusNode),
}

func runStatusNode(cmd *cobra.Command, args []string) error {
	_, rows, err := runStatusNodeInner(nodeCtx.statusShowDecommission || nodeCtx.statusShowAll, args)
	if err != nil {
		return err
	}

	sliceIter := clisqlexec.NewRowSliceIter(rows, getStatusNodeAlignment())
	return sqlExecCtx.PrintQueryOutput(os.Stdout, stderr, getStatusNodeHeaders(), sliceIter)
}

func runStatusNodeInner(
	showDecommissioned bool, args []string,
) (colNames []string, rowVals [][]string, resErr error) {
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
       sum(coalesce((metrics->>'rangekeybytes')::DECIMAL, 0))::INT AS range_key_bytes,
       sum(coalesce((metrics->>'rangevalbytes')::DECIMAL, 0))::INT AS range_value_bytes,
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

	ctx := context.Background()
	conn, err := makeTenantSQLClient(ctx, "cockroach node status", useSystemDb, catconstants.SystemTenantName)
	if err != nil {
		return nil, nil, err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

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

	if err = conn.EnsureConn(ctx); err != nil {
		return nil, nil, err
	}

	// TODO(knz): This can use a context deadline instead, now that
	// query cancellation is supported.
	if cliCtx.cmdTimeout != 0 {
		if err := conn.Exec(ctx,
			"SET statement_timeout = $1", cliCtx.cmdTimeout.String()); err != nil {
			return nil, nil, err
		}
	}

	queryString := "SELECT * FROM (" + joinUsingID(queriesToJoin) + ")"

	switch len(args) {
	case 0:
		query := clisqlclient.MakeQuery(queryString + " ORDER BY id")
		return sqlExecCtx.RunQuery(ctx, conn, query, false /* showMoreChars */)
	case 1:
		nodeID, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, nil, errors.Errorf("could not parse node_id %s", args[0])
		}
		query := clisqlclient.MakeQuery(queryString+" WHERE id = $1", nodeID)
		headers, rows, err := sqlExecCtx.RunQuery(ctx, conn, query, false /* showMoreChars */)
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
		align += decommissionStatusAlignment()
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

var decommissionNodesCheckAddlColumnHeaders = []string{
	"readiness",
	"blocking_ranges",
}

var decommissionNodeCmd = &cobra.Command{
	Use:   "decommission { --self | <node id 1> [<node id 2> ...] }",
	Short: "decommissions the node(s)",
	Long: `
Marks the nodes with the supplied IDs as decommissioning.
This will cause leases and replicas to be removed from these nodes.`,
	Args: cobra.MinimumNArgs(0),
	RunE: clierrorplus.MaybeDecorateError(runDecommissionNode),
}

func parseNodeIDs(strNodeIDs []string) ([]roachpb.NodeID, error) {
	nodeIDs := make([]roachpb.NodeID, 0, len(strNodeIDs))
	for _, str := range strNodeIDs {
		i, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse %s", str)
		}
		nodeIDs = append(nodeIDs, roachpb.NodeID(i))
	}
	return nodeIDs, nil
}

func runDecommissionNode(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if nodeCtx.nodeDecommissionSelf {
		log.Warningf(ctx, "--%s for decommission is deprecated.", cliflags.NodeDecommissionSelf.Name)
	}

	if !nodeCtx.nodeDecommissionSelf && len(args) == 0 {
		return errors.New("no node ID specified")
	}

	nodeIDs, err := parseNodeIDs(args)
	if err != nil {
		return err
	}

	conn, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()

	s := serverpb.NewStatusClient(conn)

	localNodeID, err := getLocalNodeID(ctx, s)
	if err != nil {
		return err
	}

	nodeIDs, err = handleNodeDecommissionSelf(ctx, nodeIDs, localNodeID, "decommissioning")
	if err != nil {
		return err
	}

	if err := expectNodesDecommissioned(ctx, s, nodeIDs, false /* expDecommissioned */); err != nil {
		return err
	}

	c := serverpb.NewAdminClient(conn)
	if err := runDecommissionNodeImpl(ctx, c, nodeCtx.nodeDecommissionWait,
		nodeCtx.nodeDecommissionChecks, nodeCtx.nodeDecommissionDryRun,
		nodeIDs, localNodeID,
	); err != nil {
		cause := errors.UnwrapAll(err)
		if s, ok := status.FromError(cause); ok && s.Code() == codes.NotFound {
			// Are we trying to decommission a node that does not
			// exist? See Server.Decommission for where this specific grpc error
			// code is generated.
			return errors.New("node does not exist")
		}
		return err
	}
	return nil
}

func getLocalNodeID(ctx context.Context, s serverpb.StatusClient) (roachpb.NodeID, error) {
	var nodeID roachpb.NodeID
	resp, err := s.Node(ctx, &serverpb.NodeRequest{NodeId: "local"})
	if err != nil {
		return nodeID, err
	}
	nodeID = resp.Desc.NodeID
	return nodeID, nil
}

func handleNodeDecommissionSelf(
	ctx context.Context, nodeIDs []roachpb.NodeID, localNodeID roachpb.NodeID, command string,
) ([]roachpb.NodeID, error) {
	if !nodeCtx.nodeDecommissionSelf {
		// --self not passed; nothing to do.
		return nodeIDs, nil
	}

	if len(nodeIDs) > 0 {
		return nil, errors.Newf("cannot use --%s with an explicit list of node IDs",
			cliflags.NodeDecommissionSelf.Name)
	}

	log.Infof(ctx, "%s node %d", redact.Safe(command), localNodeID)
	return []roachpb.NodeID{localNodeID}, nil
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
	checks nodeDecommissionCheckMode,
	dryRun bool,
	nodeIDs []roachpb.NodeID,
	localNodeID roachpb.NodeID,
) error {
	minReplicaCount := int64(math.MaxInt64)
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     20 * time.Second,
	}

	// Log verbosity is increased when there is possibly a decommission stall.
	// If the number of decommissioning replicas does not decrease after some time
	// (i.e. the decommission status has not changed after
	// sameStatusThreshold iterations), verbosity is automatically set.
	// Some decommissioning replicas will be reported to the operator.
	const sameStatusThreshold = 15
	const preCheckBlockingRangeErrsToReport = 5
	var (
		numReplicaReport = 0
		sameStatusCount  = 0
	)

	// Decommissioning a node is driven by a four-step process.
	// 1) Evaluate decommission pre-check status. If node(s) are unready for
	// decommission, report prior to initializing decommission and exit.
	// 2) Mark each node as 'decommissioning'. In doing so, all replicas are
	// slowly moved off of these nodes.
	// 3) Drain each node.
	// 4) Mark each node as 'decommissioned'.
	// Note: if the node serving the decommission request is a target node,
	// the draining step for that node will be skipped. This is because
	// after a drain, issuing a decommission RPC against this node will fail.
	// TODO(cameron): update the note once decommission requests are
	// routed to another selected "control" node in the cluster.
	var preCheckReq *serverpb.DecommissionPreCheckRequest
	var preCheckResp *serverpb.DecommissionPreCheckResponse
	if checks > nodeDecommissionChecksSkip {
		var err error
		preCheckReq = &serverpb.DecommissionPreCheckRequest{
			NodeIDs:         nodeIDs,
			StrictReadiness: checks == nodeDecommissionChecksStrict,
		}
		preCheckResp, err = c.DecommissionPreCheck(ctx, preCheckReq)
		if err != nil {
			fmt.Fprintln(stderr)
			return errors.Wrap(err, "while trying to check decommission readiness")
		}
	}

	// On a dry run, we simply run checks (as above), and print the decommission
	// status.
	if dryRun || !decommissionPreCheckReady(preCheckResp) {
		req := &serverpb.DecommissionStatusRequest{
			NodeIDs: nodeIDs,
		}
		resp, err := c.DecommissionStatus(ctx, req)
		if err != nil {
			fmt.Fprintln(stderr)
			return errors.Wrap(err, "while trying to check decommission status")
		}
		fmt.Fprintln(stderr)

		err = printDecommissionStatusAndReadiness(*resp, preCheckResp)
		if err == nil && !decommissionPreCheckReady(preCheckResp) {
			printDecommissionBlockingErrorSummary(preCheckResp, preCheckBlockingRangeErrsToReport)
			fmt.Fprintln(stderr)
			err = errors.New("Cannot decommission nodes.")
		}
		return err
	}

	prevResponse := serverpb.DecommissionStatusResponse{}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		req := &serverpb.DecommissionRequest{
			NodeIDs:          nodeIDs,
			TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
			NumReplicaReport: int32(numReplicaReport),
		}
		resp, err := c.Decommission(ctx, req)
		if err != nil {
			fmt.Fprintln(stderr)
			return errors.Wrap(err, "while trying to mark as decommissioning")
		}

		if numReplicaReport > 0 {
			printDecommissionReplicas(*resp)
		}

		if !reflect.DeepEqual(&prevResponse, resp) {
			fmt.Fprintln(stderr)
			if err = printDecommissionStatusAndReadiness(*resp, preCheckResp); err != nil {
				return err
			}
			prevResponse = *resp

			// The decommissioning status changed. Set `sameStatusCount` back to zero.
			sameStatusCount = 0
			numReplicaReport = 0
		} else {
			// Print a marker indicating that there has been no progress,
			// instead of printing the same status.
			fmt.Fprintf(stderr, ".")

			// Report decommissioning replicas if there's been significant time of
			// no progress.
			if sameStatusCount >= sameStatusThreshold && numReplicaReport == 0 {
				// Configure a number of replicas to report.
				numReplicaReport = 5
				// Refresh pre-check results, if applicable.
				if preCheckReq != nil {
					if newPreCheckResp, err := c.DecommissionPreCheck(ctx, preCheckReq); err != nil {
						fmt.Fprintln(stderr)
						return errors.Wrap(err, "while trying to check decommission readiness")
					} else {
						preCheckResp = newPreCheckResp
					}
				}
			} else {
				sameStatusCount++
			}
		}

		anyActive := false
		var replicaCount int64
		statusByNodeID := map[roachpb.NodeID]serverpb.DecommissionStatusResponse_Status{}
		for _, status := range resp.Status {
			anyActive = anyActive || status.Membership.Active()
			replicaCount += status.ReplicaCount
			statusByNodeID[status.NodeID] = status
		}

		if !anyActive && replicaCount == 0 {
			// We now drain the nodes in order to close all SQL connections.
			// Note: iteration is not necessary here since there are no remaining leases
			// on the decommissioning node after replica transferral.
			for _, targetNode := range nodeIDs {
				if targetNode == localNodeID {
					// Skip the draining step for the node serving the request, if it is a target node.
					_, _ = fmt.Fprintf(stderr,
						"skipping drain step for node n%d; it is decommissioning and serving the request\n",
						localNodeID,
					)
					continue
				}
				if status, ok := statusByNodeID[targetNode]; !ok || !status.IsLive {
					// Skip the draining step for the node serving the request, if it is a target node.
					_, _ = fmt.Fprintf(stderr,
						"skipping drain step for node n%d; it is not live\n", targetNode,
					)
					continue
				}
				_, _ = fmt.Fprintf(stderr, "draining node n%d\n", targetNode)

				if _, _, err := doDrain(ctx, c, targetNode.String()); err != nil {
					// NB: doDrain already prints to stdErr.
					//
					// Defense in depth: in decommission invocations that don't have to
					// do much work, if the target node was _just_ shutdown prior to
					// starting `node decommission`, the node may be absent but the liveness
					// status sent us here anyway. We don't want to fail out on the drain
					// step to make the decommissioning command more robust.
					_, _ = fmt.Fprintf(stderr,
						"drain step for node n%d failed; decommissioning anyway\n", targetNode,
					)
					_ = err // discard intentionally
				} else {
					// NB: this output is matched on in the decommission/drains roachtest.
					_, _ = fmt.Fprintf(stderr, "node n%d drained successfully\n", targetNode)
				}
			}

			// Finally, mark the nodes as fully decommissioned.
			decommissionReq := &serverpb.DecommissionRequest{
				NodeIDs:          nodeIDs,
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
			}
			_, err = c.Decommission(ctx, decommissionReq)
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

func decommissionStatusAlignment() string {
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

func decommissionReadinessAlignment() string {
	return "cr"
}

func decommissionStatusAndReadinessAlignment() string {
	return decommissionStatusAlignment() + decommissionReadinessAlignment()
}

// decommissionStatusAndReadinessValueToRows converts a
// DecommissionStatusResponse Status as well as a DecommissionPreCheckResponse
// NodeCheckResult to SQL-like result rows, so that we can pretty-print them.
func decommissionStatusAndReadinessValueToRows(
	statuses []serverpb.DecommissionStatusResponse_Status,
	reportByNodeID map[roachpb.NodeID]serverpb.DecommissionPreCheckResponse_NodeCheckResult,
) [][]string {
	var rows [][]string
	for _, node := range statuses {
		report := reportByNodeID[node.NodeID]

		rows = append(rows, []string{
			strconv.FormatInt(int64(node.NodeID), 10),
			strconv.FormatBool(node.IsLive),
			strconv.FormatInt(node.ReplicaCount, 10),
			strconv.FormatBool(!node.Membership.Active()),
			node.Membership.String(),
			strconv.FormatBool(node.Draining),
			report.DecommissionReadiness.String(),
			strconv.FormatInt(int64(len(report.CheckedRanges)), 10),
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
	RunE: clierrorplus.MaybeDecorateError(runRecommissionNode),
}

func printDecommissionStatusAndReadiness(
	statusResp serverpb.DecommissionStatusResponse, checkResp *serverpb.DecommissionPreCheckResponse,
) error {
	if checkResp == nil {
		return printDecommissionStatus(statusResp)
	}

	reportByNodeID := make(map[roachpb.NodeID]serverpb.DecommissionPreCheckResponse_NodeCheckResult)
	for _, nodeCheckResult := range checkResp.CheckedNodes {
		reportByNodeID[nodeCheckResult.NodeID] = nodeCheckResult
	}

	return sqlExecCtx.PrintQueryOutput(os.Stdout, stderr, append(decommissionNodesColumnHeaders, decommissionNodesCheckAddlColumnHeaders...),
		clisqlexec.NewRowSliceIter(
			decommissionStatusAndReadinessValueToRows(statusResp.Status, reportByNodeID),
			decommissionStatusAndReadinessAlignment(),
		))
}

func printDecommissionStatus(resp serverpb.DecommissionStatusResponse) error {
	return sqlExecCtx.PrintQueryOutput(os.Stdout, stderr, decommissionNodesColumnHeaders,
		clisqlexec.NewRowSliceIter(decommissionResponseValueToRows(resp.Status), decommissionStatusAlignment()))
}

func printDecommissionReplicas(resp serverpb.DecommissionStatusResponse) {
	fmt.Fprintln(stderr, "\npossible decommission stall detected")

	for _, nodeStatus := range resp.Status {
		for _, replica := range nodeStatus.ReportedReplicas {
			fmt.Fprintf(stderr,
				"n%d still has replica id %d for range r%d\n",
				nodeStatus.NodeID,
				replica.ReplicaID,
				replica.RangeID,
			)
		}
	}
}

func printDecommissionBlockingErrorSummary(
	resp *serverpb.DecommissionPreCheckResponse, reportLimit int,
) {
	fmt.Fprintln(stderr, "\nranges blocking decommission detected")
	reported := 0
	for _, nodeCheckResult := range resp.CheckedNodes {
		if nodeCheckResult.DecommissionReadiness != serverpb.DecommissionPreCheckResponse_ALLOCATION_ERRORS {
			continue
		}

		errCountMap := make(map[string]int)
		for _, rangeCheckResult := range nodeCheckResult.CheckedRanges {
			errCountMap[rangeCheckResult.Error] += 1
		}

		for errMsg, count := range errCountMap {
			if reported >= reportLimit {
				fmt.Fprintf(stderr, "...more blocking errors detected.\n")
				return
			}
			fmt.Fprintf(stderr,
				"n%d has %d replicas blocked with error: \"%s\"\n",
				nodeCheckResult.NodeID,
				count,
				errMsg,
			)
			reported++
		}
	}
}

// decommissionPreCheckReady checks if, given a valid response, there are any
// nodes shown to not be ready for decommission.
func decommissionPreCheckReady(resp *serverpb.DecommissionPreCheckResponse) bool {
	// If we don't have a response value, this indicates that the pre-checks are
	// disabled, and hence all nodes are implicitly ready for decommission.
	if resp == nil {
		return true
	}
	for _, nodeCheckResult := range resp.CheckedNodes {
		if !(nodeCheckResult.DecommissionReadiness == serverpb.DecommissionPreCheckResponse_READY ||
			nodeCheckResult.DecommissionReadiness == serverpb.DecommissionPreCheckResponse_ALREADY_DECOMMISSIONED) {
			return false
		}
	}

	return true
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

	conn, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to the node")
	}
	defer finish()

	s := serverpb.NewStatusClient(conn)

	localNodeID, err := getLocalNodeID(ctx, s)
	if err != nil {
		return err
	}

	nodeIDs, err = handleNodeDecommissionSelf(ctx, nodeIDs, localNodeID, "recommissioning")
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
	Use:   "drain { --self | <node id> } [ --shutdown ] [ --drain-wait=timeout ] ",
	Short: "drain a node and optionally shut it down",
	Long: `
Prepare a server so it becomes ready to be shut down safely.
This causes the server to stop accepting client connections, stop
extant connections, and finally push range leases onto other
nodes, subject to various timeout parameters configurable via
cluster settings.

After a successful drain, if the --shutdown flag is not specified,
the server process is still running; use a service manager or
orchestrator to terminate the process gracefully using e.g. a
unix signal.

If an argument is specified, the command affects the node
whose ID is given. If --self is specified, the command
affects the node that the command is connected to (via --host).
`,
	Args: cobra.MaximumNArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runDrain),
}

// runDrain calls the Drain RPC without the flag to stop the
// server process.
func runDrain(cmd *cobra.Command, args []string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !drainCtx.nodeDrainSelf && len(args) == 0 {
		fmt.Fprintf(stderr, "warning: draining a node without node ID or passing --self explicitly is deprecated.\n")
		drainCtx.nodeDrainSelf = true
	}
	if drainCtx.nodeDrainSelf && len(args) > 0 {
		return errors.Newf("cannot use --%s with an explicit node ID", cliflags.NodeDrainSelf.Name)
	}

	targetNode := "local"
	if len(args) > 0 {
		targetNode = args[0]
	}

	// Establish a RPC connection.
	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	if _, _, err := doDrain(ctx, c, targetNode); err != nil {
		return err
	}

	// Report "ok" if there was no error.
	fmt.Println("drain ok")

	if drainCtx.shutdown {
		if _, err := doShutdown(ctx, c, targetNode); err != nil {
			return err
		}
		// Report "ok" if there was no error.
		fmt.Println("shutdown ok")
	}

	return nil
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
	RunE:  UsageAndErr,
}

func init() {
	nodeCmd.AddCommand(nodeCmds...)
}
