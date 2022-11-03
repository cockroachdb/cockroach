package server

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

type ServerID int32

type ServerIterator interface {
	dialNode(
		ctx context.Context, serverID ServerID,
	) (*grpc.ClientConn, error)
	getAllNodes(
		ctx context.Context,
	) (map[ServerID]livenesspb.NodeLivenessStatus, error)
	parseServerID(serverIDParam string) (ServerID, bool, error)
	getID() ServerID
}

type tenantFanoutClient struct {
	sqlServer *SQLServer
	rpcCtx    *rpc.Context
	stopper   *stop.Stopper
}

func (t *tenantFanoutClient) getID() ServerID {
	return ServerID(t.sqlServer.SQLInstanceID())
}

var _ ServerIterator = &tenantFanoutClient{}

func (t *tenantFanoutClient) dialNode(
	ctx context.Context, serverID ServerID,
) (*grpc.ClientConn, error) {
	id := base.SQLInstanceID(serverID)
	instance, err := t.sqlServer.sqlInstanceReader.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	return t.rpcCtx.GRPCDialPod(instance.InstanceAddr, id, rpc.DefaultClass).Connect(ctx)
}

func (t *tenantFanoutClient) getAllNodes(
	ctx context.Context,
) (map[ServerID]livenesspb.NodeLivenessStatus, error) {
	liveTenantInstances, err := t.sqlServer.sqlInstanceReader.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	statuses := make(map[ServerID]livenesspb.NodeLivenessStatus, len(liveTenantInstances))
	for _, i := range liveTenantInstances {
		statuses[ServerID(i.InstanceID)] = livenesspb.NodeLivenessStatus_LIVE
	}
	return statuses, nil
}

func (t *tenantFanoutClient) parseServerID(serverIDParam string) (ServerID, bool, error) {
	// No parameter provided or set to local.
	if len(serverIDParam) == 0 || localRE.MatchString(serverIDParam) {
		return ServerID(t.sqlServer.SQLInstanceID()), true /* isLocal */, nil /* err */
	}

	id, err := strconv.ParseInt(serverIDParam, 0, 32)
	if err != nil {
		return 0 /* instanceID */, false /* isLocal */, errors.Wrap(err, "instance ID could not be parsed")
	}
	instanceID := base.SQLInstanceID(id)
	return ServerID(instanceID), instanceID == t.sqlServer.SQLInstanceID() /* isLocal */, nil
}

type kvFanoutClient struct {
	gossip       *gossip.Gossip
	rpcCtx       *rpc.Context
	db           *kv.DB
	nodeLiveness *liveness.NodeLiveness
	admin        *adminServer
	st           *cluster.Settings
	ambientCtx   log.AmbientContext
}

func (k kvFanoutClient) getID() ServerID {
	return ServerID(k.gossip.NodeID.Get())
}

func (k kvFanoutClient) dialNode(ctx context.Context, serverID ServerID) (*grpc.ClientConn, error) {
	id := roachpb.NodeID(serverID)
	addr, err := k.gossip.GetNodeIDAddress(id)
	if err != nil {
		return nil, err
	}
	return k.rpcCtx.GRPCDialNode(addr.String(), id, rpc.DefaultClass).Connect(ctx)
}

func (k kvFanoutClient) listNodes(ctx context.Context) (*serverpb.NodesResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = k.ambientCtx.AnnotateCtx(ctx)

	statuses, _, err := getNodeStatuses(ctx, k.db, 0, 0)
	if err != nil {
		return nil, err
	}
	resp := serverpb.NodesResponse{
		Nodes: statuses,
	}

	clock := k.admin.server.clock
	resp.LivenessByNodeID, err = getLivenessStatusMap(ctx, k.nodeLiveness, clock.Now().GoTime(), k.st)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (k kvFanoutClient) getAllNodes(
	ctx context.Context,
) (map[ServerID]livenesspb.NodeLivenessStatus, error) {
	nodes, err := k.listNodes(ctx)
	if err != nil {
		return nil, err
	}
	ret := make(map[ServerID]livenesspb.NodeLivenessStatus)
	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		livenessStatus := nodes.LivenessByNodeID[nodeID]
		if livenessStatus == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
			// Skip over removed nodes.
			continue
		}
		ret[ServerID(nodeID)] = livenessStatus
	}
	return ret, nil
}

func (k kvFanoutClient) parseServerID(serverIDParam string) (ServerID, bool, error) {
	n, b, err := parseNodeID(k.gossip, serverIDParam)
	return ServerID(n), b, err
}

func parseNodeID(
	gossip *gossip.Gossip, nodeIDParam string,
) (nodeID roachpb.NodeID, isLocal bool, err error) {
	// No parameter provided or set to local.
	if len(nodeIDParam) == 0 || localRE.MatchString(nodeIDParam) {
		return gossip.NodeID.Get(), true, nil
	}

	id, err := strconv.ParseInt(nodeIDParam, 0, 32)
	if err != nil {
		return 0, false, errors.Wrap(err, "node ID could not be parsed")
	}
	nodeID = roachpb.NodeID(id)
	return nodeID, nodeID == gossip.NodeID.Get(), nil
}

var _ ServerIterator = &kvFanoutClient{}
