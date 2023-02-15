// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// serverID is a type that is either a `roachpb.NodeID`
// or `base.SQLInstanceID` depending on the context.
// Different implementations of `ServerIterator` interpret
// values of `serverID` differently depending on context.
type serverID int32

// ServerIterator is an interface that provides an abstraction
// around fanning out requests to different "servers" or "nodes".
// It is meant to be implemented in two contexts: one for KV
// servers to fan out to cluster nodes, and another for SQL
// servers to fan out to SQL instances. The implementation of
// this interface is used by gRPC services to collect data from
// all cluster nodes or all tenants to return to the user.
//
// TODO(davidh): unify the `getAllNodes` and `nodesList` responses.
// They contain similar data and are confusing to use.
type ServerIterator interface {
	// dialNode provides a gRPC connection to the node or
	// SQL instance identified by serverID.
	dialNode(
		ctx context.Context, serverID serverID,
	) (*grpc.ClientConn, error)
	// getAllNodes returns a map of all nodes in the cluster
	// or instances in the tenant with their liveness status.
	getAllNodes(
		ctx context.Context,
	) (map[serverID]livenesspb.NodeLivenessStatus, error)
	// nodesList returns a list of nodes or instances for the
	// cluster or tenant suitable for returning as the NodesList
	// RPC response.
	nodesList(ctx context.Context) (*serverpb.NodesListResponse, error)
	// parseServerID parses the given string as either a node
	// or instance ID and returns a bool that is true if the
	// ID corresponds to the local node or instance that the
	// call was executed on.
	parseServerID(serverIDParam string) (serverID, bool, error)
	// getID returns the current node or SQL instance ID on the
	// local server.
	getID() serverID
	// getServerIDAddress returns a gRPC address for the given node
	// or SQL instance.
	getServerIDAddress(context.Context, serverID) (*util.UnresolvedAddr, error)
	// getServerIDSQLAddress returns a SQL address for the given node
	// or SQL instance.
	getServerIDSQLAddress(context.Context, serverID) (*util.UnresolvedAddr, error)
}

type tenantFanoutClient struct {
	sqlServer *SQLServer
	rpcCtx    *rpc.Context
	stopper   *stop.Stopper
}

func (t *tenantFanoutClient) nodesList(ctx context.Context) (*serverpb.NodesListResponse, error) {
	instances, err := t.sqlServer.sqlInstanceReader.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	var resp serverpb.NodesListResponse
	for _, instance := range instances {
		nodeDetails := serverpb.NodeDetails{
			NodeID:     int32(instance.InstanceID),
			Address:    util.MakeUnresolvedAddr("tcp", instance.InstanceRPCAddr),
			SQLAddress: util.MakeUnresolvedAddr("tcp", instance.InstanceSQLAddr),
		}
		resp.Nodes = append(resp.Nodes, nodeDetails)
	}
	return &resp, err
}

var _ ServerIterator = &tenantFanoutClient{}

func (t *tenantFanoutClient) getServerIDAddress(
	ctx context.Context, serverID serverID,
) (*util.UnresolvedAddr, error) {
	id := base.SQLInstanceID(serverID)
	instance, err := t.sqlServer.sqlInstanceReader.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	return &util.UnresolvedAddr{
		NetworkField: "tcp",
		AddressField: instance.InstanceRPCAddr,
	}, nil
}

func (t *tenantFanoutClient) getServerIDSQLAddress(
	ctx context.Context, serverID serverID,
) (*util.UnresolvedAddr, error) {
	id := base.SQLInstanceID(serverID)
	instance, err := t.sqlServer.sqlInstanceReader.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	return &util.UnresolvedAddr{
		NetworkField: "tcp",
		AddressField: instance.InstanceSQLAddr,
	}, nil
}

func (t *tenantFanoutClient) getID() serverID {
	return serverID(t.sqlServer.SQLInstanceID())
}

func (t *tenantFanoutClient) dialNode(
	ctx context.Context, serverID serverID,
) (*grpc.ClientConn, error) {
	id := base.SQLInstanceID(serverID)
	instance, err := t.sqlServer.sqlInstanceReader.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	return t.rpcCtx.GRPCDialPod(instance.InstanceRPCAddr, id, rpc.DefaultClass).Connect(ctx)
}

func (t *tenantFanoutClient) getAllNodes(
	ctx context.Context,
) (map[serverID]livenesspb.NodeLivenessStatus, error) {
	liveTenantInstances, err := t.sqlServer.sqlInstanceReader.GetAllInstances(ctx)
	if err != nil {
		return nil, err
	}
	statuses := make(map[serverID]livenesspb.NodeLivenessStatus, len(liveTenantInstances))
	for _, i := range liveTenantInstances {
		statuses[serverID(i.InstanceID)] = livenesspb.NodeLivenessStatus_LIVE
	}
	return statuses, nil
}

func (t *tenantFanoutClient) parseServerID(serverIDParam string) (serverID, bool, error) {
	// No parameter provided or set to local.
	if len(serverIDParam) == 0 || localRE.MatchString(serverIDParam) {
		return serverID(t.sqlServer.SQLInstanceID()), true /* isLocal */, nil /* err */
	}

	id, err := strconv.ParseInt(serverIDParam, 0, 32)
	if err != nil {
		return 0 /* instanceID */, false /* isLocal */, errors.Wrap(err, "instance ID could not be parsed")
	}
	instanceID := base.SQLInstanceID(id)
	return serverID(instanceID), instanceID == t.sqlServer.SQLInstanceID() /* isLocal */, nil
}

type kvFanoutClient struct {
	gossip       *gossip.Gossip
	rpcCtx       *rpc.Context
	db           *kv.DB
	nodeLiveness *liveness.NodeLiveness
	clock        *hlc.Clock
	st           *cluster.Settings
	ambientCtx   log.AmbientContext
}

func (k kvFanoutClient) nodesList(ctx context.Context) (*serverpb.NodesListResponse, error) {
	statuses, _, err := getNodeStatuses(ctx, k.db, 0 /* limit */, 0 /* offset */)
	if err != nil {
		return nil, serverError(ctx, err)
	}
	resp := &serverpb.NodesListResponse{
		Nodes: make([]serverpb.NodeDetails, len(statuses)),
	}
	for i, status := range statuses {
		resp.Nodes[i].NodeID = int32(status.Desc.NodeID)
		resp.Nodes[i].Address = status.Desc.Address
		resp.Nodes[i].SQLAddress = status.Desc.SQLAddress
	}
	return resp, nil
}

var _ ServerIterator = &kvFanoutClient{}

func (k kvFanoutClient) getServerIDAddress(
	_ context.Context, id serverID,
) (*util.UnresolvedAddr, error) {
	return k.gossip.GetNodeIDAddress(roachpb.NodeID(id))
}

func (k kvFanoutClient) getServerIDSQLAddress(
	_ context.Context, id serverID,
) (*util.UnresolvedAddr, error) {
	return k.gossip.GetNodeIDSQLAddress(roachpb.NodeID(id))
}

func (k kvFanoutClient) getID() serverID {
	return serverID(k.gossip.NodeID.Get())
}

func (k kvFanoutClient) dialNode(ctx context.Context, serverID serverID) (*grpc.ClientConn, error) {
	id := roachpb.NodeID(serverID)
	addr, err := k.gossip.GetNodeIDAddress(id)
	if err != nil {
		return nil, err
	}
	return k.rpcCtx.GRPCDialNode(addr.String(), id, rpc.DefaultClass).Connect(ctx)
}

func (k kvFanoutClient) listNodes(ctx context.Context) (*serverpb.NodesResponse, error) {
	ctx = forwardSQLIdentityThroughRPCCalls(ctx)
	ctx = k.ambientCtx.AnnotateCtx(ctx)

	statuses, _, err := getNodeStatuses(ctx, k.db, 0, 0)
	if err != nil {
		return nil, err
	}
	resp := serverpb.NodesResponse{
		Nodes: statuses,
	}

	clock := k.clock
	resp.LivenessByNodeID, err = getLivenessStatusMap(ctx, k.nodeLiveness, clock.Now().GoTime(), k.st)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (k kvFanoutClient) getAllNodes(
	ctx context.Context,
) (map[serverID]livenesspb.NodeLivenessStatus, error) {
	nodes, err := k.listNodes(ctx)
	if err != nil {
		return nil, err
	}
	ret := make(map[serverID]livenesspb.NodeLivenessStatus)
	for _, node := range nodes.Nodes {
		nodeID := node.Desc.NodeID
		livenessStatus := nodes.LivenessByNodeID[nodeID]
		if livenessStatus == livenesspb.NodeLivenessStatus_DECOMMISSIONED {
			// Skip over removed nodes.
			continue
		}
		ret[serverID(nodeID)] = livenessStatus
	}
	return ret, nil
}

func (k kvFanoutClient) parseServerID(serverIDParam string) (serverID, bool, error) {
	n, isLocal, err := parseNodeID(k.gossip, serverIDParam)
	return serverID(n), isLocal, err
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
