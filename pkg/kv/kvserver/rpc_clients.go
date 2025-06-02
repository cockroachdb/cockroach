package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	circuit2 "github.com/cockroachdb/cockroach/pkg/util/circuit"
)

type nodeClientDialer nodedialer.Dialer

func (d *nodeClientDialer) DialMultiRaftClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (MultiRaftClient, error) {
	nd := (*nodedialer.Dialer)(d)
	if true { // TODO(server) gRPC
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewMultiRaftClient(conn), nil
	}
	return nil, nil // DRPC
}

func (d *nodeClientDialer) DialPerStoreClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (PerStoreClient, error) {
	nd := (*nodedialer.Dialer)(d)
	if true { // TODO(server) gRPC
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewPerStoreClient(conn), nil
	}
	return nil, nil // DRPC
}

func (d *nodeClientDialer) DialPerReplicaClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (PerReplicaClient, error) {
	nd := (*nodedialer.Dialer)(d)
	if true { // TODO(server) gRPC
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewPerReplicaClient(conn), nil
	}
	return nil, nil // DRPC
}

func (d *nodeClientDialer) GetCircuitBreaker(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (*circuit2.Breaker, bool) {
	nd := (*nodedialer.Dialer)(d)
	return nd.GetCircuitBreaker(nodeID, class)
}

func AsClientDialer(d *nodedialer.Dialer) *nodeClientDialer {
	return (*nodeClientDialer)(d)
}
