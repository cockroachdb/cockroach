package storeliveness

import (
	"context"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	circuit2 "github.com/cockroachdb/cockroach/pkg/util/circuit"
	"google.golang.org/grpc"
)

type nodeClientDialer nodedialer.Dialer

func (d *nodeClientDialer) DialStoreLivenessClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (slpb.StoreLivenessClient, error) {
	nd := (*nodedialer.Dialer)(d)
	if true { // TODO(server) gRPC
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return slpb.NewStoreLivenessClient(conn), nil
	}
	return nil, nil // DRPC
}

func (d *nodeClientDialer) GetCircuitBreaker(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (*circuit2.Breaker, bool) {
	nd := (*nodedialer.Dialer)(d)
	return nd.GetCircuitBreaker(nodeID, class)
}

func (d *nodeClientDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	nd := (*nodedialer.Dialer)(d)
	return nd.Dial(ctx, nodeID, class)
}

func AsClientDialer(d *nodedialer.Dialer) *nodeClientDialer {
	return (*nodeClientDialer)(d)
}
