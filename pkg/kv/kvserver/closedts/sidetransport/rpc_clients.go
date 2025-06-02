package sidetransport

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
)

type nodeClientDialer struct {
	nd nodeDialer
}

func (d *nodeClientDialer) DialSideTransportClient(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (ctpb.SideTransportClient, error) {
	if true { // TODO(server) gRPC
		conn, err := d.nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return ctpb.NewSideTransportClient(conn), nil
	}
	return nil, nil // DRPC
}

func NewClientDialer(d nodeDialer) *nodeClientDialer {
	// TODO(server) can we avoid allocation?
	return &nodeClientDialer{
		nd: d,
	}
}
