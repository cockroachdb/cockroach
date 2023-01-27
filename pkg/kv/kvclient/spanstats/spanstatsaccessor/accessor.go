package spanstatsaccessor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// LocalAccessor is an implementation of spanstats.Accessor that is meant
// to provide access to span span stats to servers that are co-located on the KV
// node.
type LocalAccessor struct {
	spanStatsServer serverpb.SpanStatsServer
}

// New returns a new instance of AccessorImpl.
func New(server serverpb.SpanStatsServer) *LocalAccessor {
	return &LocalAccessor{spanStatsServer: server}
}

// SpanStats implements the spanstats.Accessor interface.
func (a *LocalAccessor) SpanStats(
	ctx context.Context, startKey,
	endKey roachpb.Key, nodeID roachpb.NodeID,
) (*serverpb.InternalSpanStatsResponse, error) {

	res, err := a.spanStatsServer.GetSpanStats(ctx,
		&serverpb.InternalSpanStatsRequest{
			Span: roachpb.Span{
				Key:    startKey,
				EndKey: endKey,
			},
			NodeID: nodeID,
		})

	if err != nil {
		return nil, err
	}

	return res, nil
}
