package server

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"google.golang.org/grpc"
)


// TODO: initialize this server
type spanStatsServer struct {
	server *Server
}

func (s *spanStatsServer) SaveBoundaries(
	ctx context.Context,
	req *keyvispb.SaveBoundariesRequest,
) (*keyvispb.SaveBoundariesResponse, error) {

	// write boundaries to system.span_stats_boundaries
	//s.server.sqlServer.internalExecutor.Exec()

	return nil, nil
}

func (s *spanStatsServer) GetSamplesFromAllNodes(
	ctx context.Context,
	req *keyvispb.GetSamplesRequest,
) (*keyvispb.GetSamplesResponse, error) {

	// dial all nodes and call `GetSamplesFromNode`

	return nil, nil
}

func (s *spanStatsServer) GetSamplesFromNode(
	ctx context.Context,
	req *keyvispb.GetSamplesRequest,
) (*keyvispb.GetSamplesResponse, error) {


	err := s.server.node.stores.VisitStores(func(s *kvserver.Store) error {
		samples := s.GetSpanStatsCollector().GetSamples(*req.Tenant, req.Start, req.End)
		_ = samples
		return nil
	})


	return nil, err
}

func (s *spanStatsServer) RegisterService(g *grpc.Server) {
	keyvispb.RegisterKeyVisualizerServer(g, s)
}
