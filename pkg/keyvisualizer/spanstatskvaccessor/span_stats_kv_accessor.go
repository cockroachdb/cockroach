package spanstatskvaccessor

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type spanStatsKVAccessor struct {
	server keyvispb.KeyVisualizerServer
}


func New(server keyvispb.KeyVisualizerServer) *spanStatsKVAccessor {
	return &spanStatsKVAccessor{server: server}
}

func (s *spanStatsKVAccessor) GetKeyVisualizerSamples(
	ctx context.Context,
	tenantID roachpb.TenantID,
	start hlc.Timestamp,
	end hlc.Timestamp,
) (*keyvispb.GetSamplesResponse, error) {

	req := &keyvispb.GetSamplesRequest{
		Tenant: &tenantID,
		Start:  start,
		End:    end,
	}

	return s.server.GetSamplesFromAllNodes(ctx, req)
}

func (s *spanStatsKVAccessor) GetTenantRanges(ctx context.Context,
	tenantID roachpb.TenantID) (*keyvispb.GetTenantRangesResponse, error) {

	return s.server.GetTenantRanges(ctx,
		&keyvispb.GetTenantRangesRequest{Tenant: &tenantID})
}

func (s *spanStatsKVAccessor) UpdateBoundaries(
	ctx context.Context,
	tenantID roachpb.TenantID,
	boundaries []*roachpb.Span,
) (*keyvispb.SaveBoundariesResponse, error) {

	return s.server.SaveBoundaries(ctx, &keyvispb.SaveBoundariesRequest{
		Tenant:     &tenantID,
		Boundaries: boundaries,
	})
}
