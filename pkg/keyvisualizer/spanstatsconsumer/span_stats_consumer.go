package spanstatsconsumer

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"time"
)

type SpanStatsConsumer struct {
	tenantID   roachpb.TenantID
	kvAccessor keyvisualizer.KVAccessor
	ie         *sql.InternalExecutor
}

var _ keyvisualizer.SpanStatsConsumer = &SpanStatsConsumer{}

func New(
	tenantID roachpb.TenantID,
	accessor keyvisualizer.KVAccessor,
	executor *sql.InternalExecutor,
) *SpanStatsConsumer {
	return &SpanStatsConsumer{
		tenantID:   tenantID,
		kvAccessor: accessor,
		ie:         executor,
	}
}


func (s *SpanStatsConsumer) FetchStats(
	ctx context.Context,
) error {

	samples, err := s.kvAccessor.GetKeyVisualizerSamples(
		ctx,
		s.tenantID,
		hlc.Timestamp{},
		hlc.Timestamp{},
	)

	if err != nil {
		return err
	}

	//small := &keyvispb.GetSamplesResponse{Samples: make([]*spanstatspb.Sample, 0)}

	for _, sample := range samples.Samples {
		//small.Samples = append(small.Samples, &spanstatspb.Sample{
		//	SampleTime: sample.SampleTime,
		//	SpanStats:  downsample(sample.SpanStats, 512)
		//})
		sample.SpanStats = downsample(sample.SpanStats, 512)
	}

	return keyvisstorage.WriteSamples(ctx, s.ie, samples)
}

func (s *SpanStatsConsumer) DecideBoundaries(ctx context.Context) error {

	// This is the first iteration of the tenant deciding which spans it should
	// tell KV to collect statistics for.
	// It will tell KV to collect statistics over its own ranges.
	resp, err := s.kvAccessor.GetTenantRanges(ctx, s.tenantID)

	if err != nil {
		return err
	}

	_, err = s.kvAccessor.UpdateBoundaries(ctx, s.tenantID, resp.Boundaries)

	return err
}

func (s *SpanStatsConsumer) DeleteOldestSamples(ctx context.Context) error {
	const TwoWeeks = time.Hour * 24 * 14
	watermark := timeutil.Now().Unix() - int64(TwoWeeks.Seconds())
	return keyvisstorage.DeleteSamplesBeforeUnixTime(ctx, s.ie, watermark)
}
