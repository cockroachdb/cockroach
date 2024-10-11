package producer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// makeSpan takes a comma delimited string like "a,b" and splits it into an
// [a, b) span.
func makeSpan(span string) roachpb.Span {
	start, end, _ := strings.Cut(span, ",")
	return roachpb.Span{
		Key:    roachpb.Key(start),
		EndKey: roachpb.Key(end),
	}
}

type rangeIteratorFactory struct {
	t      *testing.T
	ranges []string
}

func (r *rangeIteratorFactory) NewLazyIterator(
	ctx context.Context, span roachpb.Span, pageSize int,
) (rangedesc.LazyIterator, error) {
	var rangeDescs []roachpb.RangeDescriptor
	for _, span := range r.ranges {
		rangeSpan := makeSpan(span)
		rangeDescs = append(rangeDescs, roachpb.RangeDescriptor{
			StartKey: roachpb.RKey(rangeSpan.Key),
			EndKey:   roachpb.RKey(rangeSpan.EndKey),
		})
	}
	return rangedesc.NewPaginatedIter(ctx, span, pageSize, func(_ context.Context, span roachpb.Span, _ int) ([]roachpb.RangeDescriptor, error) {
		var filteredDesc []roachpb.RangeDescriptor
		for _, desc := range rangeDescs {
			toRight := span.Key.Compare(desc.EndKey.AsRawKey()) == 1
			toLeft := span.EndKey.Compare(desc.StartKey.AsRawKey()) == -1
			if toRight || toLeft {
				continue
			}
			filteredDesc = append(filteredDesc, desc)
		}
		return filteredDesc, nil
	})
}

func (r *rangeIteratorFactory) NewIterator(
	ctx context.Context, span roachpb.Span,
) (rangedesc.Iterator, error) {
	// The test only uses the lazy iterator
	panic("unimplemented")
}

func TestNewPoller(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name      string
		ranges    []string
		tracked   []string
		completed []string
		stats     streampb.StreamEvent_RangeStats
	}
	tests := []testCase{
		{
			name: "empty",
		},
		{
			name:    "mixed-range",
			ranges:  []string{"/1,/1/2"},
			tracked: []string{"/1,/1/2"},
			completed: []string{
				"/1,/1/1/100",
				// note the gap of "/1/1/100,/1/1/400"
				"/1/1/400,/1/1/600",
				// note the gap of "/1/1/600,/1/1/800"
				"/1/1/800,/1/2",
			},
			stats: streampb.StreamEvent_RangeStats{
				RangeCount:         1,
				ScanningRangeCount: 1,
			},
		},
		{
			name:      "multiple-ranges",
			ranges:    []string{"/1,/1/2", "/2/1,/2/2"},
			tracked:   []string{"/1,/1/2", "/2/1,/2/2"},
			completed: []string{"/1,/1/2"},
			stats: streampb.StreamEvent_RangeStats{
				RangeCount:         2,
				ScanningRangeCount: 1,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ranges := &rangeIteratorFactory{t, tc.ranges}

			var trackedSpans []roachpb.Span
			for _, span := range tc.tracked {
				trackedSpans = append(trackedSpans, makeSpan(span))
			}
			frontier, err := span.MakeFrontier(trackedSpans...)
			require.NoError(t, err)

			for _, span := range tc.completed {
				_, err := frontier.Forward(makeSpan(span), hlc.Timestamp{
					WallTime: time.Now().UnixNano(),
				})
				require.NoError(t, err)
			}

			poller := startStatsPoller(context.Background(), time.Minute, trackedSpans, frontier, ranges)
			testutils.SucceedsSoon(t, func() error {
				stats := poller.stats.Load()
				if stats == nil {
					return errors.New("waiting for stats")
				}
				require.Equal(t, *stats, tc.stats)
				return nil
			})
			poller.Close()
		})
	}
}
