package spanstatscollector

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type tenantStatsBucket struct {
	sp      roachpb.Span
	id      uintptr
	counter uint64
}

func (t *tenantStatsBucket) Range() interval.Range {
	return t.sp.AsRange()
}

func (t *tenantStatsBucket) ID() uintptr {
	return t.id
}

var _ interval.Interface = &tenantStatsBucket{}

type tenantStatsCollector struct {
	stashedBoundaries []*roachpb.Span
	tree              interval.Tree
}

func newTenantCollector() *tenantStatsCollector {
	return &tenantStatsCollector{
		stashedBoundaries: nil,
		tree:              interval.NewTree(interval.ExclusiveOverlapper),
	}
}

func newTreeWithBoundaries(spans []*roachpb.Span) (interval.Tree, error) {
	t := interval.NewTree(interval.ExclusiveOverlapper)

	for i, sp := range spans {
		bucket := tenantStatsBucket{
			sp:      *sp,
			id:      uintptr(i),
			counter: 0,
		}
		err := t.Insert(&bucket, false)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (t *tenantStatsCollector) getStats() []*spanstatspb.SpanStats {

	stats := make([]*spanstatspb.SpanStats, 0)
	// TODO: acquire mutex lock?
	it := t.tree.Iterator()
	for {
		i, next := it.Next()
		if next == false {
			break
		}
		bucket := i.(*tenantStatsBucket)
		stats = append(stats, &spanstatspb.SpanStats{
			Span:     &bucket.sp,
			Requests: bucket.counter,
		})
	}

	return stats
}

func (t *tenantStatsCollector) increment(sp roachpb.Span) {
	t.tree.DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*tenantStatsBucket)
		bucket.counter += 1
		return false // want more
	}, sp.AsRange())
}

type SpanStatsCollector struct {
	collectors map[roachpb.TenantID]*tenantStatsCollector
	clock      *hlc.Clock
	rff        *rangefeed.Factory
}

func New(
	clock *hlc.Clock,
	rff *rangefeed.Factory,
) *SpanStatsCollector {
	return &SpanStatsCollector{
		collectors: map[roachpb.TenantID]*tenantStatsCollector{},
		clock:      clock,
		rff:        rff,
	}
}

func (s *SpanStatsCollector) Start(ctx context.Context, stopper *stop.Stopper) error {
	codec := keys.MakeSQLCodec(roachpb.SystemTenantID)
	tablePrefix := codec.TablePrefix(keys.SpanStatsTenantBoundariesTableID)
	tableSpan := roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}

	columns := systemschema.SpanStatsTenantBoundariesTable.PublicColumns()
	decoder := valueside.MakeDecoder(columns)
	alloc := tree.DatumAlloc{}

	translateEvent := func(
		ctx context.Context,
		kv *roachpb.RangeFeedValue,
	) rangefeedbuffer.Event {

		bytes, err := kv.Value.GetTuple()
		if err != nil {
			log.Warningf(ctx, err.Error())
			return nil
		}

		// try to decode the row
		datums, err := decoder.Decode(&alloc, bytes)
		if err != nil {
			log.Warningf(ctx, err.Error())
			return nil
		}

		// decode the stashed boundaries from the binary encoded proto
		decoded := keyvispb.SaveBoundariesRequest{}
		boundariesEncoded := []byte(tree.MustBeDBytes(datums[1]))

		if err := protoutil.Unmarshal(boundariesEncoded, &decoded); err != nil {
			log.Warningf(ctx, err.Error())
			return nil
		}

		s.saveBoundaries(*decoded.Tenant, decoded.Boundaries)
		return nil
	}

	w := rangefeedcache.NewWatcher(
		"tenant-boundaries-watcher",
		s.clock,
		s.rff,
		10,
		[]roachpb.Span{tableSpan},
		false,
		translateEvent,
		func(ctx context.Context, update rangefeedcache.Update) {
			// noop
		},
		nil,
	)

	// Kick off the rangefeedcache which will retry until the stopper stops.
	if err := rangefeedcache.Start(ctx, stopper, w, func(err error) {
		log.Errorf(ctx, "span stats collector rangefeed error: %s", err.Error())
	}); err != nil {
		return err // we're shutting down
	}

	return nil

}

func (s *SpanStatsCollector) Increment(
	id roachpb.TenantID,
	span roachpb.Span,
) error {

	if collector, ok := s.collectors[id]; ok {
		collector.increment(span)
	} else {
		return errors.New("could not increment collector for tenant")
	}

	return nil
}

func (s *SpanStatsCollector) saveBoundaries(id roachpb.TenantID,
	boundaries []*roachpb.Span,
) {

	if collector, ok := s.collectors[id]; ok {
		collector.stashedBoundaries = boundaries
	} else {
		newCollector := newTenantCollector()
		newCollector.stashedBoundaries = boundaries
		s.collectors[id] = newCollector
	}
}

// GetSamples returns the tenant's statistics to the caller.
// It implicitly starts a new collection period by clearing the old
// statistics. A sample period is therefore defined by the interval between a
// tenant requesting samples. TODO(zachlite): this will change with
// improved fault tolerance mechanisms.
func (s *SpanStatsCollector) GetSamples(
	id roachpb.TenantID) ([]spanstatspb.Sample, error) {

	if collector, ok := s.collectors[id]; ok {
		stats := collector.getStats()

		t, err := newTreeWithBoundaries(collector.stashedBoundaries)
		if err != nil {
			return nil, err
		}

		collector.tree = t


		// TODO(zachlite): until the collector can stash tenant samples,
		// the collector will only return one sample at a time.
		// While this is the case,
		// the server sets the timestamp of the outgoing sample.
		return []spanstatspb.Sample{{
			SampleTime: nil,
			SpanStats:  stats,
		}}, nil
	}
	return nil, errors.New("could not get stats for tenant")
}
