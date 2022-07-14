// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func (s *Store) SetBucketBoundaries() error {
	// XXX: XXX: This is how we're "rolling over" per-store stats today, happens
	// every time the SQL pod installs new boundaries. What should happen
	// instead is each store periodically rolls this data over itself, and
	// persists locally the data corresponding to the last time period. It needs
	// to make sure that there's only a finite set of such data maintained
	// locally, after which it can delete them (circular buffer). This is a time
	// budget, the number of historical histograms maintained (where each
	// corresponds to some time interval).
	// - Each histogram should be finite in size, because the number of buckets
	//   are finite. This is pre-downsampling.
	//  - Zach: For a fixed storage size in bytes, if I have a lot of ranges,
	//    I'm storing a smaller window of histograms.
	//  - Alternative: The per-interval histogram size should be independent of
	//    the amount of data/ranges in the cluster/tenant. The SQL pod is
	//    responsible for picking a finite set of buckets. Similar downsampling
	//    ideas apply when deciding what the set of bucket boundaries ought to
	//    be, except that we have a larger number of buckets to maintain since
	//    they're done much cheaper.
	//    - We like that downsampling CPU work is attributable to the tenant,
	//      and it's a given since it'll happen in the job running in the tenant
	//      pod itself. To do such downsampling for collection boundaries, the
	//      tenant can collect whatever data is needed from KV -- which
	//      includes whatever previous histograms were present, range
	//      boundaries, etc.
	//    - The histogram we're collecting stats into is always capped to some
	//      size.
	//
	// Each KV server:
	// - Waits to be told of bucket boundaries per-tenant (at the very start of
	//   a KV server).
	//   - First thought: What happens after a KV server restart. It'd be bad to
	//     wait for the SQL pod to tell it to collect along specific boundaries,
	//     because that means the SQL pod needs to react to KV servers
	//     restarting. So instead, if we do have locally persisted histogram
	//     from before we restarted, we'll initialize our histograms using those
	//     boundaries.
	//     - What happens when a single KV server is unavailable, and a SQL pod
	//       tries to install bucket boundaries cluster-wide. What we don't want
	//       to have happen is some of the stores learn about it and collect
	//       along boundaries B2, and some others either continue with B1 from
	//       earlier, or if the KV servers were unavailable during the bucket
	//       installation process, post-restart continue maintaining B1.
	//   - Alternative: we've actually already thought about this
	//     (https://docs.google.com/document/d/1Cp-JVh460ezFaxd0ZglVFGa2orUvWPNXPJKpOgEfV6s/edit)
	//     look at the "Per-store stats collector" section, and ignore the two
	//     paragraphs above. An example of this is spanconfig.KVSubscriber (has
	//     some additional XXX: text there).
	hist := newSpanStatsHistogram()

	var err error
	s.VisitReplicas(func(replica *Replica) (wantMore bool) {
		desc := replica.Desc()

		// TODO: is this the correct time to query bytes?
		nBytes := replica.GetMVCCStats().LiveBytes // do not aggregate.
		err = hist.addBucket(desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey(), uint64(nBytes))

		if err != nil {
			return false
		}

		return true
	})

	// TODO: acquire lock
	if err != nil {
		return err
	}

	s.spanStatsHistogram = hist
	return nil
}


// TODO: use start and end times. Currently assuming the request only cares about the current active histogram.
func (s *Store) VisitSpanStatsBuckets(visitor func(span *roachpb.Span, batchRequests uint64, nBytes uint64)) {

	// TODO: acquire mutex lock?
	it := s.spanStatsHistogram.tree.Iterator()

	for {
		i, next := it.Next()
		if next == false {
			break
		}
		bucket := i.(*spanStatsHistogramBucket)
		visitor(&bucket.sp, bucket.counter, bucket.nBytes)
	}
}

type tenantShardedSpanStats struct {
	// XXX: scope this to specific time windows, and roll things over etc.
	histogram map[roachpb.TenantID]spanStatsHistogram
}

type spanStatsHistogram struct {
	syncutil.Mutex // XXX: actually use this
	// XXX: span oriented data structure, queryable by a span, values are counters.
	// Scan[a,d) --> a list of counters of increment
	// GetAllCountersBetween(a, d) -> inc each one.
	tree         interval.Tree // tree of spans -> counters,
	lastBucketId uint64
}

func (s *spanStatsHistogram) addBucket(startKey, endKey roachpb.Key, nBytes uint64) error {
	s.lastBucketId++

	bucket := spanStatsHistogramBucket{
		sp:      roachpb.Span{Key: startKey, EndKey: endKey},
		id:      uintptr(s.lastBucketId),
		counter: 0,
		nBytes: nBytes,
	}

	return s.tree.Insert(&bucket, false)
}

func newSpanStatsHistogram() *spanStatsHistogram {
	return &spanStatsHistogram{
		tree: interval.NewTree(interval.ExclusiveOverlapper),
	}
}

type spanStatsHistogramBucket struct {
	sp      roachpb.Span
	id      uintptr
	counter uint64
	nBytes 	uint64
}

func (s *spanStatsHistogramBucket) Range() interval.Range {
	return s.sp.AsRange()
}

func (s *spanStatsHistogramBucket) ID() uintptr {
	return s.id
}

var _ interval.Interface = &spanStatsHistogramBucket{}

func (s *spanStatsHistogram) increment(sp roachpb.Span) {
	s.tree.DoMatching(func(i interval.Interface) (done bool) {
		bucket := i.(*spanStatsHistogramBucket)
		bucket.counter += 1
		return false // want more
	}, sp.AsRange())
}
