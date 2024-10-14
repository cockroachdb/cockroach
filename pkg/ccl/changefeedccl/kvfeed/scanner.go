// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvfeed

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type scanConfig struct {
	Spans     []roachpb.Span
	Timestamp hlc.Timestamp
	WithDiff  bool
	Knobs     TestingKnobs
	Boundary  jobspb.ResolvedSpan_BoundaryType
}

type kvScanner interface {
	// Scan will scan all of the KVs in the spans specified by the physical config
	// at the specified timestamp and write them to the buffer.
	Scan(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error
}

type scanRequestScanner struct {
	settings                *cluster.Settings
	db                      *kv.DB
	onBackfillRangeCallback func(int64) (func(), func())
}

var _ kvScanner = (*scanRequestScanner)(nil)

func (p *scanRequestScanner) Scan(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if log.V(2) {
		var sp roachpb.Spans = cfg.Spans
		log.Infof(ctx, "performing scan on %s at %v withDiff %v",
			sp, cfg.Timestamp, cfg.WithDiff)
	}

	sender := p.db.NonTransactionalSender()
	distSender := sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
	spans, numNodesHint, err := getRangesToProcess(ctx, distSender, cfg.Spans)
	if err != nil {
		return err
	}

	var backfillDec, backfillClear func()
	if p.onBackfillRangeCallback != nil {
		backfillDec, backfillClear = p.onBackfillRangeCallback(int64(len(spans)))
		defer backfillClear()
	}

	maxConcurrentScans := maxConcurrentScanRequests(numNodesHint, &p.settings.SV)
	exportLim := limit.MakeConcurrentRequestLimiter("changefeedScanRequestLimiter", maxConcurrentScans)

	lastScanLimitUserSetting := changefeedbase.ScanRequestLimit.Get(&p.settings.SV)

	g := ctxgroup.WithContext(ctx)
	// atomicFinished is used only to enhance debugging messages.
	var atomicFinished int64
	for _, span := range spans {
		span := span

		// If the user defined scan request limit has changed, recalculate it
		if currentUserScanLimit := changefeedbase.ScanRequestLimit.Get(&p.settings.SV); currentUserScanLimit != lastScanLimitUserSetting {
			lastScanLimitUserSetting = currentUserScanLimit
			exportLim.SetLimit(maxConcurrentScanRequests(numNodesHint, &p.settings.SV))
		}

		limAlloc, err := exportLim.Begin(ctx)
		if err != nil {
			cancel()
			return errors.CombineErrors(err, g.Wait())
		}

		g.GoCtx(func(ctx context.Context) error {
			defer limAlloc.Release()
			spanAlloc, err := p.tryAcquireMemory(ctx, sink)
			if err != nil {
				return err
			}
			defer spanAlloc.Release(ctx)

			err = p.exportSpan(ctx, span, cfg.Timestamp, cfg.Boundary, cfg.WithDiff, sink, cfg.Knobs)
			finished := atomic.AddInt64(&atomicFinished, 1)
			if backfillDec != nil {
				backfillDec()
			}
			if log.V(2) {
				log.Infof(ctx, `exported %d of %d: %v`, finished, len(spans), err)
			}
			return err
		})
	}
	return g.Wait()
}

var logMemAcquireEvery = log.Every(5 * time.Second)

// tryAcquireMemory attempts to acquire memory for span export.
func (p *scanRequestScanner) tryAcquireMemory(
	ctx context.Context, sink kvevent.Writer,
) (alloc kvevent.Alloc, err error) {
	allocator, ok := sink.(kvevent.MemAllocator)
	if !ok {
		// Not an allocator -- can't acquire memory.
		return alloc, nil
	}

	// Begin by attempting to acquire memory for the request we're about to issue.
	alloc, err = allocator.AcquireMemory(ctx, changefeedbase.ScanRequestSize.Get(&p.settings.SV))
	if err == nil {
		return alloc, nil
	}

	retryOpts := retry.Options{
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
	}

	// We failed to acquire memory for this export.  Begin retry loop -- we may succeed
	// in the future, once somebody releases memory.
	for attempt := retry.StartWithCtx(ctx, retryOpts); attempt.Next(); {
		// Sink implements memory allocator interface, so acquire
		// memory needed to hold scan reply.
		if logMemAcquireEvery.ShouldLog() {
			log.Errorf(ctx, "Failed to acquire memory for export span: %s (attempt %d)",
				err, attempt.CurrentAttempt()+1)
		}
		alloc, err = allocator.AcquireMemory(ctx, changefeedbase.ScanRequestSize.Get(&p.settings.SV))
		if err == nil {
			return alloc, nil
		}
	}

	return alloc, ctx.Err()
}

func (p *scanRequestScanner) exportSpan(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
	withDiff bool,
	sink kvevent.Writer,
	knobs TestingKnobs,
) error {
	txn := p.db.NewTxn(ctx, "changefeed backfill")
	if log.V(2) {
		log.Infof(ctx, `sending ScanRequest %s at %s`, span, ts)
	}
	if err := txn.SetFixedTimestamp(ctx, ts); err != nil {
		return err
	}
	stopwatchStart := timeutil.Now()
	var scanDuration, bufferDuration time.Duration
	targetBytesPerScan := changefeedbase.ScanRequestSize.Get(&p.settings.SV)
	for remaining := &span; remaining != nil; {
		start := timeutil.Now()
		b := txn.NewBatch()
		r := kvpb.NewScan(remaining.Key, remaining.EndKey).(*kvpb.ScanRequest)
		r.ScanFormat = kvpb.BATCH_RESPONSE
		b.Header.TargetBytes = targetBytesPerScan
		b.Header.ConnectionClass = rpc.RangefeedClass
		b.AdmissionHeader = kvpb.AdmissionHeader{
			// TODO(irfansharif): Make this configurable if we want system table
			// scanners or support "high priority" changefeeds to run at higher
			// priorities. We use higher AC priorities for system-internal
			// rangefeeds listening in on system table changes.
			Priority: int32(admissionpb.BulkNormalPri),
			// We specify a creation time for each batch (as opposed to at the
			// txn level) -- this way later batches from earlier txns don't just
			// out compete batches from newer txns.
			CreateTime:               start.UnixNano(),
			Source:                   kvpb.AdmissionHeader_FROM_SQL,
			NoMemoryReservedAtSource: true,
		}
		// NB: We use a raw request rather than the Scan() method because we want
		// the MVCC timestamps which are encoded in the response but are filtered
		// during result parsing.
		b.AddRawRequest(r)
		if knobs.BeforeScanRequest != nil {
			if err := knobs.BeforeScanRequest(b); err != nil {
				return err
			}
		}

		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, `fetching changes for %s`, span)
		}
		afterScan := timeutil.Now()
		res := b.RawResponse().Responses[0].GetScan()
		if err := slurpScanResponse(ctx, sink, res, ts, withDiff, *remaining); err != nil {
			return err
		}
		afterBuffer := timeutil.Now()
		scanDuration += afterScan.Sub(start)
		bufferDuration += afterBuffer.Sub(afterScan)
		if res.ResumeSpan != nil {
			consumed := roachpb.Span{Key: remaining.Key, EndKey: res.ResumeSpan.Key}
			if err := sink.Add(
				ctx, kvevent.NewBackfillResolvedEvent(consumed, ts, boundaryType),
			); err != nil {
				return err
			}
		}
		remaining = res.ResumeSpan
	}
	// p.metrics.PollRequestNanosHist.RecordValue(scanDuration.Nanoseconds())
	if err := sink.Add(
		ctx, kvevent.NewBackfillResolvedEvent(span, ts, boundaryType),
	); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `finished Scan of %s at %s took %s`,
			span, ts.AsOfSystemTime(), timeutil.Since(stopwatchStart))
	}
	return nil
}

// getRangesToProcess returns the list of ranges covering input list of spans.
// Returns the number of nodes that are leaseholders for those spans.
func getRangesToProcess(
	ctx context.Context, ds *kvcoord.DistSender, targetSpans []roachpb.Span,
) ([]roachpb.Span, int, error) {
	ranges, numNodes, err := ds.AllRangeSpans(ctx, targetSpans)
	if err != nil {
		return nil, 0, err
	}

	type spanMarker struct{}
	type rangeMarker struct{}

	var spanCovering covering.Covering
	for _, span := range targetSpans {
		spanCovering = append(spanCovering, covering.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: spanMarker{},
		})
	}

	var rangeCovering covering.Covering
	for _, r := range ranges {
		rangeCovering = append(rangeCovering, covering.Range{
			Start:   []byte(r.Key),
			End:     []byte(r.EndKey),
			Payload: rangeMarker{},
		})
	}

	chunks := covering.OverlapCoveringMerge(
		[]covering.Covering{spanCovering, rangeCovering},
	)

	var requests []roachpb.Span
	for _, chunk := range chunks {
		if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
			continue
		}
		requests = append(requests, roachpb.Span{Key: chunk.Start, EndKey: chunk.End})
	}
	return requests, numNodes, nil
}

// slurpScanResponse iterates the ScanResponse and inserts the contained kvs into
// the KVFeed's buffer.
func slurpScanResponse(
	ctx context.Context,
	sink kvevent.Writer,
	res *kvpb.ScanResponse,
	backfillTS hlc.Timestamp,
	withDiff bool,
	span roachpb.Span,
) error {
	var keyBytes, valBytes []byte
	var ts hlc.Timestamp
	var err error
	for _, br := range res.BatchResponses {
		for len(br) > 0 {
			keyBytes, ts, valBytes, br, err = enginepb.ScanDecodeKeyValue(br)
			if err != nil {
				return errors.Wrapf(err, `decoding changes for %s`, span)
			}
			if log.V(3) {
				log.Infof(ctx, "scanResponse: %s@%s", keys.PrettyPrint(nil, keyBytes), ts)
			}
			if err = sink.Add(ctx, kvevent.NewBackfillKVEvent(keyBytes, ts, valBytes, withDiff, backfillTS)); err != nil {
				return errors.Wrapf(err, `buffering changes for %s`, span)
			}
		}
	}
	return nil
}

// maxConcurrentScanRequests returns the number of concurrent scan requests.
func maxConcurrentScanRequests(numNodesHint int, sv *settings.Values) int {
	// If the user specified ScanRequestLimit -- use that value.
	if max := changefeedbase.ScanRequestLimit.Get(sv); max > 0 {
		return int(max)
	}
	if numNodesHint < 1 {
		return 1
	}

	// This is all hand-wavy: 3 per node used to be the default for a very long time.
	// However, this could get out of hand if the clusters are large.
	// So cap the max to an arbitrary value of a 100.
	max := 3 * numNodesHint
	if max > 100 {
		max = 100
	}
	return max
}
