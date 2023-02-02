// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type scanConfig struct {
	Spans     []roachpb.Span
	Timestamp hlc.Timestamp
	WithDiff  bool
	Knobs     TestingKnobs
}

type kvScanner interface {
	// Scan will scan all of the KVs in the spans specified by the physical config
	// at the specified timestamp and write them to the buffer.
	Scan(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error
}

type scanRequestScanner struct {
	settings                *cluster.Settings
	gossip                  gossip.OptionalGossip
	db                      *kv.DB
	onBackfillRangeCallback func(int64) (func(), func())
}

var _ kvScanner = (*scanRequestScanner)(nil)

func (p *scanRequestScanner) Scan(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if log.V(2) {
		log.Infof(ctx, "performing scan on %v at %v withDiff %v",
			cfg.Spans, cfg.Timestamp, cfg.WithDiff)
	}

	sender := p.db.NonTransactionalSender()
	distSender := sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
	spans, err := getSpansToProcess(ctx, distSender, cfg.Spans)
	if err != nil {
		return err
	}

	var backfillDec, backfillClear func()
	if p.onBackfillRangeCallback != nil {
		backfillDec, backfillClear = p.onBackfillRangeCallback(int64(len(spans)))
		defer backfillClear()
	}

	maxConcurrentScans := maxConcurrentScanRequests(p.gossip, &p.settings.SV)
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
			exportLim.SetLimit(maxConcurrentScanRequests(p.gossip, &p.settings.SV))
		}

		limAlloc, err := exportLim.Begin(ctx)
		if err != nil {
			cancel()
			return errors.CombineErrors(err, g.Wait())
		}

		var spanAlloc kvevent.Alloc
		if allocator, ok := sink.(kvevent.MemAllocator); ok {
			// Sink implements memory allocator interface, so acquire
			// memory needed to hold scan reply.
			spanAlloc, err = allocator.AcquireMemory(ctx, changefeedbase.ScanRequestSize.Get(&p.settings.SV))
			if err != nil {
				cancel()
				return errors.CombineErrors(err, g.Wait())
			}
		}

		g.GoCtx(func(ctx context.Context) error {
			defer limAlloc.Release()
			defer spanAlloc.Release(ctx)

			err := p.exportSpan(ctx, span, cfg.Timestamp, cfg.WithDiff, sink, cfg.Knobs)
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

func (p *scanRequestScanner) exportSpan(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
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
		r := roachpb.NewScan(remaining.Key, remaining.EndKey, false /* forUpdate */).(*roachpb.ScanRequest)
		r.ScanFormat = roachpb.BATCH_RESPONSE
		b.Header.TargetBytes = targetBytesPerScan
		b.AdmissionHeader = roachpb.AdmissionHeader{
			// TODO(irfansharif): Make this configurable if we want system table
			// scanners or support "high priority" changefeeds to run at higher
			// priorities. We use higher AC priorities for system-internal
			// rangefeeds listening in on system table changes.
			Priority: int32(admissionpb.BulkNormalPri),
			// We specify a creation time for each batch (as opposed to at the
			// txn level) -- this way later batches from earlier txns don't just
			// out compete batches from newer txns.
			CreateTime:               start.UnixNano(),
			Source:                   roachpb.AdmissionHeader_FROM_SQL,
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
				ctx, kvevent.NewBackfillResolvedEvent(consumed, ts, jobspb.ResolvedSpan_NONE),
			); err != nil {
				return err
			}
		}
		remaining = res.ResumeSpan
	}
	// p.metrics.PollRequestNanosHist.RecordValue(scanDuration.Nanoseconds())
	if err := sink.Add(
		ctx, kvevent.NewBackfillResolvedEvent(span, ts, jobspb.ResolvedSpan_NONE),
	); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `finished Scan of %s at %s took %s`,
			span, ts.AsOfSystemTime(), timeutil.Since(stopwatchStart))
	}
	return nil
}

func getSpansToProcess(
	ctx context.Context, ds *kvcoord.DistSender, targetSpans []roachpb.Span,
) ([]roachpb.Span, error) {
	ranges, err := AllRangeSpans(ctx, ds, targetSpans)
	if err != nil {
		return nil, err
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
	return requests, nil
}

// slurpScanResponse iterates the ScanResponse and inserts the contained kvs into
// the KVFeed's buffer.
func slurpScanResponse(
	ctx context.Context,
	sink kvevent.Writer,
	res *roachpb.ScanResponse,
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
			if err = sink.Add(ctx, kvevent.NewBackfillKVEvent(keyBytes, ts, valBytes, withDiff, backfillTS)); err != nil {
				return errors.Wrapf(err, `buffering changes for %s`, span)
			}
		}
	}
	return nil
}

// AllRangeSpans returns the list of all ranges that for the specified list of spans.
func AllRangeSpans(
	ctx context.Context, ds *kvcoord.DistSender, spans []roachpb.Span,
) ([]roachpb.Span, error) {

	ranges := make([]roachpb.Span, 0, len(spans))

	it := kvcoord.MakeRangeIterator(ds)

	for i := range spans {
		rSpan, err := keys.SpanAddr(spans[i])
		if err != nil {
			return nil, err
		}
		for it.Seek(ctx, rSpan.Key, kvcoord.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			ranges = append(ranges, roachpb.Span{
				Key: it.Desc().StartKey.AsRawKey(), EndKey: it.Desc().EndKey.AsRawKey(),
			})
			if !it.NeedAnother(rSpan) {
				break
			}
		}
	}

	return ranges, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(gw gossip.OptionalGossip) int {
	g, err := gw.OptionalErr(47971)
	if err != nil {
		// can't count nodes in tenants
		return 1
	}
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeDescPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes
}

// maxConcurrentScanRequests returns the number of concurrent scan requests.
func maxConcurrentScanRequests(gw gossip.OptionalGossip, sv *settings.Values) int {
	// If the user specified ScanRequestLimit -- use that value.
	if max := changefeedbase.ScanRequestLimit.Get(sv); max > 0 {
		return int(max)
	}

	// TODO(yevgeniy): Currently, issuing multiple concurrent updates scaled to the size of
	//  the cluster only make sense for the core change feeds.  This configuration shoould
	//  be specified explicitly when creating scanner.
	nodes := clusterNodeCount(gw)
	// This is all hand-wavy: 3 per node used to be the default for a very long time.
	// However, this could get out of hand if the clusters are large.
	// So cap the max to an arbitrary value of a 100.
	max := 3 * nodes
	if max > 100 {
		max = 100
	}
	return max
}
