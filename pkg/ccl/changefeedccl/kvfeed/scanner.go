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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type kvScanner interface {
	// Scan will scan all of the KVs in the spans specified by the physical config
	// at the specified timestamp and write them to the buffer.
	Scan(ctx context.Context, sink EventBufferWriter, cfg physicalConfig) error
}

type scanRequestScanner struct {
	settings *cluster.Settings
	gossip   gossip.DeprecatedGossip
	db       *kv.DB
}

var _ kvScanner = (*scanRequestScanner)(nil)

func (p *scanRequestScanner) Scan(
	ctx context.Context, sink EventBufferWriter, cfg physicalConfig,
) error {
	if log.V(2) {
		log.Infof(ctx, "performing scan on %v at %v withDiff %v",
			cfg.Spans, cfg.Timestamp, cfg.WithDiff)
	}

	spans, err := getSpansToProcess(ctx, p.db, cfg.Spans)
	if err != nil {
		return err
	}

	// Export requests for the various watched spans are executed in parallel,
	// with a semaphore-enforced limit based on a cluster setting.
	// The spans here generally correspond with range boundaries.
	approxNodeCount, err := clusterNodeCount(p.gossip)
	if err != nil {
		return err
	}
	maxConcurrentExports := approxNodeCount *
		int(kvserver.ExportRequestsLimit.Get(&p.settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)
	g := ctxgroup.WithContext(ctx)

	// atomicFinished is used only to enhance debugging messages.
	var atomicFinished int64

	for _, span := range spans {
		span := span

		// Wait for our semaphore.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exportsSem <- struct{}{}:
		}

		g.GoCtx(func(ctx context.Context) error {
			defer func() { <-exportsSem }()

			err := p.exportSpan(ctx, span, cfg.Timestamp, cfg.WithDiff, sink)
			finished := atomic.AddInt64(&atomicFinished, 1)
			if log.V(2) {
				log.Infof(ctx, `exported %d of %d: %v`, finished, len(spans), err)
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func (p *scanRequestScanner) exportSpan(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, withDiff bool, sink EventBufferWriter,
) error {
	txn := p.db.NewTxn(ctx, "changefeed backfill")
	if log.V(2) {
		log.Infof(ctx, `sending ScanRequest %s at %s`, span, ts)
	}
	txn.SetFixedTimestamp(ctx, ts)
	stopwatchStart := timeutil.Now()
	var scanDuration, bufferDuration time.Duration
	const targetBytesPerScan = 16 << 20 // 16 MiB
	for remaining := span; ; {
		start := timeutil.Now()
		b := txn.NewBatch()
		r := roachpb.NewScan(remaining.Key, remaining.EndKey, false /* forUpdate */).(*roachpb.ScanRequest)
		r.ScanFormat = roachpb.BATCH_RESPONSE
		b.Header.TargetBytes = targetBytesPerScan
		// NB: We use a raw request rather than the Scan() method because we want
		// the MVCC timestamps which are encoded in the response but are filtered
		// during result parsing.
		b.AddRawRequest(r)
		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, `fetching changes for %s`, span)
		}
		afterScan := timeutil.Now()
		res := b.RawResponse().Responses[0].GetScan()
		if err := slurpScanResponse(ctx, sink, res, ts, withDiff, remaining); err != nil {
			return err
		}
		afterBuffer := timeutil.Now()
		scanDuration += afterScan.Sub(start)
		bufferDuration += afterBuffer.Sub(afterScan)
		if res.ResumeSpan != nil {
			remaining = *res.ResumeSpan
		} else {
			break
		}
	}
	// p.metrics.PollRequestNanosHist.RecordValue(scanDuration.Nanoseconds())
	if err := sink.AddResolved(ctx, span, ts, false); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `finished Scan of %s at %s took %s`,
			span, ts.AsOfSystemTime(), timeutil.Since(stopwatchStart))
	}
	return nil
}

func getSpansToProcess(
	ctx context.Context, db *kv.DB, targetSpans []roachpb.Span,
) ([]roachpb.Span, error) {
	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return nil, errors.Wrapf(err, "fetching range descriptors")
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
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, covering.Range{
			Start:   []byte(rangeDesc.StartKey),
			End:     []byte(rangeDesc.EndKey),
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
	sink EventBufferWriter,
	res *roachpb.ScanResponse,
	ts hlc.Timestamp,
	withDiff bool,
	span roachpb.Span,
) error {
	for _, br := range res.BatchResponses {
		for len(br) > 0 {
			var kv roachpb.KeyValue
			var err error
			kv.Key, kv.Value.Timestamp, kv.Value.RawBytes, br, err = enginepb.ScanDecodeKeyValue(br)
			if err != nil {
				return errors.Wrapf(err, `decoding changes for %s`, span)
			}
			var prevVal roachpb.Value
			if withDiff {
				// Include the same value for the "before" and "after" KV, but
				// interpret them at different timestamp. Specifically, interpret
				// the "before" KV at the timestamp immediately before the schema
				// change. This is handled in kvsToRows.
				prevVal = kv.Value
			}
			if err = sink.AddKV(ctx, kv, prevVal, ts); err != nil {
				return errors.Wrapf(err, `buffering changes for %s`, span)
			}
		}
	}
	return nil
}

func allRangeDescriptors(ctx context.Context, txn *kv.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(gw gossip.DeprecatedGossip) (int, error) {
	g, err := gw.OptionalErr(47971)
	if err != nil {
		return 0, err
	}
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes, nil
}
