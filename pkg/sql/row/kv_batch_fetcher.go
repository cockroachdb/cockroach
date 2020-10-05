// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// kvBatchSize is the number of keys we request at a time.
// On a single node, 1000 was enough to avoid any performance degradation. On
// multi-node clusters, we want bigger chunks to make up for the higher latency.
// TODO(radu): parameters like this should be configurable
var kvBatchSize int64 = int64(util.ConstantWithMetamorphicTestValue(
	10000, /* defaultValue */
	1,     /* metamorphicValue */
))

// TestingSetKVBatchSize changes the kvBatchFetcher batch size, and returns a function that restores it.
// This is to be used only in tests - we have no test coverage for arbitrary kv batch sizes at this time.
func TestingSetKVBatchSize(val int64) func() {
	oldVal := kvBatchSize
	kvBatchSize = val
	return func() { kvBatchSize = oldVal }
}

// sendFunc is the function used to execute a KV batch; normally
// wraps (*client.Txn).Send.
type sendFunc func(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error)

// txnKVFetcher handles retrieval of key/values.
type txnKVFetcher struct {
	// "Constant" fields, provided by the caller.
	sendFn sendFunc
	spans  roachpb.Spans
	// If useBatchLimit is true, batches are limited to kvBatchSize. If
	// firstBatchLimit is also set, the first batch is limited to that value.
	// Subsequent batches are larger, up to kvBatchSize.
	firstBatchLimit int64
	useBatchLimit   bool
	reverse         bool
	// lockStrength represents the locking mode to use when fetching KVs.
	lockStrength descpb.ScanLockingStrength
	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	lockWaitPolicy descpb.ScanLockingWaitPolicy

	fetchEnd bool
	batchIdx int

	// requestSpans contains the spans that were requested in the last request,
	// and is one to one with responses. This field is kept separately from spans
	// so that the fetcher can keep track of which response was produced for each
	// input span.
	requestSpans roachpb.Spans
	responses    []roachpb.ResponseUnion

	origSpan         roachpb.Span
	remainingBatches [][]byte
	mon              *mon.BytesMonitor
	acc              mon.BoundAccount
}

var _ kvBatchFetcher = &txnKVFetcher{}

// getBatchSize returns the max size of the next batch.
func (f *txnKVFetcher) getBatchSize() int64 {
	return f.getBatchSizeForIdx(f.batchIdx)
}

func (f *txnKVFetcher) getBatchSizeForIdx(batchIdx int) int64 {
	if !f.useBatchLimit {
		return 0
	}
	if f.firstBatchLimit == 0 || f.firstBatchLimit >= kvBatchSize {
		return kvBatchSize
	}

	// We grab the first batch according to the limit. If it turns out that we
	// need another batch, we grab a bigger batch. If that's still not enough,
	// we revert to the default batch size.
	switch batchIdx {
	case 0:
		return f.firstBatchLimit

	case 1:
		// Make the second batch 10 times larger (but at most the default batch
		// size and at least 1/10 of the default batch size). Sample
		// progressions of batch sizes:
		//
		//  First batch | Second batch | Subsequent batches
		//  -----------------------------------------------
		//         1    |     1,000     |     10,000
		//       100    |     1,000     |     10,000
		//       500    |     5,000     |     10,000
		//      1000    |    10,000     |     10,000
		secondBatch := f.firstBatchLimit * 10
		switch {
		case secondBatch < kvBatchSize/10:
			return kvBatchSize / 10
		case secondBatch > kvBatchSize:
			return kvBatchSize
		default:
			return secondBatch
		}

	default:
		return kvBatchSize
	}
}

// getKeyLockingStrength returns the configured per-key locking strength to use
// for key-value scans.
func (f *txnKVFetcher) getKeyLockingStrength() lock.Strength {
	switch f.lockStrength {
	case descpb.ScanLockingStrength_FOR_NONE:
		return lock.None

	case descpb.ScanLockingStrength_FOR_KEY_SHARE:
		// Promote to FOR_SHARE.
		fallthrough
	case descpb.ScanLockingStrength_FOR_SHARE:
		// We currently perform no per-key locking when FOR_SHARE is used
		// because Shared locks have not yet been implemented.
		return lock.None

	case descpb.ScanLockingStrength_FOR_NO_KEY_UPDATE:
		// Promote to FOR_UPDATE.
		fallthrough
	case descpb.ScanLockingStrength_FOR_UPDATE:
		// We currently perform exclusive per-key locking when FOR_UPDATE is
		// used because Upgrade locks have not yet been implemented.
		return lock.Exclusive

	default:
		panic(errors.AssertionFailedf("unknown locking strength %s", f.lockStrength))
	}
}

// getWaitPolicy returns the configured lock wait policy to use for key-value
// scans.
func (f *txnKVFetcher) getWaitPolicy() lock.WaitPolicy {
	switch f.lockWaitPolicy {
	case descpb.ScanLockingWaitPolicy_BLOCK:
		return lock.WaitPolicy_Block

	case descpb.ScanLockingWaitPolicy_SKIP:
		// Should not get here. Query should be rejected during planning.
		panic(errors.AssertionFailedf("unsupported wait policy %s", f.lockWaitPolicy))

	case descpb.ScanLockingWaitPolicy_ERROR:
		return lock.WaitPolicy_Error

	default:
		panic(errors.AssertionFailedf("unknown wait policy %s", f.lockWaitPolicy))
	}
}

// makeKVBatchFetcher initializes a kvBatchFetcher for the given spans.
//
// If useBatchLimit is true, batches are limited to kvBatchSize. If
// firstBatchLimit is also set, the first batch is limited to that value.
// Subsequent batches are larger, up to kvBatchSize.
//
// Batch limits can only be used if the spans are ordered.
func makeKVBatchFetcher(
	txn *kv.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	mon *mon.BytesMonitor,
) (txnKVFetcher, error) {
	sendFn := func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		return res, nil
	}
	return makeKVBatchFetcherWithSendFunc(
		sendFn, spans, reverse, useBatchLimit, firstBatchLimit, lockStrength, lockWaitPolicy, mon,
	)
}

// makeKVBatchFetcherWithSendFunc is like makeKVBatchFetcher but uses a custom
// send function.
func makeKVBatchFetcherWithSendFunc(
	sendFn sendFunc,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	mon *mon.BytesMonitor,
) (txnKVFetcher, error) {
	if firstBatchLimit < 0 || (!useBatchLimit && firstBatchLimit != 0) {
		return txnKVFetcher{}, errors.Errorf("invalid batch limit %d (useBatchLimit: %t)",
			firstBatchLimit, useBatchLimit)
	}

	if useBatchLimit {
		// Verify the spans are ordered if a batch limit is used.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) < 0 {
				return txnKVFetcher{}, errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
			}
		}
	} else if util.RaceEnabled {
		// Otherwise, just verify the spans don't contain consecutive overlapping
		// spans.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) >= 0 {
				// Current span's start key is greater than or equal to the last span's
				// end key - we're good.
				continue
			} else if spans[i].EndKey.Compare(spans[i-1].Key) < 0 {
				// Current span's end key is less than or equal to the last span's start
				// key - also good.
				continue
			}
			// Otherwise, the two spans overlap, which isn't allowed - it leaves us at
			// risk of incorrect results, since the row fetcher can't distinguish
			// between identical rows in two different batches.
			return txnKVFetcher{}, errors.Errorf("overlapping neighbor spans (%s %s)", spans[i-1], spans[i])
		}
	}

	// Make a copy of the spans because we update them.
	copySpans := make(roachpb.Spans, len(spans))
	for i := range spans {
		if reverse {
			// Reverse scans receive the spans in decreasing order.
			copySpans[len(spans)-i-1] = spans[i]
		} else {
			copySpans[i] = spans[i]
		}
	}

	return txnKVFetcher{
		sendFn:          sendFn,
		spans:           copySpans,
		reverse:         reverse,
		useBatchLimit:   useBatchLimit,
		firstBatchLimit: firstBatchLimit,
		lockStrength:    lockStrength,
		lockWaitPolicy:  lockWaitPolicy,
		mon:             mon,
		acc:             mon.MakeBoundAccount(),
	}, nil
}

// maxScanResponseBytes is the maximum number of bytes a scan request can
// return.
const maxScanResponseBytes = 10 * (1 << 20)

// fetch retrieves spans from the kv layer.
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	var ba roachpb.BatchRequest
	ba.Header.WaitPolicy = f.getWaitPolicy()
	ba.Header.MaxSpanRequestKeys = f.getBatchSize()
	if ba.Header.MaxSpanRequestKeys > 0 {
		// If this kvfetcher limits the number of rows returned, also use
		// target bytes to guard against the case in which the average row
		// is very large.
		// If no limit is set, the assumption is that SQL *knows* that there
		// is only a "small" amount of data to be read, and wants to preserve
		// concurrency for this request inside of DistSender, which setting
		// TargetBytes would interfere with.
		ba.Header.TargetBytes = maxScanResponseBytes
	}
	ba.Requests = make([]roachpb.RequestUnion, len(f.spans))
	keyLocking := f.getKeyLockingStrength()
	if f.reverse {
		scans := make([]struct {
			req   roachpb.ReverseScanRequest
			union roachpb.RequestUnion_ReverseScan
		}, len(f.spans))
		for i := range f.spans {
			scans[i].req.SetSpan(f.spans[i])
			scans[i].req.ScanFormat = roachpb.BATCH_RESPONSE
			scans[i].req.KeyLocking = keyLocking
			scans[i].union.ReverseScan = &scans[i].req
			ba.Requests[i].Value = &scans[i].union
		}
	} else {
		scans := make([]struct {
			req   roachpb.ScanRequest
			union roachpb.RequestUnion_Scan
		}, len(f.spans))
		for i := range f.spans {
			scans[i].req.SetSpan(f.spans[i])
			scans[i].req.ScanFormat = roachpb.BATCH_RESPONSE
			scans[i].req.KeyLocking = keyLocking
			scans[i].union.Scan = &scans[i].req
			ba.Requests[i].Value = &scans[i].union
		}
	}
	if cap(f.requestSpans) < len(f.spans) {
		f.requestSpans = make(roachpb.Spans, len(f.spans))
	} else {
		f.requestSpans = f.requestSpans[:len(f.spans)]
	}
	copy(f.requestSpans, f.spans)

	if log.ExpensiveLogEnabled(ctx, 2) {
		var buf bytes.Buffer
		for i, span := range f.spans {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(span.String())
		}
		log.VEventf(ctx, 2, "Scan %s", buf.String())
	}

	monitoring := f.acc.Monitor() != nil

	const tokenFetchAllocation = 1 << 10
	if monitoring && f.acc.Used() < tokenFetchAllocation {
		// Pre-reserve a token fraction of the maximum amount of memory this scan
		// could return. Most of the time, scans won't use this amount of memory,
		// so it's unnecessary to reserve it all. We reserve something rather than
		// nothing at all to preserve some accounting.
		if err := f.acc.ResizeTo(ctx, tokenFetchAllocation); err != nil {
			return err
		}
	}

	// Reset spans in preparation for adding resume-spans below.
	f.spans = f.spans[:0]

	br, err := f.sendFn(ctx, ba)
	if err != nil {
		return err
	}
	if br != nil {
		f.responses = br.Responses
	} else {
		f.responses = nil
	}
	returnedBytes := int64(br.Size())
	if monitoring && (returnedBytes > maxScanResponseBytes || returnedBytes > f.acc.Used()) {
		// Resize up to the actual amount of bytes we got back from the fetch,
		// but don't ratchet down below maxScanResponseBytes if we ever exceed it.
		// We would much prefer to over-account than under-account, especially when
		// we are in a situation where we have large batches caused by parallel
		// unlimited scans (index joins and lookup joins where cols are key).
		//
		// The reason we don't want to precisely account here is to hopefully
		// protect ourselves from "slop" in our memory handling. In general, we
		// expect that all SQL operators that buffer data for longer than a single
		// call to Next do their own accounting, so theoretically, by the time
		// this fetch method is called again, all memory will either be released
		// from the system or accounted for elsewhere. In reality, though, Go's
		// garbage collector has some lag between when the memory is no longer
		// referenced and when it is freed. Also, we're not perfect with
		// accounting by any means. When we start doing large fetches, it's more
		// likely that we'll expose ourselves to OOM conditions, so that's the
		// reasoning for why we never ratchet this account down past the maximum
		// fetch size once it's exceeded.
		if err := f.acc.ResizeTo(ctx, returnedBytes); err != nil {
			return err
		}
	}

	// Set end to true until disproved.
	f.fetchEnd = true
	var sawResumeSpan bool
	for _, resp := range f.responses {
		reply := resp.GetInner()
		header := reply.Header()

		if header.NumKeys > 0 && sawResumeSpan {
			return errors.Errorf(
				"span with results after resume span; it shouldn't happen given that "+
					"we're only scanning non-overlapping spans. New spans: %s",
				catalogkeys.PrettySpans(nil, f.spans, 0 /* skip */))
		}

		if resumeSpan := header.ResumeSpan; resumeSpan != nil {
			// A span needs to be resumed.
			f.fetchEnd = false
			f.spans = append(f.spans, *resumeSpan)
			// Verify we don't receive results for any remaining spans.
			sawResumeSpan = true
		}
	}

	f.batchIdx++

	// TODO(radu): We should fetch the next chunk in the background instead of waiting for the next
	// call to fetch(). We can use a pool of workers to issue the KV ops which will also limit the
	// total number of fetches that happen in parallel (and thus the amount of resources we use).
	return nil
}

// nextBatch returns the next batch of key/value pairs. If there are none
// available, a fetch is initiated. When there are no more keys, ok is false.
// origSpan returns the span that batch was fetched from, and bounds all of the
// keys returned.
func (f *txnKVFetcher) nextBatch(
	ctx context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, origSpan roachpb.Span, err error) {
	if len(f.remainingBatches) > 0 {
		batch := f.remainingBatches[0]
		f.remainingBatches = f.remainingBatches[1:]
		return true, nil, batch, f.origSpan, nil
	}
	if len(f.responses) > 0 {
		reply := f.responses[0].GetInner()
		f.responses = f.responses[1:]
		origSpan := f.requestSpans[0]
		f.requestSpans = f.requestSpans[1:]
		var batchResp []byte
		switch t := reply.(type) {
		case *roachpb.ScanResponse:
			if len(t.BatchResponses) > 0 {
				batchResp = t.BatchResponses[0]
				f.remainingBatches = t.BatchResponses[1:]
			}
			return true, t.Rows, batchResp, origSpan, nil
		case *roachpb.ReverseScanResponse:
			if len(t.BatchResponses) > 0 {
				batchResp = t.BatchResponses[0]
				f.remainingBatches = t.BatchResponses[1:]
			}
			return true, t.Rows, batchResp, origSpan, nil
		}
	}
	if f.fetchEnd {
		return false, nil, nil, roachpb.Span{}, nil
	}
	if err := f.fetch(ctx); err != nil {
		return false, nil, nil, roachpb.Span{}, err
	}
	return f.nextBatch(ctx)
}

// close releases the resources of this txnKVFetcher.
func (f *txnKVFetcher) close(ctx context.Context) {
	f.acc.Close(ctx)
}
