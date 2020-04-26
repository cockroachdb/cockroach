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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// kvBatchSize is the number of keys we request at a time.
// On a single node, 1000 was enough to avoid any performance degradation. On
// multi-node clusters, we want bigger chunks to make up for the higher latency.
// TODO(radu): parameters like this should be configurable
var kvBatchSize int64 = 10000

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
	// lockStr represents the locking mode to use when fetching KVs.
	lockStr sqlbase.ScanLockingStrength
	// returnRangeInfo, if set, causes the kvBatchFetcher to populate rangeInfos.
	// See also rowFetcher.returnRangeInfo.
	returnRangeInfo bool

	fetchEnd bool
	batchIdx int

	// requestSpans contains the spans that were requested in the last request,
	// and is one to one with responses. This field is kept separately from spans
	// so that the fetcher can keep track of which response was produced for each
	// input span.
	requestSpans roachpb.Spans
	responses    []roachpb.ResponseUnion

	// As the kvBatchFetcher fetches batches of kvs, it accumulates information on the
	// replicas where the batches came from. This info can be retrieved through
	// getRangeInfo(), to be used for updating caches.
	// rangeInfos are deduped, so they're not ordered in any particular way and
	// they don't map to kvBatchFetcher.spans in any particular way.
	rangeInfos       []roachpb.RangeInfo
	origSpan         roachpb.Span
	remainingBatches [][]byte
}

var _ kvBatchFetcher = &txnKVFetcher{}

func (f *txnKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	if !f.returnRangeInfo {
		panic(errors.AssertionFailedf("GetRangesInfo() called on kvBatchFetcher that wasn't configured with returnRangeInfo"))
	}
	return f.rangeInfos
}

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
	switch f.lockStr {
	case sqlbase.ScanLockingStrength_FOR_NONE:
		return lock.None

	case sqlbase.ScanLockingStrength_FOR_KEY_SHARE:
		// Promote to FOR_SHARE.
		fallthrough
	case sqlbase.ScanLockingStrength_FOR_SHARE:
		// We currently perform no per-key locking when FOR_SHARE is used
		// because Shared locks have not yet been implemented.
		return lock.None

	case sqlbase.ScanLockingStrength_FOR_NO_KEY_UPDATE:
		// Promote to FOR_UPDATE.
		fallthrough
	case sqlbase.ScanLockingStrength_FOR_UPDATE:
		// We currently perform exclusive per-key locking when FOR_UPDATE is
		// used because Upgrade locks have not yet been implemented.
		return lock.Exclusive

	default:
		panic(fmt.Sprintf("unknown locking strength %s", f.lockStr))
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
	lockStr sqlbase.ScanLockingStrength,
	returnRangeInfo bool,
) (txnKVFetcher, error) {
	sendFn := func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		return res, nil
	}
	return makeKVBatchFetcherWithSendFunc(
		sendFn, spans, reverse, useBatchLimit, firstBatchLimit, lockStr, returnRangeInfo,
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
	lockStr sqlbase.ScanLockingStrength,
	returnRangeInfo bool,
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
		lockStr:         lockStr,
		returnRangeInfo: returnRangeInfo,
	}, nil
}

// fetch retrieves spans from the kv
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	var ba roachpb.BatchRequest
	ba.Header.MaxSpanRequestKeys = f.getBatchSize()
	if ba.Header.MaxSpanRequestKeys > 0 {
		// If this kvfetcher limits the number of rows returned, also use
		// target bytes to guard against the case in which the average row
		// is very large.
		// If no limit is set, the assumption is that SQL *knows* that there
		// is only a "small" amount of data to be read, and wants to preserve
		// concurrency for this request inside of DistSender, which setting
		// TargetBytes would interfere with.
		ba.Header.TargetBytes = 10 * (1 << 20)
	}
	ba.Header.ReturnRangeInfo = f.returnRangeInfo
	ba.Requests = make([]roachpb.RequestUnion, len(f.spans))
	keyLocking := f.getKeyLockingStrength()
	if f.reverse {
		scans := make([]roachpb.ReverseScanRequest, len(f.spans))
		for i := range f.spans {
			scans[i].SetSpan(f.spans[i])
			scans[i].ScanFormat = roachpb.BATCH_RESPONSE
			scans[i].KeyLocking = keyLocking
			ba.Requests[i].MustSetInner(&scans[i])
		}
	} else {
		scans := make([]roachpb.ScanRequest, len(f.spans))
		for i := range f.spans {
			scans[i].SetSpan(f.spans[i])
			scans[i].ScanFormat = roachpb.BATCH_RESPONSE
			scans[i].KeyLocking = keyLocking
			ba.Requests[i].MustSetInner(&scans[i])
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
				sqlbase.PrettySpans(nil, f.spans, 0 /* skip */))
		}

		if resumeSpan := header.ResumeSpan; resumeSpan != nil {
			// A span needs to be resumed.
			f.fetchEnd = false
			f.spans = append(f.spans, *resumeSpan)
			// Verify we don't receive results for any remaining spans.
			sawResumeSpan = true
		}

		// Fill up the RangeInfos, in case we got any.
		if f.returnRangeInfo {
			for _, ri := range header.RangeInfos {
				f.rangeInfos = roachpb.InsertRangeInfo(f.rangeInfos, ri)
			}
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
