// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// PrettyKey pretty-prints the specified key, skipping over the first `skip`
// fields. The pretty printed key looks like:
//
//   /Table/<tableID>/<indexID>/...
//
// We always strip off the /Table prefix and then `skip` more fields. Note that
// this assumes that the fields themselves do not contain '/', but that is
// currently true for the fields we care about stripping (the table and index
// ID).
func PrettyKey(key roachpb.Key, skip int) string {
	p := key.String()
	for i := 0; i <= skip; i++ {
		n := strings.IndexByte(p[1:], '/')
		if n == -1 {
			return ""
		}
		p = p[n+1:]
	}
	return p
}

// PrettySpan returns a human-readable representation of a span.
func PrettySpan(span roachpb.Span, skip int) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s-%s", PrettyKey(span.Key, skip), PrettyKey(span.EndKey, skip))
	return buf.String()
}

// PrettySpans returns a human-readable description of the spans.
func PrettySpans(spans []roachpb.Span, skip int) string {
	var buf bytes.Buffer
	for i, span := range spans {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(PrettySpan(span, skip))
	}
	return buf.String()
}

// kvBatchSize is the number of keys we request at a time.
// On a single node, 1000 was enough to avoid any performance degradation. On
// multi-node clusters, we want bigger chunks to make up for the higher latency.
// TODO(radu): parameters like this should be configurable
var kvBatchSize int64 = 10000

// SetKVBatchSize changes the kvFetcher batch size, and returns a function that restores it.
func SetKVBatchSize(val int64) func() {
	oldVal := kvBatchSize
	kvBatchSize = val
	return func() { kvBatchSize = oldVal }
}

// txnKVFetcher handles retrieval of key/values.
type txnKVFetcher struct {
	// "Constant" fields, provided by the caller.
	txn             *client.Txn
	spans           roachpb.Spans
	firstBatchLimit int64
	useBatchLimit   bool
	reverse         bool
	// returnRangeInfo, if set, causes the kvFetcher to populate rangeInfos.
	// See also rowFetcher.returnRangeInfo.
	returnRangeInfo bool

	fetchEnd  bool
	batchIdx  int
	responses []roachpb.ResponseUnion
	kvs       []roachpb.KeyValue

	// As the kvFetcher fetches batches of kvs, it accumulates information on the
	// replicas where the batches came from. This info can be retrieved through
	// getRangeInfo(), to be used for updating caches.
	// rangeInfos are deduped, so they're not ordered in any particular way and
	// they don't map to kvFetcher.spans in any particular way.
	rangeInfos []roachpb.RangeInfo
}

func (f *txnKVFetcher) getRangesInfo() []roachpb.RangeInfo {
	if !f.returnRangeInfo {
		panic("GetRangeInfo() called on kvFetcher that wasn't configured with returnRangeInfo")
	}
	return f.rangeInfos
}

// getBatchSize returns the max size of the next batch.
func (f *txnKVFetcher) getBatchSize() int64 {
	if !f.useBatchLimit {
		return 0
	}
	if f.firstBatchLimit == 0 || f.firstBatchLimit >= kvBatchSize {
		return kvBatchSize
	}

	// We grab the first batch according to the limit. If it turns out that we
	// need another batch, we grab a bigger batch. If that's still not enough,
	// we revert to the default batch size.
	switch f.batchIdx {
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

// makeKVFetcher initializes a kvFetcher for the given spans.
//
// If useBatchLimit is true, batches are limited to kvBatchSize. If
// firstBatchLimit is also set, the first batch is limited to that value.
// Subsequent batches are larger, up to kvBatchSize.
//
// Batch limits can only be used if the spans are ordered.
func makeKVFetcher(
	txn *client.Txn,
	spans roachpb.Spans,
	reverse bool,
	useBatchLimit bool,
	firstBatchLimit int64,
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
		txn:             txn,
		spans:           copySpans,
		reverse:         reverse,
		useBatchLimit:   useBatchLimit,
		firstBatchLimit: firstBatchLimit,
		returnRangeInfo: returnRangeInfo,
	}, nil
}

// fetch retrieves spans from the kv
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	var ba roachpb.BatchRequest
	ba.Header.MaxSpanRequestKeys = f.getBatchSize()
	ba.Header.ReturnRangeInfo = f.returnRangeInfo
	ba.Requests = make([]roachpb.RequestUnion, len(f.spans))
	if f.reverse {
		scans := make([]roachpb.ReverseScanRequest, len(f.spans))
		for i := range f.spans {
			scans[i].Span = f.spans[i]
			ba.Requests[i].MustSetInner(&scans[i])
		}
	} else {
		scans := make([]roachpb.ScanRequest, len(f.spans))
		for i := range f.spans {
			scans[i].Span = f.spans[i]
			ba.Requests[i].MustSetInner(&scans[i])
		}
	}

	if log.ExpensiveLogEnabled(ctx, 2) {
		buf := bytes.NewBufferString("Scan ")
		for i, span := range f.spans {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(span.String())
		}
		log.VEvent(ctx, 2, buf.String())
	}

	// Reset spans in preparation for adding resume-spans below.
	f.spans = f.spans[:0]

	br, err := f.txn.Send(ctx, ba)
	if err != nil {
		return err.GoError()
	}
	f.responses = br.Responses

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
				PrettySpans(f.spans, 0 /* skip */))
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

// nextKV returns the next key/value (initiating fetches as necessary). When
// there are no more keys, returns false and an empty key/value.
func (f *txnKVFetcher) nextKV(ctx context.Context) (bool, roachpb.KeyValue, error) {
	var kv roachpb.KeyValue
	for {
		for len(f.kvs) == 0 && len(f.responses) > 0 {
			reply := f.responses[0].GetInner()
			f.responses = f.responses[1:]

			switch t := reply.(type) {
			case *roachpb.ScanResponse:
				f.kvs = t.Rows
			case *roachpb.ReverseScanResponse:
				f.kvs = t.Rows
			}
		}

		if len(f.kvs) > 0 {
			kv = f.kvs[0]
			f.kvs = f.kvs[1:]
			return true, kv, nil
		}
		if f.fetchEnd {
			return false, kv, nil
		}
		if err := f.fetch(ctx); err != nil {
			return false, kv, err
		}
	}
}
