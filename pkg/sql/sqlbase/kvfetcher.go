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
//
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlbase

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// kvFetcher handles retrieval of key/values.
type kvFetcher struct {
	// "Constant" fields, provided by the caller.
	txn             *client.Txn
	spans           roachpb.Spans
	reverse         bool
	useBatchLimit   bool
	firstBatchLimit int64

	// Fields used to avoid allocations.
	batch client.Batch

	batchIdx     int
	fetchEnd     bool
	kvs          []client.KeyValue
	kvIndex      int
	totalFetched int64

	// returnRangeInfo, is set, causes the kvFetcher to populate rangeInfos.
	// See also rowFetcher.returnRangeInfo.
	returnRangeInfo bool

	// As the kvFetcher fetches batches of kvs, it accumulates information on the
	// replicas where the batches came from. This info can be retrieved through
	// getRangeInfo(), to be used for updating caches.
	// rangeInfos are deduped, so they're not ordered in any particular way and
	// they don't map to kvFetcher.spans in any particular way.
	rangeInfos []roachpb.RangeInfo
}

func (f *kvFetcher) getRangesInfo() []roachpb.RangeInfo {
	if !f.returnRangeInfo {
		panic("GetRangeInfo() called on kvFetcher that wasn't configured with returnRangeInfo")
	}
	return f.rangeInfos
}

// getBatchSize returns the max size of the next batch.
func (f *kvFetcher) getBatchSize() int64 {
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
) (kvFetcher, error) {
	if firstBatchLimit < 0 || (!useBatchLimit && firstBatchLimit != 0) {
		return kvFetcher{}, errors.Errorf("invalid batch limit %d (useBatchLimit: %t)",
			firstBatchLimit, useBatchLimit)
	}

	if useBatchLimit {
		// Verify the spans are ordered if a batch limit is used.
		for i := 1; i < len(spans); i++ {
			if spans[i].Key.Compare(spans[i-1].EndKey) < 0 {
				return kvFetcher{}, errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
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

	return kvFetcher{
		txn:             txn,
		spans:           copySpans,
		reverse:         reverse,
		useBatchLimit:   useBatchLimit,
		firstBatchLimit: firstBatchLimit,
		returnRangeInfo: returnRangeInfo,
	}, nil
}

// fetch retrieves spans from the kv
func (f *kvFetcher) fetch(ctx context.Context) error {
	batchSize := f.getBatchSize()

	b := &f.batch
	*b = client.Batch{}
	b.Header.MaxSpanRequestKeys = batchSize
	b.Header.ReturnRangeInfo = f.returnRangeInfo

	for _, span := range f.spans {
		if f.reverse {
			b.ReverseScan(span.Key, span.EndKey)
		} else {
			b.Scan(span.Key, span.EndKey)
		}
	}
	// Reset spans and add resume-spans later.
	f.spans = f.spans[:0]

	if err := f.txn.Run(ctx, b); err != nil {
		return err
	}

	if f.kvs == nil {
		numResults := 0
		for _, result := range b.Results {
			numResults += len(result.Rows)
		}
		f.kvs = make([]client.KeyValue, 0, numResults)
	} else {
		f.kvs = f.kvs[:0]
	}

	// Set end to true until disproved.
	f.fetchEnd = true
	var sawResumeSpan bool
	for _, result := range b.Results {
		if result.ResumeSpan.Key != nil {
			// A span needs to be resumed.
			f.fetchEnd = false
			f.spans = append(f.spans, result.ResumeSpan)
		}
		if len(result.Rows) > 0 {
			if sawResumeSpan {
				return errors.Errorf(
					"span with results after resume span; new spans: %s",
					PrettySpans(f.spans, 0))
			}
			f.kvs = append(f.kvs, result.Rows...)
		}
		if result.ResumeSpan.Key != nil {
			// Verify we don't receive results for any remaining spans.
			sawResumeSpan = true
		}
		if f.returnRangeInfo {
			for _, ri := range result.RangeInfos {
				f.rangeInfos = roachpb.InsertRangeInfo(f.rangeInfos, ri)
			}
		}
	}

	f.batchIdx++
	f.totalFetched += int64(len(f.kvs))
	f.kvIndex = 0

	// TODO(radu): We should fetch the next chunk in the background instead of waiting for the next
	// call to fetch(). We can use a pool of workers to issue the KV ops which will also limit the
	// total number of fetches that happen in parallel (and thus the amount of resources we use).
	return nil
}

// nextKV returns the next key/value (initiating fetches as necessary). When there are no more keys,
// returns false and an empty key/value.
func (f *kvFetcher) nextKV(ctx context.Context) (bool, client.KeyValue, error) {
	if f.kvIndex == len(f.kvs) {
		if f.fetchEnd {
			return false, client.KeyValue{}, nil
		}
		err := f.fetch(ctx)
		if err != nil {
			return false, client.KeyValue{}, err
		}
		if len(f.kvs) == 0 {
			return false, client.KeyValue{}, nil
		}
	}
	f.kvIndex++
	return true, f.kvs[f.kvIndex-1], nil
}
