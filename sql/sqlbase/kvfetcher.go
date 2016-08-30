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

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
)

// prettyKey pretty-prints the specified key, skipping over the first `skip`
// fields. The pretty printed key looks like:
//
//   /Table/<tableID>/<indexID>/...
//
// We always strip off the /Table prefix and then `skip` more fields. Note that
// this assumes that the fields themselves do not contain '/', but that is
// currently true for the fields we care about stripping (the table and index
// ID).
func prettyKey(key roachpb.Key, skip int) string {
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
	fmt.Fprintf(&buf, "%s-%s", prettyKey(span.Key, skip), prettyKey(span.EndKey, skip))
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
// On a single node, 1000 was enough to avoid any performance degradation. On multi-node clusters,
// we want bigger chunks to make up for the higher latency.
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
	firstBatchLimit int64

	batchIdx     int
	fetchEnd     bool
	kvs          []client.KeyValue
	kvIndex      int
	totalFetched int64
}

// getBatchSize returns the max size of the next batch.
func (f *kvFetcher) getBatchSize() int64 {
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

// makeKVFetcher initializes a kvFetcher for the given spans. If non-zero,
// firstBatchLimit limits the size of the first batch (subsequent batches use
// the default size). When a batch limit is used, the spans supplied should
// not overlap each other or be out of increasing order, or else, multiple
// partial responses can be returned from the KV store.
func makeKVFetcher(
	txn *client.Txn, spans roachpb.Spans, reverse bool, firstBatchLimit int64,
) kvFetcher {
	if firstBatchLimit < 0 {
		panic(fmt.Sprintf("invalid batch limit %d", firstBatchLimit))
	}
	// Make a copy of the spans because we update them.
	copySpans := make(roachpb.Spans, len(spans))
	for i := range spans {
		if reverse {
			copySpans[len(spans)-i-1] = spans[i]
		} else {
			copySpans[i] = spans[i]
		}
	}
	return kvFetcher{txn: txn, spans: copySpans, reverse: reverse, firstBatchLimit: firstBatchLimit}
}

// fetch retrieves spans from the kv
func (f *kvFetcher) fetch() error {
	batchSize := f.getBatchSize()

	b := &client.Batch{}
	b.Header.MaxSpanRequestKeys = batchSize

	for _, span := range f.spans {
		if f.reverse {
			b.ReverseScan(span.Key, span.EndKey)
		} else {
			b.Scan(span.Key, span.EndKey)
		}
	}
	// Reset spans and add resume-spans later.
	f.spans = f.spans[:0]

	if err := f.txn.Run(b); err != nil {
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
	var readPartialResponse bool
	for _, result := range b.Results {
		if result.ResumeSpan.Key != nil {
			// A span needs to be resumed.
			f.fetchEnd = false
			f.spans = append(f.spans, result.ResumeSpan)
		}
		if len(result.Rows) > 0 {
			if readPartialResponse {
				return errors.Errorf(
					"read partial responses for more than one span, new spans: %s",
					PrettySpans(f.spans, 0))
			}
			f.kvs = append(f.kvs, result.Rows...)
			if result.ResumeSpan.Key != nil {
				// There are more keys to be read from this span, so ensure
				// that the code is not appending keys from another span until
				// this span has been completely read.
				readPartialResponse = true
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
func (f *kvFetcher) nextKV() (bool, client.KeyValue, error) {
	if f.kvIndex == len(f.kvs) {
		if f.fetchEnd {
			return false, client.KeyValue{}, nil
		}
		err := f.fetch()
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
