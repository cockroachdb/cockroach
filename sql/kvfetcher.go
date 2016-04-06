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

package sql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

type span struct {
	start roachpb.Key // inclusive key
	end   roachpb.Key // exclusive key
	count int64       // max # of keys for this span
}

type spans []span

// implement Sort.Interface
func (a spans) Len() int           { return len(a) }
func (a spans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a spans) Less(i, j int) bool { return a[i].start.Compare(a[j].start) < 0 }

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

func prettyDatums(vals []parser.Datum) string {
	var buf bytes.Buffer
	for _, v := range vals {
		fmt.Fprintf(&buf, "/%v", v)
	}
	return buf.String()
}

func prettySpan(span span, skip int) string {
	var buf bytes.Buffer
	if span.count != 0 {
		fmt.Fprintf(&buf, "%d:", span.count)
	}
	fmt.Fprintf(&buf, "%s-%s", prettyKey(span.start, skip), prettyKey(span.end, skip))
	return buf.String()
}

func prettySpans(spans []span, skip int) string {
	var buf bytes.Buffer
	for i, span := range spans {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(prettySpan(span, skip))
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
	spans           spans
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

// makeKVFetcher initializes a kvFetcher for the given spans. If non-zero, firstBatchLimit limits
// the size of the first batch (subsequent batches use the default size).
func makeKVFetcher(txn *client.Txn, spans spans, reverse bool, firstBatchLimit int64) kvFetcher {
	if firstBatchLimit < 0 {
		panic(fmt.Sprintf("invalid batch limit %d", firstBatchLimit))
	}
	return kvFetcher{txn: txn, spans: spans, reverse: reverse, firstBatchLimit: firstBatchLimit}
}

// fetch retrieves spans from the kv
func (f *kvFetcher) fetch() *roachpb.Error {
	batchSize := f.getBatchSize()

	b := &client.Batch{MaxScanResults: batchSize}

	var resumeKey roachpb.Key
	if len(f.kvs) > 0 {
		resumeKey = f.kvs[len(f.kvs)-1].Key
		// To resume forward scans we will set the (inclusive) scan start to the Next of the last
		// received key. To resume reverse scans we will set the (exclusive) scan end to the last
		// received key.
		if !f.reverse {
			resumeKey = resumeKey.Next()
		}
	}

	atEnd := true
	if !f.reverse {
		for i := 0; i < len(f.spans); i++ {
			start := f.spans[i].start
			if resumeKey != nil {
				if resumeKey.Compare(f.spans[i].end) >= 0 {
					// We are resuming from a key after this span.
					continue
				}
				if resumeKey.Compare(start) > 0 {
					// We are resuming from a key inside this span.
					// In this case we should technically reduce the max count for the span; but
					// since this count is only an optimization it's not incorrect to retrieve more
					// keys for the span.
					start = resumeKey
				}
			}
			atEnd = false
			b.Scan(start, f.spans[i].end, f.spans[i].count)
		}
	} else {
		for i := len(f.spans) - 1; i >= 0; i-- {
			end := f.spans[i].end
			if resumeKey != nil {
				if resumeKey.Compare(f.spans[i].start) <= 0 {
					// We are resuming from a key before this span.
					continue
				}
				if resumeKey.Compare(end) < 0 {
					// We are resuming from a key inside this span.
					// In this case we should technically reduce the max count for the span; but
					// since this count is only an optimization it's not incorrect to retrieve more
					// keys for the span.
					end = resumeKey
				}
			}
			atEnd = false
			b.ReverseScan(f.spans[i].start, end, f.spans[i].count)
		}
	}

	if atEnd {
		// The last scan happened to finish just at the end of the last span.
		f.kvs = nil
		f.fetchEnd = true
		return nil
	}

	if pErr := f.txn.Run(b); pErr != nil {
		return pErr
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

	for _, result := range b.Results {
		f.kvs = append(f.kvs, result.Rows...)
	}

	f.batchIdx++
	f.totalFetched += int64(len(f.kvs))
	f.kvIndex = 0

	if int64(len(f.kvs)) < batchSize {
		f.fetchEnd = true
	}

	// TODO(radu): We should fetch the next chunk in the background instead of waiting for the next
	// call to fetch(). We can use a pool of workers to issue the KV ops which will also limit the
	// total number of fetches that happen in parallel (and thus the amount of resources we use).
	return nil
}

// nextKV returns the next key/value (initiating fetches as necessary). When there are no more keys,
// returns false and an empty key/value.
func (f *kvFetcher) nextKV() (bool, client.KeyValue, *roachpb.Error) {
	if f.kvIndex == len(f.kvs) {
		if f.fetchEnd {
			return false, client.KeyValue{}, nil
		}
		pErr := f.fetch()
		if pErr != nil {
			return false, client.KeyValue{}, pErr
		}
		if len(f.kvs) == 0 {
			return false, client.KeyValue{}, nil
		}
	}
	f.kvIndex++
	return true, f.kvs[f.kvIndex-1], nil
}
