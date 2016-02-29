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

// kvFetcher handles retrieval of key/values.
type kvFetcher struct {
	// "Constant" fields, provided by the caller.
	txn     *client.Txn
	spans   spans
	reverse bool

	fetched bool
	kvs     []client.KeyValue
	kvIndex int
}

func makeKVFetcher(txn *client.Txn, spans spans, reverse bool) kvFetcher {
	return kvFetcher{txn: txn, spans: spans, reverse: reverse}
}

// fetchKVs fetches all spans from the kv
//
// TODO(radu): The key-value scan currently reads all of the key-value
// pairs, but they could just as easily be read in chunks. Probably worthwhile
// to separate out the retrieval of the key-value pairs into a separate
// structure.
func (f *kvFetcher) fetch() *roachpb.Error {
	// Retrieve all the spans.
	b := &client.Batch{}
	if f.reverse {
		for i := len(f.spans) - 1; i >= 0; i-- {
			b.ReverseScan(f.spans[i].start, f.spans[i].end, f.spans[i].count)
		}
	} else {
		for i := 0; i < len(f.spans); i++ {
			b.Scan(f.spans[i].start, f.spans[i].end, f.spans[i].count)
		}
	}
	if pErr := f.txn.Run(b); pErr != nil {
		return pErr
	}

	for _, result := range b.Results {
		if f.kvs == nil {
			f.kvs = result.Rows
		} else {
			f.kvs = append(f.kvs, result.Rows...)
		}
	}
	f.fetched = true
	return nil
}

// nextKey returns the next key (initiating scan operations as needed). When there are no more keys,
// returns false and an empty key/value.
func (f *kvFetcher) nextKey() (bool, client.KeyValue, *roachpb.Error) {
	if !f.fetched {
		pErr := f.fetch()
		if pErr != nil {
			return false, client.KeyValue{}, pErr
		}
	}
	if f.kvIndex == len(f.kvs) {
		return false, client.KeyValue{}, nil
	}
	f.kvIndex++
	return true, f.kvs[f.kvIndex-1], nil
}
