// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"iter"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
)

// Transactions is a go iterator. It takes a sorted []streampb.StreamEvent_KV batch
// and yields sub-slices grouped by MVCC timestamp. Each sub-slice contains all
// KVs that share the same value timestamp, representing a single transaction's
// write set. The input batch must be sorted by value timestamp.
func Transactions(batch []streampb.StreamEvent_KV) iter.Seq[[]streampb.StreamEvent_KV] {
	// See https://pkg.go.dev/iter
	return func(yield func([]streampb.StreamEvent_KV) bool) {
		for len(batch) != 0 {
			ts := batch[0].KeyValue.Value.Timestamp
			end := 1
			for end < len(batch) && batch[end].KeyValue.Value.Timestamp == ts {
				end++
			}
			if !yield(batch[:end]) {
				return
			}
			batch = batch[end:]
		}
	}
}
