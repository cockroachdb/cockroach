// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvnemesis

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SeqTracker is a container that helps kvnemesis map MVCC versions to
// operations as identified by their Seq.
//
// SeqTracker is threadsafe.
type SeqTracker struct {
	syncutil.Mutex
	seen map[keyTS]kvnemesisutil.Seq
}

type keyTS struct {
	key, endKey string
	ts          hlc.Timestamp
}

// Add associates key@ts with the provided Seq.
func (tr *SeqTracker) Add(key, endKey roachpb.Key, ts hlc.Timestamp, seq kvnemesisutil.Seq) {
	tr.Lock()
	defer tr.Unlock()

	if tr.seen == nil {
		tr.seen = map[keyTS]kvnemesisutil.Seq{}
	}

	tr.seen[keyTS{key: string(key), endKey: string(endKey), ts: ts}] = seq
}

// Lookup checks whether the version key@ts is associated with a Seq.
func (tr *SeqTracker) Lookup(key, endKey roachpb.Key, ts hlc.Timestamp) (kvnemesisutil.Seq, bool) {
	tr.Lock()
	defer tr.Unlock()
	seq, ok := tr.seen[keyTS{key: string(key), endKey: string(endKey), ts: ts}]
	return seq, ok
}
