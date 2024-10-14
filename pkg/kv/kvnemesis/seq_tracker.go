// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvnemesis

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

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

func (tr *SeqTracker) String() string {
	tr.Lock()
	defer tr.Unlock()

	var sl []keyTS
	for k := range tr.seen {
		sl = append(sl, k)
	}
	slices.SortFunc(sl, func(a, b keyTS) int {
		return cmp.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	})

	var buf strings.Builder
	for _, el := range sl {
		fmt.Fprintf(&buf, "%s %s -> %s\n", roachpb.Span{Key: roachpb.Key(el.key), EndKey: roachpb.Key(el.endKey)}, el.ts, tr.seen[el])
	}
	return buf.String()
}

// Add associates key@ts with the provided Seq. It is called in two places:
//
//   - For regular point/range key writes, we hook into the server-side rangefeed
//     processor via RangefeedValueHeaderFilter and read it there.
//
//   - For AddSSTables, we scan the emitted SST in the rangefeed watcher,
//     which contains the embedded seqnos.
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
	// Rangedels can be split, but the tracker will always see the pre-split
	// value (since it's reported by the operation's BatchRequest). So this
	// method checks whether the input span is contained in any span seen
	// by the tracker.
	if seq, fastPathOK := tr.seen[keyTS{
		key:    string(key),
		endKey: string(endKey),
		ts:     ts,
	}]; fastPathOK {
		// Fast path - exact match. Should be the common case outside of MVCC range
		// deletions.
		return seq, true
	}

	for kts := range tr.seen {
		if kts.ts != ts {
			continue
		}
		cur := roachpb.Span{Key: roachpb.Key(kts.key), EndKey: roachpb.Key(kts.endKey)}
		if cur.Contains(roachpb.Span{Key: key, EndKey: endKey}) {
			return tr.seen[kts], true
		}
	}
	return 0, false
}
