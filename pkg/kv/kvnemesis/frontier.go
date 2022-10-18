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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type lastWrite struct {
	roachpb.Span
	idx int
	ts  hlc.Timestamp
}

type frontier []lastWrite // non-overlapping sorted spans

func (f frontier) Add(w lastWrite) frontier {
	// Find the first element of the frontier overlapping `w`. Recall that the
	// frontier has no self-overlap and elements are sorted in ascending order.
	idx := sort.Search(len(f), func(i int) bool {
		return f[i].Span.EndKey.Compare(w.Span.Key) > 0 &&
			f[i].Span.Key.Compare(w.Span.EndKey) < 0
	})
	if idx >= len(f) {
		// The entire frontier is strictly to the left of `w` so
		// we can just append `w`.
		return append(f, w)
	}
	var g frontier
	for i := 0; i < idx; i++ {
		g = append(g, f[:idx]...)
	}
	// A span overlaps `w`. It may extend to the left, or to the right, or be fully contained in `w`.
	if f[idx].Key.Compare(w.Key) < 0 {
		// Current element extends to the left of `w`.
		//    [------...   cur
		//       [---...   w
		// Add an element representing only the "outside" part.
		tmp := f[idx]
		tmp.EndKey = w.Key
		g = append(g, tmp)
	}
	// Next is `w`, though note that it may overlap subsequent elements, which
	// will be elided/truncated in subsequent loop iterations below.
	g = append(g, w)

	for ; idx < len(f); idx++ {
		if f[idx].EndKey.Compare(w.EndKey) <= 0 {
			// f[idx] is covered by `w` so elide it.
			continue
		}
		if f[idx].Key.Compare(w.EndKey) >= 0 {
			// f[idx] is entirely to the right of `w` so it gets in verbatim.
			g = append(g, f[idx])
			continue
		}
		if f[idx].EndKey.Compare(w.EndKey) > 0 {
			// Current element overlaps `w` and extends to the right. This can
			// happen only once, though we don't use that.
			//  ...-------)    cur
			//  ...----)       w
			// Add an element representing only the "outside" part.
			tmp := f[idx]
			tmp.Key = w.EndKey
		}
		panic("unreachable")
	}
	return g
}

func (f frontier) String() string {
	// Flatten all start and end keys in a slice, sort them, and
	// assign them indexes (used for printing pretty pictures).
	//
	// We don't use the invariant that the frontier consists of
	// self-nonoverlapping sorted elements here because it's nice
	// to be able to nicely stringify any set of spans.
	var ks []string
	for _, w := range f {
		ks = append(ks, string(w.Key))
		if len(w.EndKey) > 0 {
			ks = append(ks, string(w.EndKey))
		}
	}
	sort.Strings(ks)

	k2indent := map[string]int{} // key to indent
	var indent int
	for _, k := range ks {
		if _, ok := k2indent[k]; !ok {
			indent += len(k)
			k2indent[k] = indent
			indent += 1
		}
	}
	indent += 3

	var buf strings.Builder
	for _, w := range f {
		k := string(w.Key)
		ek := string(w.EndKey)
		pk := k2indent[k]
		var pek int
		if ek != "" {
			pek = k2indent[ek] - pk
		}
		pr := indent - pk - pek
		fmt.Fprintf(&buf, "%"+strconv.Itoa(pk)+"s", k)
		if ek != "" {
			fmt.Fprintf(&buf, "%s%s", strings.Repeat("-", pek-len(ek)), ek)
		}
		fmt.Fprintf(&buf, "%"+strconv.Itoa(pr)+"s", "") // just pad
		fmt.Fprintf(&buf, "<-- ts=%d\n", w.ts.WallTime)
	}

	return buf.String()
}
