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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type lastWrite struct {
	roachpb.Span
	idx int
	ts  hlc.Timestamp
}

type frontier []lastWrite // non-overlapping sorted spans

// `g` are the elements of `f` that survived the insertion (i.e. weren't entirely
// replaced by lw). Together with `delta` (the this gives the resulting
// frontier (after sorting).
func (f frontier) addInternal(lw lastWrite) (g, delta frontier) {

	/*
		var g frontier
		// The effects of `lw`.
		var delta frontier

		_ = g
		_ = delta

		defer func() {
			sort.Slice(g, func(i, j int) bool {
				return keys.MustAddr(g[i].Span.Key).Less(keys.MustAddr(g[j].Key))
			})
		}()
		for _, ex := range f {
			if len(lw.Span.EndKey) == 0 {
				// A previous iteration already consumed all of `lw`, so just have to add
				// `f` back to `g`.
				g = append(g, ex)
				continue
			}

			// This span is in the frontier and may now (partially or entirely) be
			// replaced by (part of) lw.
			//
			// Example 1:
			//
			// [----------)     ex
			//    [---)         lw
			//
			// gives:
			//
			// [--)   [---)     keep
			//    [---)         insert
			//     empty        todo
			//
			// Example 2:
			// [-----)          ex
			//    [-------)     lw
			//
			// gives:
			//
			// [--)             keep
			//    [--)          insert
			//       [----)     todo
			//
			// In particular `todo` and `insert` are either zero or one spans,
			// and `todo` is up to two spans.
			var keep roachpb.SpanGroup
			keep.Add(ex.Span)
			keep.Sub(lw.Span)
			var insert roachpb.SpanGroup
			insert.Add(ex.Span)
			insert.Sub(keep.Slice()...)
			if sl := insert.Slice(); len(sl) != 1 {
				panic(fmt.Sprintf("expected to insert one slice: %+v", sl))
			}
			var todo roachpb.SpanGroup
			todo.Add(lw.Span)
			todo.Sub(insert.Slice()...)
			for _, sp := range keep.Slice() {
				tmp := ex
				tmp.Span = sp
				g = append(g, tmp)
			}
			for _, sp := range insert.Slice() {
				tmp := lw
				tmp.Span = sp
				g = append(g, tmp)
			}
			todoSl := todo.Slice()
			if len(todoSl) == 0 {
				lw.Span = roachpb.Span{}
				// Keep going to add subsequent items of `f` back to `g`.
				continue
			}
			if len(todoSl) == 2 {
				// A part of `todo` is to the left of the current span in the existing
				// frontier. Because we visit those in ascending order, we can blindly
				// insert `lw` into `todoSl[0]` since we know it doesn't overlap anything
				// (or previous iteration would've handled it already).
				tmp := lw
				tmp.Span = todoSl[0]
				todoSl = todoSl[1:]
				g = append(g, tmp)
			}
			lw.Span = todoSl[0]
		}
		if len(lw.EndKey) != 0 {
			// Whatever is left of `lw` can blindly be inserted since it doesn't
			// overlap `f`.
			g = append(g, lw)
		}
		return g

	*/
	return nil, nil
}
