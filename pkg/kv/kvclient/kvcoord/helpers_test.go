// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// asSortedSlice returns the set data in sorted order.
//
// Too inefficient for production.
func (s *condensableSpanSet) asSortedSlice() []roachpb.Span {
	set := s.asSlice()
	cpy := make(roachpb.Spans, len(set))
	copy(cpy, set)
	sort.Sort(cpy)
	return cpy
}
