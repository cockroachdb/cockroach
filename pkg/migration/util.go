// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// identical returns whether or not two lists of node IDs are identical as sets.
func identical(a, b []roachpb.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i] < b[j]
	})

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
