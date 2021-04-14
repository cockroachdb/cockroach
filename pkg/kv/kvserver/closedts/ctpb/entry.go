// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctpb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Epoch is an int64 with its own type to avoid mix-ups in positional arguments.
type Epoch int64

// LAI is an int64 denoting a lease applied index with its own type to avoid
// mix-ups in positional arguments.
type LAI int64

// SafeValue implements the redact.SafeValue interface.
func (LAI) SafeValue() {}

// String formats Entry for human consumption as well as testing (by avoiding
// randomness in the output caused by map iteraton order).
func (e Entry) String() string {
	rangeIDs := make([]roachpb.RangeID, 0, len(e.MLAI))
	for k := range e.MLAI {
		rangeIDs = append(rangeIDs, k)
	}

	sort.Slice(rangeIDs, func(i, j int) bool {
		a, b := rangeIDs[i], rangeIDs[j]
		if a == b {
			return e.MLAI[a] < e.MLAI[b]
		}
		return a < b
	})
	sl := make([]string, 0, len(rangeIDs))
	for _, rangeID := range rangeIDs {
		sl = append(sl, fmt.Sprintf("r%d: %d", rangeID, e.MLAI[rangeID]))
	}
	if len(sl) == 0 {
		sl = []string{"(empty)"}
	}
	return fmt.Sprintf("CT: %s @ Epoch %d\nFull: %t\nMLAI: %s\n", e.ClosedTimestamp, e.Epoch, e.Full, strings.Join(sl, ", "))
}

func (r Reaction) String() string {
	return fmt.Sprintf("Refresh: %v", r.Requested)
}
