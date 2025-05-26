// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rditer

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for _, tc := range []struct {
		name   string
		sp     roachpb.RSpan
		filter SelectRangedOptions
	}{
		{
			name: "no_span",
		},
		{
			name: "r1",
			sp: roachpb.RSpan{
				// r1 is special - see https://github.com/cockroachdb/cockroach/issues/95055.
				Key:    roachpb.RKeyMin,
				EndKey: roachpb.RKey("c"),
			},
			filter: SelectRangedOptions{
				SystemKeys: true,
				LockTable:  true,
				UserKeys:   true,
			},
		},
		{
			name: "r2",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKey("c"),
			},
			filter: SelectRangedOptions{
				SystemKeys: true,
				LockTable:  true,
				UserKeys:   true,
			},
		},
		{
			name: "r2_excludeuser",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKey("c"),
			},
			filter: SelectRangedOptions{
				SystemKeys: true,
				LockTable:  true,
				UserKeys:   false,
			},
		},
		{
			name: "r2_useronly",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKey("c"),
			},
			filter: SelectRangedOptions{
				SystemKeys: false,
				LockTable:  false,
				UserKeys:   true,
			},
		},
		{
			name: "r2_excludelocks",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKey("c"),
			},
			filter: SelectRangedOptions{
				SystemKeys: true,
				LockTable:  false,
				UserKeys:   true,
			},
		},
		{
			name: "r2_locksonly",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKey("c"),
			},
			filter: SelectRangedOptions{
				SystemKeys: false,
				LockTable:  true,
				UserKeys:   false,
			},
		},
		{
			name: "r3",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKeyMax,
			},
			filter: SelectRangedOptions{
				SystemKeys: true,
				LockTable:  true,
				UserKeys:   true,
			},
		},
	} {
		t.Run(tc.name, w.Run(t, tc.name, func(t *testing.T) string {
			var buf strings.Builder
			for _, replicatedByRangeID := range []bool{false, true} {
				for _, unreplicatedByRangeID := range []bool{false, true} {
					ranged := tc.filter
					ranged.Span = tc.sp
					opts := SelectOpts{
						Ranged:                ranged,
						ReplicatedByRangeID:   replicatedByRangeID,
						UnreplicatedByRangeID: unreplicatedByRangeID,
					}
					fmt.Fprintf(&buf, "Select(%+v):\n", opts)
					sl := Select(roachpb.RangeID(123), opts)
					assert.True(t, slices.IsSortedFunc(sl, func(a, b roachpb.Span) int {
						return bytes.Compare(a.EndKey, b.Key)
					}))
					for _, sp := range sl {
						fmt.Fprintf(&buf, "  %s\n", sp)
					}
				}
			}
			return buf.String()
		}))
	}
}
