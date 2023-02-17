// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rditer

import (
	"bytes"
	"fmt"
	"sort"
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
		name string
		sp   roachpb.RSpan
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
		},
		{
			name: "r2",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKey("c"),
			},
		},
		{
			name: "r3",
			sp: roachpb.RSpan{
				Key:    roachpb.RKey("a"),
				EndKey: roachpb.RKeyMax,
			},
		},
	} {
		t.Run(tc.name, w.Run(t, tc.name, func(t *testing.T) string {
			var buf strings.Builder
			for _, replicatedByRangeID := range []bool{false, true} {
				for _, unreplicatedByRangeID := range []bool{false, true} {
					opts := SelectOpts{
						ReplicatedBySpan:      tc.sp,
						ReplicatedByRangeID:   replicatedByRangeID,
						UnreplicatedByRangeID: unreplicatedByRangeID,
					}
					fmt.Fprintf(&buf, "Select(%+v):\n", opts)
					sl := Select(roachpb.RangeID(123), opts)
					assert.True(t, sort.SliceIsSorted(sl, func(i, j int) bool {
						return bytes.Compare(sl[i].EndKey, sl[j].Key) < 0
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
