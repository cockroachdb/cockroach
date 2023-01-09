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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type tc struct {
		// Inputs.
		name string
		opts SelectionOptions

		// Outputs.
		s Selection
	}

	var tcs []tc

	for _, span := range []roachpb.RSpan{
		{},
		{
			Key:    roachpb.RKey("a"),
			EndKey: roachpb.RKey("c"),
		},
	} {
		for _, replicatedByRangeID := range []bool{false, true} {
			for _, unreplicatedByRangeID := range []bool{false, true} {
				opts := SelectionOptions{
					StateMachineSelectionOptions: StateMachineSelectionOptions{
						ReplicatedBySpan:    span,
						ReplicatedByRangeID: replicatedByRangeID,
					},
					UnreplicatedByRangeID: unreplicatedByRangeID,
				}
				var name []string
				if replicatedByRangeID {
					name = append(name, "replicatedid")
				}
				if unreplicatedByRangeID {
					name = append(name, "unreplicatedid")
				}
				if !opts.ReplicatedBySpan.Equal(roachpb.RSpan{}) {
					name = append(name, "span")
				}
				if len(name) == 0 {
					name = append(name, "zero")
				}
				tcs = append(tcs, tc{
					name: strings.Join(name, "_"),
					opts: opts,
					s:    Select(roachpb.RangeID(123), opts),
				})
			}
		}
	}

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))

	for _, tc := range tcs {
		t.Run(tc.name, w.Run(t, tc.name, func(t *testing.T) string {
			var buf strings.Builder
			fmt.Fprintln(&buf, "Spans():")
			for _, sp := range tc.s.Spans() {
				fmt.Fprintf(&buf, "- %s\n", sp)
			}
			fmt.Fprintln(&buf, "StateMachineSpans():")
			for _, sp := range tc.s.StateMachineSpans() {
				fmt.Fprintf(&buf, "- %s\n", sp)
			}
			fmt.Fprintln(&buf, "NonStateMachineSpans():")
			for _, sp := range tc.s.NonStateMachineSpans() {
				fmt.Fprintf(&buf, "- %s\n", sp)
			}
			return buf.String()
		}))
	}
}
