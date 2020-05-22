// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package minprop

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func Example() {
	ctx := context.Background()

	tracker := NewTracker()
	const ep1 ctpb.Epoch = 1
	fmt.Println("The newly initialized tracker has a zero closed timestamp:")
	fmt.Println(tracker)

	fmt.Println("A first command arrives on range 12 (though the range isn't known yet to the Tracker).")
	ts, done1 := tracker.Track(ctx)
	fmt.Println("All commands initially start out on the right. The command has its timestamp forwarded to", ts, ".")
	fmt.Println(tracker)

	fmt.Println("Two more commands arrive, on r1 and r12.")
	_, done2 := tracker.Track(ctx)
	_, done3 := tracker.Track(ctx)
	fmt.Println(tracker)

	fmt.Println("The command on r1 finishes evaluating at Lease Applied Index 10 and lets the Tracker know.")
	done2(ctx, ep1, 1, 10)
	fmt.Println(tracker)

	fmt.Println("The command on r12 also finishes quickly, at LAI 77.")
	done3(ctx, ep1, 12, 77)
	fmt.Println(tracker)

	fmt.Println("The system closes out a timestamp (registering 1000 as the next timestamp to close out).")
	closed1, mlai1, _ := tracker.Close(hlc.Timestamp{WallTime: 1e9}, ep1)
	fmt.Println("No problem: nothing is tracked on the left side; returns:", closed1, "and", mlaiString(mlai1))
	fmt.Println("Note how the items on the right have moved to the left, as they are relevant for the")
	fmt.Println("next call to Close.")
	fmt.Println(tracker)

	fmt.Println("Nothing happens for a while until the system tries to close out the next timestamp.")
	fmt.Println("However, the very first proposal is still tracked and blocks progress.")
	closed2, mlai2, _ := tracker.Close(hlc.Timestamp{WallTime: 2e9}, ep1)
	fmt.Println("The call returns a no-op in the form", closed2, mlaiString(mlai2), ".")
	fmt.Println(tracker)

	ts4, done4 := tracker.Track(ctx)
	fmt.Println("A new command gets tracked on r12 (and is forwarded to", ts4, "(if necessary).")
	fmt.Println("It terminates quickly, leaving an MLAI entry of 78 behind.")
	done4(ctx, ep1, 12, 78)
	fmt.Println(tracker)

	fmt.Println("Finally! The slow evaluation finishes and the command gets proposed at index 79.")
	fmt.Println("Note that the right now tracks a smaller value of 78. Consumers have to keep the")
	fmt.Println("maximum they've seen.")
	done1(ctx, ep1, 12, 79)
	fmt.Println(tracker)

	closed3, mlai3, _ := tracker.Close(hlc.Timestamp{WallTime: 3e9}, ep1)
	fmt.Println("The next call to Close() is successful and returns:", closed3, "and", mlaiString(mlai3))
	fmt.Println(tracker)

	// Output:
	// The newly initialized tracker has a zero closed timestamp:
	//
	//   closed=0,0
	//       |            next=0,1
	//       |          left | right
	//       |             0 # 0
	//       |             1 e 1
	//       v               v
	// ---------------------------------------------------------> time
	//
	// A first command arrives on range 12 (though the range isn't known yet to the Tracker).
	// All commands initially start out on the right. The command has its timestamp forwarded to 0,2 .
	//
	//   closed=0,0
	//       |            next=0,1
	//       |          left | right
	//       |             0 # 1
	//       |             1 e 1
	//       v               v
	// ---------------------------------------------------------> time
	//
	// Two more commands arrive, on r1 and r12.
	//
	//   closed=0,0
	//       |            next=0,1
	//       |          left | right
	//       |             0 # 3
	//       |             1 e 1
	//       v               v
	// ---------------------------------------------------------> time
	//
	// The command on r1 finishes evaluating at Lease Applied Index 10 and lets the Tracker know.
	//
	//   closed=0,0
	//       |            next=0,1
	//       |          left | right
	//       |             0 # 2
	//       |             1 e 1
	//       |               @ 10     (r1)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// The command on r12 also finishes quickly, at LAI 77.
	//
	//   closed=0,0
	//       |            next=0,1
	//       |          left | right
	//       |             0 # 1
	//       |             1 e 1
	//       |               @ 10     (r1)
	//       |               @ 77     (r12)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// The system closes out a timestamp (registering 1000 as the next timestamp to close out).
	// No problem: nothing is tracked on the left side; returns: 0,1 and map[]
	// Note how the items on the right have moved to the left, as they are relevant for the
	// next call to Close.
	//
	//   closed=0,1
	//       |            next=1.000000000,0
	//       |          left | right
	//       |             1 # 0
	//       |             1 e 1
	//       |            10 @        (r1)
	//       |            77 @        (r12)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// Nothing happens for a while until the system tries to close out the next timestamp.
	// However, the very first proposal is still tracked and blocks progress.
	// The call returns a no-op in the form 0,1 map[] .
	//
	//   closed=0,1
	//       |            next=1.000000000,0
	//       |          left | right
	//       |             1 # 0
	//       |             1 e 1
	//       |            10 @        (r1)
	//       |            77 @        (r12)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// A new command gets tracked on r12 (and is forwarded to 1.000000000,1 (if necessary).
	// It terminates quickly, leaving an MLAI entry of 78 behind.
	//
	//   closed=0,1
	//       |            next=1.000000000,0
	//       |          left | right
	//       |             1 # 0
	//       |             1 e 1
	//       |            10 @        (r1)
	//       |            77 @        (r12)
	//       |               @ 78     (r12)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// Finally! The slow evaluation finishes and the command gets proposed at index 79.
	// Note that the right now tracks a smaller value of 78. Consumers have to keep the
	// maximum they've seen.
	//
	//   closed=0,1
	//       |            next=1.000000000,0
	//       |          left | right
	//       |             0 # 0
	//       |             1 e 1
	//       |            10 @        (r1)
	//       |               @ 78     (r12)
	//       |            79 @        (r12)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// The next call to Close() is successful and returns: 1.000000000,0 and map[1:10 12:79]
	//
	//   closed=1.000000000,0
	//       |            next=3.000000000,0
	//       |          left | right
	//       |             0 # 0
	//       |             1 e 1
	//       |            78 @        (r12)
	//       v               v
	// ---------------------------------------------------------> time
}

// mlaiString converts an mlai map into a string. Avoids randomized ordering of
// map elements in string output.
func mlaiString(mlai map[roachpb.RangeID]ctpb.LAI) string {
	var rangeIDs []roachpb.RangeID
	for rangeID := range mlai {
		rangeIDs = append(rangeIDs, rangeID)
	}
	sort.Slice(rangeIDs, func(i, j int) bool {
		return rangeIDs[i] < rangeIDs[j]
	})

	var sb strings.Builder
	sb.WriteString("map[")
	for i, rangeID := range rangeIDs {
		if i > 0 {
			sb.WriteString(" ")
		}
		fmt.Fprintf(&sb, "%d:%d", rangeID, mlai[rangeID])
	}
	sb.WriteString("]")
	return sb.String()
}
