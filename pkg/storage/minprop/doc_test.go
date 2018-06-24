// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package minprop_test

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/storage/minprop"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func ExampleNewTracker() {
	t := minprop.NewTracker()
	fmt.Println(t)
	// Output:
	// 0.000000000,0 has 0 (last)
	// 0.000000000,1 has 0 (cur)
}

func Example() {
	t := minprop.NewTracker()

	// Let's walk through an example of how the MPT works. Initially, last and
	// cur demarcate some time interval. Three commands arrive via the Track
	// method; cur picks up a refcount of three (new commands are forced above
	// cur, though in this case they were there to begin with, and the example
	// code here does not have the timestamp forwarding anyway, though it does
	// show the min proposal timestamp):
	//
	//
	//              0        3
	//            last      cur    commands
	//              |        |        /\   \_______
	//              |        |       /  \          |
	//              v        v       v  v          v
	//    ---------------------------x--x----------x------------> time

	// Read: caller must use timestamp m1 or higher for this proposal
	m1, f1 := t.Track(context.Background())
	m2, f2 := t.Track(context.Background())
	m3, f3 := t.Track(context.Background())
	fmt.Println(m1, m2, m3) // 0,1 0,1 0,1
	fmt.Println(t)          // 3 refs at 0,1 (cur)

	// Next, the system calls Close (passing a more recent timestamp). Since last
	// has a refcount of zero, we know that nothing is in progress for timestamps
	// [last, cur) and we can advance last to cur, and move cur to
	// now-target duration. Note that cur now has a new refcount of zero, while
	// last picked up curs previous refcount, three. This demonstrates the
	// "morally" in the intervals assigned to cur and last: one of the commands
	// is in [cur, ∞) and yet last is now in charge of tracking it. This is
	// common in practice since cur typically trails the node's clock by seconds.
	//
	//
	//                       3                   0
	//                      last    commands    cur
	//                       |        /\   \_____|__
	//                       |       /  \        | |
	//                       v       v  v        v v
	//    ---------------------------x--x----------x------------> time

	// Read: no more proposals at or below closed1 are possible from now on.
	closed1 := t.Close(hlc.Timestamp{WallTime: 300})
	fmt.Println(closed1) // 0,1
	fmt.Println(t)       // 3 refs at 0,1 (last)

	// Two of the commands get proposed, decrementing lasts
	// refcount. Additionally, two new commands arrive at timestamps below
	// cur. As before, cur picks up a refcount of two, but additionally
	// the commands' timestamps are forwarded to cur. These new commands
	// get proposed quickly (so they don't show up again) and curs refcount
	// will drop back to zero.
	//
	//
	//                       1     in-flight      2
	//                     last     command      cur
	//                       |         \          |
	//                       |          \         |
	//                       v          v         v
	//    ------------------------------x-----------------------> time
	//                                            ʌ
	//                                            |
	//             _______________________________/
	//            |   forwarding    |
	//            |                 |
	//        new command         new command
	//      (finishes quickly) (finishes quickly)
	f1()
	f2()
	{
		m4, f4 := t.Track(context.Background())
		m5, f5 := t.Track(context.Background())
		fmt.Println(m4) // 300,0
		fmt.Println(m5) // 300,0
		f4()
		f5()
	}
	fmt.Println(t) // 1 @ 0,1 (last)

	// The remaining command sticks around. This is unfortunate; it's time
	// for another coalesced heartbeat, but we can't return a higher closed
	// timestamp than before and must stick to the same one.
	//
	//
	//                   (blocked)             (blocked)
	//                       1     in-flight      0
	//                     last     command      cur
	//                       |         \          |
	//                       |          \         |
	//                       v          v         v
	//    ------------------------------x-----------------------> time
	closed2 := t.Close(hlc.Timestamp{WallTime: 400})
	fmt.Println(closed2) // 0,1

	// Finally the command gets proposed. A new command comes in at some
	// reasonable timestamp and cur picks up a ref, but that doesn't bother
	// us.
	//
	//                       0                    1
	//                     last                  cur     in-flight
	//                       |                    |      proposal
	//                       |                    |        |
	//                       v                    v        v
	//    -------------------------------------------------x----> time
	f3()
	fmt.Println(t) // nothing in flight

	//
	//
	// Time for the next call to Close. We can finally move last to
	// cur (picking up its refcount) and cur to now-target duration
	// with a zero refcount, concluding the example. In fact we
	// optimize the returned closed timestamp; it's 499 instead of
	// last (300) because we observe that cur is without a ref.
	//
	//                                            1               0
	//                                          last             cur ---···
	//                                            |
	//                                            |
	//                                            v
	//    -------------------------------------------------x----> time
	closed3 := t.Close(hlc.Timestamp{WallTime: 500})
	fmt.Println(closed3) // 499

	// Output:
	// 0.000000000,1 0.000000000,1 0.000000000,1
	// 0.000000000,0 has 0 (last)
	// 0.000000000,1 has 3 (cur)
	// 0.000000000,1
	// 0.000000000,1 has 3 (last)
	// 0.000000300,0 has 0 (cur)
	// 0.000000300,0
	// 0.000000300,0
	// 0.000000000,1 has 1 (last)
	// 0.000000300,0 has 0 (cur)
	// 0.000000000,1
	// 0.000000000,1 has 0 (last)
	// 0.000000300,0 has 0 (cur)
	// 0.000000499,0
}
