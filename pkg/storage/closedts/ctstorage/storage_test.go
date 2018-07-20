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

package ctstorage

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func ExampleSingleStorage() {
	s := NewMemStorage(10*time.Second, 4)
	fmt.Println("The empty storage renders as below:")
	fmt.Println(s)

	fmt.Println("After adding the following entry:")
	e1 := ctpb.Entry{
		Full:            true,
		ClosedTimestamp: hlc.Timestamp{WallTime: 123E9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			1: 1000,
			9: 2000,
		},
	}
	fmt.Println(e1)
	s.Add(e1)
	fmt.Println("the result is:")
	fmt.Println(s)
	fmt.Println("Note how the most recent bucket picked up the update.")

	fmt.Println("A new update comes in only two seconds later:")
	e2 := ctpb.Entry{
		ClosedTimestamp: hlc.Timestamp{WallTime: 125E9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			1: 1001,
			7: 12,
		},
	}
	fmt.Println(e2)
	s.Add(e2)
	fmt.Println("The first bucket now contains the union of both updates.")
	fmt.Println("The second bucket holds on to the previous value of the first.")
	fmt.Println("The remaining buckets are unchanged. The best we could do is")
	fmt.Println("give them identical copies of the second, but that's nonsense.")
	fmt.Println(s)

	fmt.Println("Another update, another eight seconds later:")
	e3 := ctpb.Entry{
		ClosedTimestamp: hlc.Timestamp{WallTime: 133E9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			9: 2020,
			1: 999,
		},
	}
	fmt.Println(e3)
	s.Add(e3)
	fmt.Println("Note how the second bucket didn't rotate, for it is not yet")
	fmt.Println("older than 10s. Note also how the first bucket ignores the")
	fmt.Println("downgrade for r1; these can occur in practice.")
	fmt.Println(s)

	fmt.Println("Half a second later, with the next update, it will rotate:")
	e4 := ctpb.Entry{
		ClosedTimestamp: hlc.Timestamp{WallTime: 133E9 + 1E9/2},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			7: 17,
			8: 711,
		},
	}
	fmt.Println(e4)
	s.Add(e4)
	fmt.Println("Consequently we now see the third bucket fill up.")
	fmt.Println(s)

	fmt.Println("Next update arrives a whopping 46.5s later (why not).")
	e5 := ctpb.Entry{
		ClosedTimestamp: hlc.Timestamp{WallTime: 180E9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			1: 1004,
			7: 19,
			2: 929922,
		},
	}
	fmt.Println(e5)
	s.Add(e5)
	fmt.Println("The second bucket rotated, but due to the sparseness of updates,")
	fmt.Println("it's still above its target age and will rotate again next time.")
	fmt.Println("The same is true for the remaining buckets.")
	fmt.Println(s)

	fmt.Println("Another five seconds later, another update:")
	e6 := ctpb.Entry{
		ClosedTimestamp: hlc.Timestamp{WallTime: 185E9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			3: 1771,
		},
	}
	fmt.Println(e6)
	s.Add(e6)
	fmt.Println("All buckets rotate, but the third and fourth remain over target age.")
	fmt.Println("This would resolve itself if reasonably spaced updates kept coming in.")
	fmt.Println(s)

	// Output:
	// The empty storage renders as below:
	// +--+---------------------+----------------------+----------------------+----------------------+
	//         0.000000000,0      0.000000000,0 age=0s   0.000000000,0 age=0s   0.000000000,0 age=0s
	//      age=0s (target ≤0s)      (target ≤10s)          (target ≤20s)          (target ≤40s)
	// +--+---------------------+----------------------+----------------------+----------------------+
	// +--+---------------------+----------------------+----------------------+----------------------+
	//
	// After adding the following entry:
	// CT: 123.000000000,0 @ Epoch 0
	// Full: true
	// MLAI: r1: 1000, r9: 2000
	//
	// the result is:
	// +----+---------------------+------------------------+------------------------+------------------------+
	//          123.000000000,0     0.000000000,0 age=2m3s   0.000000000,0 age=2m3s   0.000000000,0 age=2m3s
	//        age=0s (target ≤0s)       (target ≤10s)            (target ≤20s)            (target ≤40s)
	// +----+---------------------+------------------------+------------------------+------------------------+
	//   r1                  1000
	//   r9                  2000
	// +----+---------------------+------------------------+------------------------+------------------------+
	//
	// Note how the most recent bucket picked up the update.
	// A new update comes in only two seconds later:
	// CT: 125.000000000,0 @ Epoch 0
	// Full: false
	// MLAI: r1: 1001, r7: 12
	//
	// The first bucket now contains the union of both updates.
	// The second bucket holds on to the previous value of the first.
	// The remaining buckets are unchanged. The best we could do is
	// give them identical copies of the second, but that's nonsense.
	// +----+---------------------+----------------------+------------------------+------------------------+
	//          125.000000000,0       123.000000000,0      0.000000000,0 age=2m5s   0.000000000,0 age=2m5s
	//        age=0s (target ≤0s)   age=2s (target ≤10s)       (target ≤20s)            (target ≤40s)
	// +----+---------------------+----------------------+------------------------+------------------------+
	//   r1                  1001                   1000
	//   r7                    12
	//   r9                  2000                   2000
	// +----+---------------------+----------------------+------------------------+------------------------+
	//
	// Another update, another eight seconds later:
	// CT: 133.000000000,0 @ Epoch 0
	// Full: false
	// MLAI: r1: 999, r9: 2020
	//
	// Note how the second bucket didn't rotate, for it is not yet
	// older than 10s. Note also how the first bucket ignores the
	// downgrade for r1; these can occur in practice.
	// +----+---------------------+-----------------------+-------------------------+-------------------------+
	//          133.000000000,0        123.000000000,0      0.000000000,0 age=2m13s   0.000000000,0 age=2m13s
	//        age=0s (target ≤0s)   age=10s (target ≤10s)        (target ≤20s)             (target ≤40s)
	// +----+---------------------+-----------------------+-------------------------+-------------------------+
	//   r1                  1001                    1000
	//   r7                    12
	//   r9                  2020                    2000
	// +----+---------------------+-----------------------+-------------------------+-------------------------+
	//
	// Half a second later, with the next update, it will rotate:
	// CT: 133.500000000,0 @ Epoch 0
	// Full: false
	// MLAI: r7: 17, r8: 711
	//
	// Consequently we now see the third bucket fill up.
	// +----+---------------------+-------------------------+-------------------------+---------------------------+
	//          133.500000000,0         133.000000000,0           123.000000000,0       0.000000000,0 age=2m13.5s
	//        age=0s (target ≤0s)   age=500ms (target ≤10s)   age=10.5s (target ≤20s)         (target ≤40s)
	// +----+---------------------+-------------------------+-------------------------+---------------------------+
	//   r1                  1001                      1001                      1000
	//   r7                    17                        12
	//   r8                   711
	//   r9                  2020                      2020                      2000
	// +----+---------------------+-------------------------+-------------------------+---------------------------+
	//
	// Next update arrives a whopping 46.5s later (why not).
	// CT: 180.000000000,0 @ Epoch 0
	// Full: false
	// MLAI: r1: 1004, r2: 929922, r7: 19
	//
	// The second bucket rotated, but due to the sparseness of updates,
	// it's still above its target age and will rotate again next time.
	// The same is true for the remaining buckets.
	// +----+---------------------+-------------------------+-----------------------+-----------------------+
	//          180.000000000,0         133.500000000,0          133.000000000,0         123.000000000,0
	//        age=0s (target ≤0s)   age=46.5s (target ≤10s)   age=47s (target ≤20s)   age=57s (target ≤40s)
	// +----+---------------------+-------------------------+-----------------------+-----------------------+
	//   r1                  1004                      1001                    1001                    1000
	//   r2                929922
	//   r7                    19                        17                      12
	//   r8                   711                       711
	//   r9                  2020                      2020                    2020                    2000
	// +----+---------------------+-------------------------+-----------------------+-----------------------+
	//
	// Another five seconds later, another update:
	// CT: 185.000000000,0 @ Epoch 0
	// Full: false
	// MLAI: r3: 1771
	//
	// All buckets rotate, but the third and fourth remain over target age.
	// This would resolve itself if reasonably spaced updates kept coming in.
	// +----+---------------------+----------------------+-------------------------+-----------------------+
	//          185.000000000,0       180.000000000,0          133.500000000,0          133.000000000,0
	//        age=0s (target ≤0s)   age=5s (target ≤10s)   age=51.5s (target ≤20s)   age=52s (target ≤40s)
	// +----+---------------------+----------------------+-------------------------+-----------------------+
	//   r1                  1004                   1004                      1001                    1001
	//   r2                929922                 929922
	//   r3                  1771
	//   r7                    19                     19                        17                      12
	//   r8                   711                    711                       711
	//   r9                  2020                   2020                      2020                    2020
	// +----+---------------------+----------------------+-------------------------+-----------------------+
}
