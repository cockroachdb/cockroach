// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func ExampleSingleStorage() {
	s := NewMemStorage(10*time.Second, 4)
	fmt.Println("The empty storage renders as below:")
	fmt.Println(s)

	fmt.Println("After adding the following entry:")
	e1 := ctpb.Entry{
		Full:            true,
		ClosedTimestamp: hlc.Timestamp{WallTime: 123e9},
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
		ClosedTimestamp: hlc.Timestamp{WallTime: 125e9},
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
		ClosedTimestamp: hlc.Timestamp{WallTime: 133e9},
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
		ClosedTimestamp: hlc.Timestamp{WallTime: 133e9 + 1e9/2},
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
		ClosedTimestamp: hlc.Timestamp{WallTime: 180e9},
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
		ClosedTimestamp: hlc.Timestamp{WallTime: 185e9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			3: 1771,
		},
	}
	fmt.Println(e6)
	s.Add(e6)
	fmt.Println("All buckets rotate, but the third and fourth remain over target age.")
	fmt.Println("This would resolve itself if reasonably spaced updates kept coming in.")
	fmt.Println(s)

	fmt.Println("Finally, when the storage is cleared, all buckets are reset.")
	s.Clear()
	fmt.Println(s)

	// Output:
	// The empty storage renders as below:
	// +--+---------------------+----------------------+----------------------+----------------------+
	//      0,0 age=0s (target     0,0 age=0s (target     0,0 age=0s (target     0,0 age=0s (target
	//         ≤0s) epoch=0          ≤10s) epoch=0          ≤20s) epoch=0          ≤40s) epoch=0
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
	//          123.000000000,0      0,0 age=2m3s (target     0,0 age=2m3s (target     0,0 age=2m3s (target
	//        age=0s (target ≤0s)       ≤10s) epoch=0            ≤20s) epoch=0            ≤40s) epoch=0
	//              epoch=0
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
	//          125.000000000,0       123.000000000,0       0,0 age=2m5s (target     0,0 age=2m5s (target
	//        age=0s (target ≤0s)   age=2s (target ≤10s)       ≤20s) epoch=0            ≤40s) epoch=0
	//              epoch=0               epoch=0
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
	//          133.000000000,0        123.000000000,0       0,0 age=2m13s (target     0,0 age=2m13s (target
	//        age=0s (target ≤0s)   age=10s (target ≤10s)        ≤20s) epoch=0             ≤40s) epoch=0
	//              epoch=0                epoch=0
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
	//          133.500000000,0         133.000000000,0           123.000000000,0        0,0 age=2m13.5s (target
	//        age=0s (target ≤0s)   age=500ms (target ≤10s)   age=10.5s (target ≤20s)         ≤40s) epoch=0
	//              epoch=0                 epoch=0                   epoch=0
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
	//              epoch=0                 epoch=0                  epoch=0                 epoch=0
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
	//              epoch=0               epoch=0                  epoch=0                  epoch=0
	// +----+---------------------+----------------------+-------------------------+-----------------------+
	//   r1                  1004                   1004                      1001                    1001
	//   r2                929922                 929922
	//   r3                  1771
	//   r7                    19                     19                        17                      12
	//   r8                   711                    711                       711
	//   r9                  2020                   2020                      2020                    2020
	// +----+---------------------+----------------------+-------------------------+-----------------------+
	//
	// Finally, when the storage is cleared, all buckets are reset.
	// +--+---------------------+----------------------+----------------------+----------------------+
	//      0,0 age=0s (target     0,0 age=0s (target     0,0 age=0s (target     0,0 age=0s (target
	//         ≤0s) epoch=0          ≤10s) epoch=0          ≤20s) epoch=0          ≤40s) epoch=0
	// +--+---------------------+----------------------+----------------------+----------------------+
	// +--+---------------------+----------------------+----------------------+----------------------+
}

func ExampleMultiStorage_epoch() {
	ms := NewMultiStorage(func() SingleStorage {
		return NewMemStorage(time.Millisecond, 2)
	})

	e1 := ctpb.Entry{
		Epoch:           10,
		ClosedTimestamp: hlc.Timestamp{WallTime: 1e9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			9: 17,
		},
	}
	fmt.Println("First, the following entry is added:")
	fmt.Println(e1)
	ms.Add(1, e1)
	fmt.Println(ms)

	fmt.Println("The epoch changes. It can only increase, for we receive Entries in a fixed order.")
	e2 := ctpb.Entry{
		Epoch:           11,
		ClosedTimestamp: hlc.Timestamp{WallTime: 2e9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			9:  18,
			10: 99,
		},
	}
	ms.Add(1, e2)
	fmt.Println(e2)
	fmt.Println(ms)

	fmt.Println("If it *did* decrease, a higher level component should trigger an assertion.")
	fmt.Println("The storage itself will simply ignore such updates:")
	e3 := ctpb.Entry{
		Epoch:           8,
		ClosedTimestamp: hlc.Timestamp{WallTime: 3e9},
		MLAI: map[roachpb.RangeID]ctpb.LAI{
			9:  19,
			10: 199,
		},
	}
	fmt.Println(e3)
	ms.Add(1, e3)
	fmt.Println(ms)

	// Output:
	// First, the following entry is added:
	// CT: 1.000000000,0 @ Epoch 10
	// Full: false
	// MLAI: r9: 17
	//
	// ***** n1 *****
	// +----+---------------------+----------------------+
	//           1.000000000,0       0,0 age=1s (target
	//        age=0s (target ≤0s)      ≤1ms) epoch=0
	//             epoch=10
	// +----+---------------------+----------------------+
	//   r9                    17
	// +----+---------------------+----------------------+
	//
	// The epoch changes. It can only increase, for we receive Entries in a fixed order.
	// CT: 2.000000000,0 @ Epoch 11
	// Full: false
	// MLAI: r9: 18, r10: 99
	//
	// ***** n1 *****
	// +-----+---------------------+----------------------+
	//            2.000000000,0         1.000000000,0
	//         age=0s (target ≤0s)   age=1s (target ≤1ms)
	//              epoch=11               epoch=10
	// +-----+---------------------+----------------------+
	//   r9                     18                     17
	//   r10                    99
	// +-----+---------------------+----------------------+
	//
	// If it *did* decrease, a higher level component should trigger an assertion.
	// The storage itself will simply ignore such updates:
	// CT: 3.000000000,0 @ Epoch 8
	// Full: false
	// MLAI: r9: 19, r10: 199
	//
	// ***** n1 *****
	// +-----+---------------------+----------------------+
	//            2.000000000,0         2.000000000,0
	//         age=0s (target ≤0s)   age=0s (target ≤1ms)
	//              epoch=11               epoch=11
	// +-----+---------------------+----------------------+
	//   r9                     18                     18
	//   r10                    99                     99
	// +-----+---------------------+----------------------+
}

func TestZeroValueGetsStored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test ensures that a zero values MLAI is stored for an epoch especially
	// after we've already stored a non-zero MLAI for a different range in the
	// same epoch. See #32904.
	ms := NewMultiStorage(func() SingleStorage {
		return NewMemStorage(time.Millisecond, 10)
	})
	e := ctpb.Entry{
		Epoch:           1,
		ClosedTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		MLAI:            map[roachpb.RangeID]ctpb.LAI{1: 1},
	}
	ms.Add(1, e)
	e.ClosedTimestamp.WallTime++
	r := roachpb.RangeID(2)
	e.MLAI = map[roachpb.RangeID]ctpb.LAI{r: 0}
	ms.Add(1, e)
	var seen bool
	ms.VisitDescending(1, func(e ctpb.Entry) (done bool) {
		for rr, mlai := range e.MLAI {
			if rr == r && mlai == 0 {
				seen = true
				return true
			}
		}
		return false
	})
	if !seen {
		t.Fatalf("Failed to see added zero value MLAI for range %v", r)
	}
}

// TestConcurrent runs a very basic sanity check against a Storage, verifiying
// that the bucketed Entries don't regress in obvious ways.
func TestConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ms := NewMultiStorage(func() SingleStorage {
		return NewMemStorage(time.Millisecond, 10)
	})

	var g errgroup.Group

	const (
		iters             = 10
		numNodes          = roachpb.NodeID(2)
		numRanges         = roachpb.RangeID(3)
		numReadersPerNode = 3
		numWritersPerNode = 3
	)

	// concurrently add and read from storage
	// after add: needs to be visible to future read
	// read ts never regresses
	globalRand, seed := randutil.NewPseudoRand()
	t.Log("seed is", seed)

	for i := 0; i < numWritersPerNode; i++ {
		for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
			nodeID := nodeID // goroutine-local copy
			for i := 0; i < iters; i++ {
				r := rand.New(rand.NewSource(globalRand.Int63()))
				m := make(map[roachpb.RangeID]ctpb.LAI)
				for rangeID := roachpb.RangeID(1); rangeID < numRanges; rangeID++ {
					if r.Intn(int(numRanges)) == 0 {
						continue
					}
					m[rangeID] = ctpb.LAI(rand.Intn(100))
				}
				ct := hlc.Timestamp{WallTime: r.Int63n(100), Logical: r.Int31n(10)}
				epo := ctpb.Epoch(r.Int63n(100))
				g.Go(func() error {
					<-time.After(time.Duration(rand.Intn(1e7)))
					ms.Add(nodeID, ctpb.Entry{
						Epoch:           epo,
						ClosedTimestamp: ct,
						MLAI:            m,
					})
					return nil
				})
			}
		}
	}

	for i := 0; i < numReadersPerNode; i++ {
		for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
			nodeID := nodeID
			g.Go(func() error {
				epo := ctpb.Epoch(-1)
				var ct hlc.Timestamp
				var mlai map[roachpb.RangeID]ctpb.LAI
				var err error
				var n int
				ms.VisitDescending(nodeID, func(e ctpb.Entry) bool {
					n++
					if n > 1 && e.Epoch > epo {
						err = errors.Errorf("epoch regressed from %d to %d", epo, e.Epoch)
						return true // done
					}
					if n > 1 && ct.Less(e.ClosedTimestamp) {
						err = errors.Errorf("closed timestamp regressed from %s to %s", ct, e.ClosedTimestamp)
						return true // done
					}
					for rangeID := roachpb.RangeID(1); rangeID <= numRanges; rangeID++ {
						if l := mlai[rangeID]; l < e.MLAI[rangeID] && n > 1 {
							err = errors.Errorf("MLAI for r%d regressed: %+v to %+v", rangeID, mlai, e.MLAI)
							return true // done
						}
					}

					epo = e.Epoch
					ct = e.ClosedTimestamp
					mlai = e.MLAI
					return false // not done
				})
				return err
			})
		}
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}
