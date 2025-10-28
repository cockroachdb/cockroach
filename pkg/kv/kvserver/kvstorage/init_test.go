// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestIterateRangeIDKeys lays down a number of RangeTombstone and ReplicaID
// keys (at keys.RangeTombstoneKey and keys.RaftReplicaIDKey) interspersed with
// other irrelevant keys (both chosen randomly). It then verifies that
// iterateRangeIDKeys correctly returns only the relevant keys and values.
func TestIterateRangeIDKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	seed := randutil.NewPseudoSeed()
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	ops := []func(rangeID roachpb.RangeID) roachpb.Key{
		keys.RaftHardStateKey, // unreplicated; sorts after tombstone
		// Replicated key-anchored local key (i.e. not one we should care about).
		// Will be written at zero timestamp, but that's ok.
		func(rangeID roachpb.RangeID) roachpb.Key {
			return keys.RangeDescriptorKey([]byte(fmt.Sprintf("fakerange%d", rangeID)))
		},
		func(rangeID roachpb.RangeID) roachpb.Key {
			return roachpb.Key(fmt.Sprintf("fakeuserkey%d", rangeID))
		},
	}

	const rangeCount = 10
	rangeIDFn := func() roachpb.RangeID {
		return 1 + roachpb.RangeID(rng.Intn(10*rangeCount)) // spread rangeIDs out
	}
	toss := func(x, outOf int) bool {
		return rng.Intn(outOf) < x
	}

	// Write a number of keys that should be irrelevant to the iteration in this test.
	for i := 0; i < rangeCount; i++ {
		rangeID := rangeIDFn()
		// Grab between one and all ops, randomly.
		for _, opIdx := range rng.Perm(len(ops))[:rng.Intn(1+len(ops))] {
			key := ops[opIdx](rangeID)
			t.Logf("writing op=%d rangeID=%d", opIdx, rangeID)
			_, err := storage.MVCCPut(
				ctx, eng, key, hlc.Timestamp{},
				roachpb.MakeValueFromString("fake value for "+key.String()),
				storage.MVCCWriteOptions{},
			)
			require.NoError(t, err)
		}
	}

	type seenT struct {
		rangeID   roachpb.RangeID
		tombstone kvserverpb.RangeTombstone
		replicaID kvserverpb.RaftReplicaID
	}

	// Next, write the keys we're planning to see again.
	wanted := make([]seenT, 0, rangeCount)
	for used := make(map[roachpb.RangeID]struct{}); len(wanted) < rangeCount; {
		rangeID := rangeIDFn()
		if _, ok := used[rangeID]; ok {
			// We already wrote this RangeID, so roll the dice again.
			continue
		}
		used[rangeID] = struct{}{}

		// Write one or both keys, each combination with 1/3 chance.
		writeTombstone := toss(2, 3)                    // p == 2/3
		writeReplicaID := !writeTombstone || toss(1, 2) // p == 2/3

		sl := MakeStateLoader(rangeID)
		written := seenT{rangeID: rangeID}
		if writeTombstone {
			written.tombstone = kvserverpb.RangeTombstone{
				NextReplicaID: roachpb.ReplicaID(rng.Int31n(100)),
			}
			t.Logf("writing tombstone at rangeID=%d", rangeID)
			require.NoError(t, sl.SetRangeTombstone(ctx, eng, written.tombstone))
		}
		if writeReplicaID {
			id := roachpb.ReplicaID(rng.Int31n(100))
			written.replicaID = kvserverpb.RaftReplicaID{ReplicaID: id}
			t.Logf("writing ReplicaID at rangeID=%d", rangeID)
			require.NoError(t, sl.SetRaftReplicaID(ctx, eng, id))
		}

		wanted = append(wanted, written)
	}

	sort.Slice(wanted, func(i, j int) bool {
		return wanted[i].rangeID < wanted[j].rangeID
	})

	var seen []seenT
	require.NoError(t, iterateRangeIDKeys(ctx, eng, func(id roachpb.RangeID, get readKeyFn) error {
		var tombstone kvserverpb.RangeTombstone
		foundTS, err := get(keys.RangeTombstoneKey(id), &tombstone)
		if err != nil {
			return err
		}
		var replicaID kvserverpb.RaftReplicaID
		foundID, err := get(keys.RaftReplicaIDKey(id), &replicaID)
		if err != nil {
			return err
		}
		if foundTS || foundID {
			seen = append(seen, seenT{rangeID: id, tombstone: tombstone, replicaID: replicaID})
		}
		return nil
	}))

	require.Equal(t, wanted, seen)
}
