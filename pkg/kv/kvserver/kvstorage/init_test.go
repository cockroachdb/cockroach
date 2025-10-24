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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// TestIterateIDPrefixKeys lays down a number of tombstones (at keys.RangeTombstoneKey) interspersed
// with other irrelevant keys (both chosen randomly). It then verifies that IterateIDPrefixKeys
// correctly returns only the relevant keys and values.
func TestIterateIDPrefixKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)

	seed := randutil.NewPseudoSeed()
	// const seed = -1666367124291055473
	t.Logf("seed is %d", seed)
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

	// Write a number of keys that should be irrelevant to the iteration in this test.
	for i := 0; i < rangeCount; i++ {
		rangeID := rangeIDFn()

		// Grab between one and all ops, randomly.
		for _, opIdx := range rng.Perm(len(ops))[:rng.Intn(1+len(ops))] {
			key := ops[opIdx](rangeID)
			t.Logf("writing op=%d rangeID=%d", opIdx, rangeID)
			if _, err := storage.MVCCPut(
				ctx,
				eng,
				key,
				hlc.Timestamp{},
				roachpb.MakeValueFromString("fake value for "+key.String()),
				storage.MVCCWriteOptions{},
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	type seenT struct {
		rangeID   roachpb.RangeID
		tombstone kvserverpb.RangeTombstone
	}

	// Next, write the keys we're planning to see again.
	var wanted []seenT
	{
		used := make(map[roachpb.RangeID]struct{})
		for {
			rangeID := rangeIDFn()
			if _, ok := used[rangeID]; ok {
				// We already wrote this key, so roll the dice again.
				continue
			}

			tombstone := kvserverpb.RangeTombstone{
				NextReplicaID: roachpb.ReplicaID(rng.Int31n(100)),
			}

			used[rangeID] = struct{}{}
			wanted = append(wanted, seenT{rangeID: rangeID, tombstone: tombstone})

			t.Logf("writing tombstone at rangeID=%d", rangeID)
			require.NoError(t, MakeStateLoader(rangeID).SetRangeTombstone(ctx, eng, tombstone))

			if len(wanted) >= rangeCount {
				break
			}
		}
	}

	sort.Slice(wanted, func(i, j int) bool {
		return wanted[i].rangeID < wanted[j].rangeID
	})

	var seen []seenT
	var tombstone kvserverpb.RangeTombstone

	handleTombstone := func(rangeID roachpb.RangeID) error {
		seen = append(seen, seenT{rangeID: rangeID, tombstone: tombstone})
		return nil
	}

	if err := IterateIDPrefixKeys(ctx, eng, keys.RangeTombstoneKey, &tombstone, handleTombstone); err != nil {
		t.Fatal(err)
	}
	placeholder := seenT{
		rangeID: roachpb.RangeID(9999),
	}

	if len(wanted) != len(seen) {
		t.Errorf("wanted %d results, got %d", len(wanted), len(seen))
	}

	for len(wanted) < len(seen) {
		wanted = append(wanted, placeholder)
	}
	for len(seen) < len(wanted) {
		seen = append(seen, placeholder)
	}

	if diff := pretty.Diff(wanted, seen); len(diff) > 0 {
		pretty.Ldiff(t, wanted, seen)
		t.Fatal("diff(wanted, seen) is nonempty")
	}
}
