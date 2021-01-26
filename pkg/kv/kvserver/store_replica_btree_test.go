// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func makeMockPH(start, end string) *ReplicaPlaceholder {
	ph := &ReplicaPlaceholder{}
	ph.rangeDesc.StartKey = roachpb.RKey(start)
	ph.rangeDesc.EndKey = roachpb.RKey(end)
	return ph
}

func TestStoreReplicaBTree_VisitKeyRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	ac := makeMockPH("a", "c")
	cd := makeMockPH("c", "d")
	ef := makeMockPH("e", "f")

	b := newStoreReplicaBTree()
	require.Nil(t, b.ReplaceOrInsertPlaceholder(ctx, ac).item)
	require.Nil(t, b.ReplaceOrInsertPlaceholder(ctx, cd).item)
	require.Nil(t, b.ReplaceOrInsertPlaceholder(ctx, ef).item)

	collect := func(from, to string, order IterationOrder) []*ReplicaPlaceholder {
		t.Helper()
		var seen []*ReplicaPlaceholder
		require.NoError(t, b.VisitKeyRange(
			ctx, roachpb.RKey(from), roachpb.RKey(to), order,
			func(ctx context.Context, it replicaOrPlaceholder) error {
				seen = append(seen, it.ph)
				return nil
			}))
		return seen
	}

	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		check := func(t *testing.T, act []*ReplicaPlaceholder, exp ...*ReplicaPlaceholder) {
			t.Helper()
			if reverse {
				exp = append(([]*ReplicaPlaceholder)(nil), exp...)
				for i, n := 0, len(exp); i < n/2; i++ {
					exp[i], exp[n-i-1] = exp[n-i-1], exp[i]
				}
			}
			require.Equal(t, exp, act)
		}
		order := AscendingKeyOrder
		if reverse {
			order = DescendingKeyOrder
		}

		check(t, collect("", "a", order))
		check(t, collect("", "aa", order), ac)
		check(t, collect("a", "aa", order), ac)
		check(t, collect("aa", "ab", order), ac)
		check(t, collect("aa", "c", order), ac)
		check(t, collect("aa", "ca", order), ac, cd)
		check(t, collect("c", "ca", order), cd)
		check(t, collect("", "zzz", order), ac, cd, ef)
		// These test cases are interesting because the logic in VisitKeyRange
		// that winds back the start key to align with the current range's start
		// key must make sure not to wind back to a range that does not contain
		// the original input start key.
		check(t, collect("d", "e", order))
		check(t, collect("da", "db", order))
		check(t, collect("d", "ea", order), ef)
		check(t, collect("cz", "e", order), cd)
	})
}

func TestStoreReplicaBTree_LookupPrecedingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	makeRepl := func(start, end string) *Replica {
		desc := &roachpb.RangeDescriptor{}
		desc.StartKey = roachpb.RKey(start)
		desc.EndKey = roachpb.RKey(end)
		r := &Replica{}
		r.mu.state.Desc = desc
		r.startKey = desc.StartKey // this is what's actually used in the btree
		return r
	}

	b := newStoreReplicaBTree()

	repl2 := makeRepl("a", "b")
	require.Zero(t, b.ReplaceOrInsertReplica(ctx, repl2))

	repl3 := makeRepl("b", "c")
	require.Zero(t, b.ReplaceOrInsertReplica(ctx, repl3))

	ph := makeMockPH("c", "d")
	require.Zero(t, b.ReplaceOrInsertPlaceholder(ctx, ph))

	repl5 := makeRepl("e", "f")
	require.Zero(t, b.ReplaceOrInsertReplica(ctx, repl5))

	for i, tc := range []struct {
		key     string
		expRepl *Replica
	}{
		{"", nil},
		{"a", nil},
		{"aa", nil},
		{"b", repl2},
		{"bb", repl2},
		{"c", repl3},
		{"cc", repl3},
		{"d", repl3},
		{"dd", repl3},
		{"e", repl3},
		{"ee", repl3},
		{"f", repl5},
		{"\xff\xff", repl5},
	} {
		if repl := b.LookupPrecedingReplica(ctx, roachpb.RKey(tc.key)); repl != tc.expRepl {
			t.Errorf("%d: expected replica %v; got %v", i, tc.expRepl, repl)
		}
	}
}

func TestStoreReplicaBTree_ReplicaCanBeLockedDuringInsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Verify that the replica can be locked while being inserted (and removed).
	// This is important for `Store.maybeMarkReplicaInitializedLockedReplLocked`.
	ctx := context.Background()
	repl := &Replica{}
	k := roachpb.RKey("a")
	repl.mu.state.Desc = &roachpb.RangeDescriptor{
		RangeID: 12,
	}
	repl.startKey = k
	repl.mu.Lock()
	defer repl.mu.Unlock()

	br := newStoreReplicaBTree()
	require.Nil(t, br.ReplaceOrInsertReplica(ctx, repl).item)
	require.Equal(t, repl, br.ReplaceOrInsertReplica(ctx, repl).repl)
	require.Equal(t, repl, br.DeleteReplica(ctx, repl).repl)
}
