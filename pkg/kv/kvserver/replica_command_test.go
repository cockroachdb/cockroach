// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"encoding/binary"
	math "math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression test for #38308. Summary: a non-nullable field was added to
// RangeDescriptor which broke splits, merges, and replica changes if the
// cluster had been upgraded from a previous version of cockroach.
func TestRangeDescriptorUpdateProtoChangedAcrossVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Control our own split destiny.
	args := base.TestServerArgs{Knobs: base.TestingKnobs{Store: &StoreTestingKnobs{
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	}}}
	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	bKey := roachpb.Key("b")
	if err := kvDB.AdminSplit(
		ctx,
		bKey,             /* splitKey */
		hlc.MaxTimestamp, /* expirationTime */
	); err != nil {
		t.Fatal(err)
	}

	// protoVarintField returns an encoded proto field of type varint with the
	// given id.
	protoVarintField := func(fieldID int) []byte {
		var scratch [binary.MaxVarintLen64]byte
		const typ = 0 // varint type field
		tag := uint64(fieldID<<3) | typ
		tagLen := binary.PutUvarint(scratch[:], tag)
		// A proto message is a series of <tag><data> where <tag> is a varint
		// including the field id and the data type and <data> depends on the type.
		buf := append([]byte(nil), scratch[:tagLen]...)
		// The test doesn't care what we use for the field data, so use the tag
		// since the data is a varint and it's already an encoded varint.
		buf = append(buf, scratch[:tagLen]...)
		return buf
	}

	// Update the serialized RangeDescriptor proto for the b to max range to have
	// an unknown proto field. Previously, this would break splits, merges,
	// replica changes. The real regression was a missing field, but an extra
	// unknown field tests the same thing.
	{
		bDescKey := keys.RangeDescriptorKey(roachpb.RKey(bKey))
		bDescKV, err := kvDB.Get(ctx, bDescKey)
		require.NoError(t, err)
		require.NotNil(t, bDescKV.Value, `could not find "b" descriptor`)

		// Update the serialized proto with a new field we don't know about. The
		// proto encoding is just a series of these, so we can do this simply by
		// appending it.
		newBDescBytes, err := bDescKV.Value.GetBytes()
		require.NoError(t, err)
		newBDescBytes = append(newBDescBytes, protoVarintField(9999)...)

		newBDescValue := roachpb.MakeValueFromBytes(newBDescBytes)
		require.NoError(t, kvDB.Put(ctx, bDescKey, &newBDescValue))
	}

	// Verify that splits still work. We could also do a similar thing to test
	// merges and replica changes, but they all go through updateRangeDescriptor
	// so it's unnecessary.
	cKey := roachpb.Key("c")
	if err := kvDB.AdminSplit(
		ctx,
		cKey,             /* splitKey */
		hlc.MaxTimestamp, /* expirationTime */
	); err != nil {
		t.Fatal(err)
	}
}

func TestSynthesizeTargetsByChangeType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		name                                      string
		changes                                   []kvpb.ReplicationChange
		expPromotions, expDemotions               []int32
		expVoterAdditions, expVoterRemovals       []int32
		expNonVoterAdditions, expNonVoterRemovals []int32
	}

	mkTarget := func(t int32) roachpb.ReplicationTarget {
		return roachpb.ReplicationTarget{
			NodeID: roachpb.NodeID(t), StoreID: roachpb.StoreID(t),
		}
	}

	mkTargetList := func(targets []int32) []roachpb.ReplicationTarget {
		if len(targets) == 0 {
			return nil
		}
		res := make([]roachpb.ReplicationTarget, len(targets))
		for i, t := range targets {
			res[i] = mkTarget(t)
		}
		return res
	}

	tests := []testCase{
		{
			name: "simple voter addition",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
			},
			expVoterAdditions: []int32{2},
		},
		{
			name: "simple voter removal",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(2)},
			},
			expVoterRemovals: []int32{2},
		},
		{
			name: "simple non-voter addition",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(2)},
			},
			expNonVoterAdditions: []int32{2},
		},
		{
			name: "simple non-voter removal",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
			},
			expNonVoterRemovals: []int32{2},
		},
		{
			name: "promote non_voter to voter",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
			},
			expPromotions: []int32{2},
		},
		{
			name: "demote voter to non_voter",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
			},
			expDemotions: []int32{1},
		},
		{
			name: "swap voter with non_voter",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
			},
			expPromotions: []int32{2},
			expDemotions:  []int32{1},
		},
		{
			name: "swap with simple addition",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(4)},
			},
			expPromotions:     []int32{2},
			expDemotions:      []int32{1},
			expVoterAdditions: []int32{4},
		},
		{
			name: "swap with simple removal",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(4)},
			},
			expPromotions:    []int32{2},
			expDemotions:     []int32{1},
			expVoterRemovals: []int32{4},
		},
		{
			name: "swap with addition promotion",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(3)},
			},
			expPromotions: []int32{2, 3},
			expDemotions:  []int32{1},
		},
		{
			name: "swap with additional demotion",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(4)},
			},
			expPromotions: []int32{2},
			expDemotions:  []int32{1, 4},
		},
		{
			name: "two swaps",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(3)},
			},
			expPromotions: []int32{2, 3},
			expDemotions:  []int32{1, 4},
		},
		{
			name: "all at once",
			changes: []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: mkTarget(1)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(2)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(3)},
				{ChangeType: roachpb.ADD_VOTER, Target: mkTarget(4)},
				{ChangeType: roachpb.ADD_NON_VOTER, Target: mkTarget(5)},
				{ChangeType: roachpb.REMOVE_NON_VOTER, Target: mkTarget(6)},
			},
			expPromotions:        []int32{2, 3},
			expDemotions:         []int32{1},
			expVoterAdditions:    []int32{4},
			expNonVoterAdditions: []int32{5},
			expNonVoterRemovals:  []int32{6},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := SynthesizeTargetsByChangeType(test.changes)
			require.Equal(t, result.NonVoterPromotions, mkTargetList(test.expPromotions))
			require.Equal(t, result.VoterDemotions, mkTargetList(test.expDemotions))
			require.Equal(t, result.VoterAdditions, mkTargetList(test.expVoterAdditions))
			require.Equal(t, result.VoterRemovals, mkTargetList(test.expVoterRemovals))
			require.Equal(t, result.NonVoterAdditions, mkTargetList(test.expNonVoterAdditions))
			require.Equal(t, result.NonVoterRemovals, mkTargetList(test.expNonVoterRemovals))
		})
	}
}

func TestWaitForLeaseAppliedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const maxLAI = math.MaxUint64

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{}
	tc.Start(ctx, t, stopper)
	db := tc.store.DB()

	// Submit a write and read it back to bump the initial LAI.
	write := func(key, value string) {
		require.NoError(t, db.Put(ctx, key, value))
		_, err := db.Get(ctx, key)
		require.NoError(t, err)
	}
	write("foo", "bar")

	// Should return immediately when already at or past the LAI.
	currentLAI := tc.repl.GetLeaseAppliedIndex()
	require.NotZero(t, currentLAI)
	resultLAI, err := tc.repl.WaitForLeaseAppliedIndex(ctx, currentLAI)
	require.NoError(t, err)
	require.GreaterOrEqual(t, resultLAI, currentLAI)

	// Should wait for a future LAI to be reached.
	const numWrites = 10
	waitLAI := tc.repl.GetLeaseAppliedIndex() + numWrites
	laiC := make(chan kvpb.LeaseAppliedIndex, 1)
	go func() {
		lai, err := tc.repl.WaitForLeaseAppliedIndex(ctx, waitLAI)
		assert.NoError(t, err) // can't use require in goroutine
		laiC <- lai
	}()

	select {
	case lai := <-laiC:
		t.Fatalf("unexpected early LAI %d", lai)
	case <-time.After(time.Second):
	}

	for i := 0; i < numWrites; i++ {
		write("foo", "bar")
	}

	select {
	case lai := <-laiC:
		require.GreaterOrEqual(t, lai, waitLAI)
		require.GreaterOrEqual(t, tc.repl.GetLeaseAppliedIndex(), lai)
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for LAI %d", waitLAI)
	}

	// Should error on context cancellation.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = tc.repl.WaitForLeaseAppliedIndex(cancelCtx, maxLAI)
	require.Error(t, err)
	require.Equal(t, cancelCtx.Err(), err)

	// Should error on destroyed replicas.
	stopper.Stop(ctx)

	destroyErr := errors.New("destroyed")
	func() {
		tc.repl.readOnlyCmdMu.Lock()
		defer tc.repl.readOnlyCmdMu.Unlock()
		tc.repl.mu.Lock()
		defer tc.repl.mu.Unlock()
		tc.repl.shMu.destroyStatus.Set(destroyErr, destroyReasonRemoved)
	}()

	_, err = tc.repl.WaitForLeaseAppliedIndex(ctx, maxLAI)
	require.Error(t, err)
	require.Equal(t, destroyErr, err)
}
