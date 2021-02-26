// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sidetransport

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

type mockStores struct {
	recording []rangeUpdate
	sem       chan struct{}
}

type rangeUpdate struct {
	rid      roachpb.RangeID
	closedTS hlc.Timestamp
	lai      ctpb.LAI
}

var _ Stores = &mockStores{}

func (m *mockStores) ForwardSideTransportClosedTimestampForRange(
	ctx context.Context, rangeID roachpb.RangeID, closedTS hlc.Timestamp, lai ctpb.LAI,
) {
	upd := rangeUpdate{
		rid:      rangeID,
		closedTS: closedTS,
		lai:      lai,
	}
	m.recording = append(m.recording, upd)
	if m.sem != nil {
		m.sem <- struct{}{}
		<-m.sem
	}
}

func (m *mockStores) getAndClearRecording() []rangeUpdate {
	res := m.recording
	m.recording = nil
	return res
}

var ts10 = hlc.Timestamp{WallTime: 10}
var ts11 = hlc.Timestamp{WallTime: 11}
var ts12 = hlc.Timestamp{WallTime: 12}
var ts20 = hlc.Timestamp{WallTime: 20, Synthetic: true}
var ts21 = hlc.Timestamp{WallTime: 21, Synthetic: true}
var ts22 = hlc.Timestamp{WallTime: 22, Synthetic: true}
var laiZero = ctpb.LAI(0)

const lai100 = ctpb.LAI(100)
const lai101 = ctpb.LAI(101)
const lai102 = ctpb.LAI(102)
const lai103 = ctpb.LAI(102)

func TestIncomingStreamProcessUpdateBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	nid := &base.NodeIDContainer{}
	nid.Set(ctx, 1)
	stores := &mockStores{}
	server := NewReceiver(nid, stopper, stores, receiverTestingKnobs{})
	r := newIncomingStream(server, stores)
	r.nodeID = 1

	msg := &ctpb.Update{
		NodeID:   1,
		SeqNum:   1,
		Snapshot: true,
		ClosedTimestamps: []ctpb.Update_GroupUpdate{
			{Policy: roachpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: ts10},
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, ClosedTimestamp: ts20},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 1, LAI: lai100, Policy: roachpb.LAG_BY_CLUSTER_SETTING},
			{RangeID: 2, LAI: lai101, Policy: roachpb.LAG_BY_CLUSTER_SETTING},
			{RangeID: 3, LAI: lai102, Policy: roachpb.LEAD_FOR_GLOBAL_READS},
		},
		Removed: nil,
	}
	r.processUpdate(ctx, msg)
	ts, lai := r.GetClosedTimestamp(ctx, 1)
	require.Equal(t, ts10, ts)
	require.Equal(t, lai100, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 2)
	require.Equal(t, ts10, ts)
	require.Equal(t, lai101, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 3)
	require.Equal(t, ts20, ts)
	require.Equal(t, lai102, lai)
	require.Empty(t, stores.getAndClearRecording())

	// Remove range 1, update 2 implicitly, update 3 explicitly.
	msg = &ctpb.Update{
		NodeID:   1,
		SeqNum:   2,
		Snapshot: false,
		ClosedTimestamps: []ctpb.Update_GroupUpdate{
			{Policy: roachpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: ts11},
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, ClosedTimestamp: ts21},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 3, LAI: lai103, Policy: roachpb.LEAD_FOR_GLOBAL_READS},
		},
		Removed: []roachpb.RangeID{1},
	}
	r.processUpdate(ctx, msg)
	ts, lai = r.GetClosedTimestamp(ctx, 1)
	require.Empty(t, ts)
	require.Equal(t, laiZero, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 2)
	require.Equal(t, ts11, ts)
	require.Equal(t, lai101, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 3)
	require.Equal(t, ts21, ts)
	require.Equal(t, lai103, lai)
	require.Equal(t, []rangeUpdate{{rid: 1, closedTS: ts10, lai: lai100}}, stores.getAndClearRecording())

	// Send a snapshot and check that it rests all the state.
	msg = &ctpb.Update{
		NodeID:   1,
		SeqNum:   3,
		Snapshot: true,
		ClosedTimestamps: []ctpb.Update_GroupUpdate{
			{Policy: roachpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: ts12},
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, ClosedTimestamp: ts22},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 3, LAI: lai102, Policy: roachpb.LEAD_FOR_GLOBAL_READS},
			{RangeID: 4, LAI: lai100, Policy: roachpb.LAG_BY_CLUSTER_SETTING},
		},
		Removed: nil,
	}
	r.processUpdate(ctx, msg)
	ts, lai = r.GetClosedTimestamp(ctx, 2)
	require.Empty(t, ts)
	require.Equal(t, laiZero, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 3)
	require.Equal(t, ts22, ts)
	require.Equal(t, lai102, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 4)
	require.Equal(t, ts12, ts)
	require.Equal(t, lai100, lai)
	require.Empty(t, stores.getAndClearRecording())
}

// Test that when the incomingStream calls into the Stores to update a range, it
// doesn't hold its internal lock. Or, in other words, test that replicas can
// call into the stream while the stream is blocked updating the stores. In
// particular, the replica being updated might be calling into the stream to get
// its closed timestamp (async, for another operation), and it'd better not
// deadlock.
func TestIncomingStreamCallsIntoStoresDontHoldLock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	nid := &base.NodeIDContainer{}
	nid.Set(ctx, 1)
	stores := &mockStores{}
	server := NewReceiver(nid, stopper, stores, receiverTestingKnobs{})
	r := newIncomingStream(server, stores)
	r.nodeID = 1

	// Add a range to the stream.
	msg := &ctpb.Update{
		NodeID: 1, SeqNum: 1, Snapshot: true,
		ClosedTimestamps: []ctpb.Update_GroupUpdate{
			{Policy: roachpb.LEAD_FOR_GLOBAL_READS, ClosedTimestamp: ts10},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 1, LAI: lai100, Policy: roachpb.LEAD_FOR_GLOBAL_READS},
		},
		Removed: nil,
	}
	r.processUpdate(ctx, msg)

	// Remove the range and block the removal in the Stores.
	ch := make(chan struct{})
	stores.sem = ch
	msg = &ctpb.Update{
		NodeID: 1, SeqNum: 2, Snapshot: false,
		Removed: []roachpb.RangeID{1},
	}
	go r.processUpdate(ctx, msg)
	// Wait for the processUpdate to block.
	<-ch
	// With the update blocked, call into the stream. We're testing that this
	// doesn't deadlock.
	ts, _ := r.GetClosedTimestamp(ctx, 1)
	require.Equal(t, ts10, ts)
	// Unblock the process.
	ch <- struct{}{}
}
