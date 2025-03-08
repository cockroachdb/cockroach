// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sidetransport

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
	lai      kvpb.LeaseAppliedIndex
}

var _ Stores = &mockStores{}

func (m *mockStores) ForwardSideTransportClosedTimestampForRange(
	ctx context.Context, rangeID roachpb.RangeID, closedTS hlc.Timestamp, lai kvpb.LeaseAppliedIndex,
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
var ts15 = hlc.Timestamp{WallTime: 15}
var ts16 = hlc.Timestamp{WallTime: 16}
var ts17 = hlc.Timestamp{WallTime: 17}
var ts18 = hlc.Timestamp{WallTime: 18}
var ts19 = hlc.Timestamp{WallTime: 19}
var ts20 = hlc.Timestamp{WallTime: 20}
var ts21 = hlc.Timestamp{WallTime: 21}
var ts22 = hlc.Timestamp{WallTime: 22}
var ts25 = hlc.Timestamp{WallTime: 25}
var ts26 = hlc.Timestamp{WallTime: 26}
var ts27 = hlc.Timestamp{WallTime: 27}
var laiZero = kvpb.LeaseAppliedIndex(0)

const lai100 = kvpb.LeaseAppliedIndex(100)
const lai101 = kvpb.LeaseAppliedIndex(101)
const lai102 = kvpb.LeaseAppliedIndex(102)
const lai103 = kvpb.LeaseAppliedIndex(103)

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
			{Policy: ctpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: ts10},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY, ClosedTimestamp: ts20},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE, ClosedTimestamp: ts15},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE, ClosedTimestamp: ts18},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION, ClosedTimestamp: ts25},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 1, LAI: lai100, Policy: ctpb.LAG_BY_CLUSTER_SETTING},
			{RangeID: 2, LAI: lai101, Policy: ctpb.LAG_BY_CLUSTER_SETTING},
			{RangeID: 3, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY},
			{RangeID: 4, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION},
			{RangeID: 5, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE},
			{RangeID: 6, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE},
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
	ts, lai = r.GetClosedTimestamp(ctx, 4)
	require.Equal(t, ts25, ts)
	require.Equal(t, lai102, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 5)
	require.Equal(t, ts18, ts)
	require.Equal(t, lai102, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 6)
	require.Equal(t, ts15, ts)
	require.Equal(t, lai102, lai)
	require.Empty(t, stores.getAndClearRecording())

	// Remove range 1, update 2 implicitly, update 3 (LAI), 4,6(locality) explicitly.
	msg = &ctpb.Update{
		NodeID:   1,
		SeqNum:   2,
		Snapshot: false,
		ClosedTimestamps: []ctpb.Update_GroupUpdate{
			{Policy: ctpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: ts11},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY, ClosedTimestamp: ts21},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE, ClosedTimestamp: ts16},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE, ClosedTimestamp: ts19},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION, ClosedTimestamp: ts26},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 3, LAI: lai103, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY},
			{RangeID: 4, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE},
			{RangeID: 6, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION},
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
	ts, lai = r.GetClosedTimestamp(ctx, 4)
	require.Equal(t, ts16, ts)
	require.Equal(t, lai102, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 5)
	require.Equal(t, ts19, ts)
	require.Equal(t, lai102, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 6)
	require.Equal(t, ts26, ts)
	require.Equal(t, lai102, lai)
	require.Equal(t, []rangeUpdate{{rid: 1, closedTS: ts10, lai: lai100}}, stores.getAndClearRecording())

	// Send a snapshot and check that it rests all the state.
	msg = &ctpb.Update{
		NodeID:   1,
		SeqNum:   3,
		Snapshot: true,
		ClosedTimestamps: []ctpb.Update_GroupUpdate{
			{Policy: ctpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: ts12},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY, ClosedTimestamp: ts22},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE, ClosedTimestamp: ts17},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE, ClosedTimestamp: ts20},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION, ClosedTimestamp: ts27},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 3, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY},
			{RangeID: 4, LAI: lai100, Policy: ctpb.LAG_BY_CLUSTER_SETTING},
			{RangeID: 6, LAI: lai102, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY},
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
	ts, lai = r.GetClosedTimestamp(ctx, 5)
	require.Empty(t, ts)
	require.Equal(t, laiZero, lai)
	ts, lai = r.GetClosedTimestamp(ctx, 6)
	require.Equal(t, ts22, ts)
	require.Equal(t, lai102, lai)
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
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY, ClosedTimestamp: ts10},
		},
		AddedOrUpdated: []ctpb.Update_RangeUpdate{
			{RangeID: 1, LAI: lai100, Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY},
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
