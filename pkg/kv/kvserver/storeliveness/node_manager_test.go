// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"testing"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNodeContainerSupportState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts1 := hlc.ClockTimestamp{WallTime: 100}
	ts2 := hlc.ClockTimestamp{WallTime: 200}
	ts3 := hlc.ClockTimestamp{WallTime: 300}

	testCases := []struct {
		name           string
		storeLiveness  []*mockFabric
		expState       SupportState
		expWithdrawnTS hlc.ClockTimestamp
	}{
		{
			name: "single store supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, ts1),
			},
			expState:       StateSupporting,
			expWithdrawnTS: ts1,
		},
		{
			name: "single store not supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(StateNotSupporting, ts2),
			},
			expState:       StateNotSupporting,
			expWithdrawnTS: ts2,
		},
		{
			name: "single store unknown",
			storeLiveness: []*mockFabric{
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
			},
			expState:       StateUnknown,
			expWithdrawnTS: hlc.ClockTimestamp{},
		},
		{
			name: "single store supporting never withdrawn",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, hlc.ClockTimestamp{}),
			},
			expState:       StateSupporting,
			expWithdrawnTS: hlc.ClockTimestamp{},
		},
		{
			name: "multiple stores supporting some never withdrawn",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, hlc.ClockTimestamp{}),
				newMockFabric(StateSupporting, ts1),
				newMockFabric(StateSupporting, hlc.ClockTimestamp{}),
			},
			expState:       StateSupporting,
			expWithdrawnTS: ts1,
		},
		{
			name: "multiple stores supporting all never withdrawn",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, hlc.ClockTimestamp{}),
				newMockFabric(StateSupporting, hlc.ClockTimestamp{}),
				newMockFabric(StateSupporting, hlc.ClockTimestamp{}),
			},
			expState:       StateSupporting,
			expWithdrawnTS: hlc.ClockTimestamp{},
		},
		{
			name: "all stores supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, ts1),
				newMockFabric(StateSupporting, ts2),
				newMockFabric(StateSupporting, ts3),
			},
			expState:       StateSupporting,
			expWithdrawnTS: ts3,
		},
		{
			name: "one store not supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, ts1),
				newMockFabric(StateNotSupporting, ts2),
				newMockFabric(StateSupporting, ts1),
			},
			expState:       StateNotSupporting,
			expWithdrawnTS: ts2,
		},
		{
			name: "all stores not supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(StateNotSupporting, ts1),
				newMockFabric(StateNotSupporting, ts2),
				newMockFabric(StateNotSupporting, ts3),
			},
			expState:       StateNotSupporting,
			expWithdrawnTS: ts3,
		},
		{
			name: "mix of supporting and unknown",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, ts1),
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
				newMockFabric(StateSupporting, ts2),
			},
			expState:       StateSupporting,
			expWithdrawnTS: ts2,
		},
		{
			name: "mix of not supporting and unknown",
			storeLiveness: []*mockFabric{
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
				newMockFabric(StateNotSupporting, ts2),
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
			},
			expState:       StateNotSupporting,
			expWithdrawnTS: ts2,
		},
		{
			name: "all stores unknown",
			storeLiveness: []*mockFabric{
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
			},
			expState:       StateUnknown,
			expWithdrawnTS: hlc.ClockTimestamp{},
		},
		{
			name: "mix of all three states",
			storeLiveness: []*mockFabric{
				newMockFabric(StateSupporting, ts1),
				newMockFabric(StateUnknown, hlc.ClockTimestamp{}),
				newMockFabric(StateNotSupporting, ts3),
				newMockFabric(StateSupporting, ts2),
			},
			expState:       StateNotSupporting,
			expWithdrawnTS: ts3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nc := &NodeContainer{}
			nc.mu.supportManagers = make(map[roachpb.StoreID]Fabric)

			for i, sl := range tc.storeLiveness {
				storeID := roachpb.StoreID(i + 1)
				nc.mu.supportManagers[storeID] = sl
			}

			gotState, gotWithdrawnTS := nc.SupportState(slpb.StoreIdent{NodeID: 1, StoreID: 1})
			require.Equal(t, tc.expState, gotState)
			require.Equal(t, tc.expWithdrawnTS, gotWithdrawnTS)
		})
	}
}

type mockFabric struct {
	state       SupportState
	withdrawnTS hlc.ClockTimestamp
}

var _ Fabric = (*mockFabric)(nil)

func newMockFabric(state SupportState, withdrawnTS hlc.ClockTimestamp) *mockFabric {
	return &mockFabric{
		state:       state,
		withdrawnTS: withdrawnTS,
	}
}

func (m *mockFabric) SupportState(id slpb.StoreIdent) (SupportState, hlc.ClockTimestamp) {
	return m.state, m.withdrawnTS
}

func (m *mockFabric) SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool) {
	panic("unimplemented")
}

func (m *mockFabric) SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp) {
	panic("unimplemented")
}

func (m *mockFabric) SupportFromEnabled(ctx context.Context) bool {
	panic("unimplemented")
}

func (m *mockFabric) RegisterSupportWithdrawalCallback(func(map[roachpb.StoreID]struct{})) {
	panic("unimplemented")
}

func (m *mockFabric) InspectSupportFrom() slpb.InspectSupportFromStatesPerStore {
	panic("unimplemented")
}

func (m *mockFabric) InspectSupportFor() slpb.InspectSupportForStatesPerStore {
	panic("unimplemented")
}
