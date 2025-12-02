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

func TestNodeContainerIsSupporting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts1 := hlc.Timestamp{WallTime: 100}
	ts2 := hlc.Timestamp{WallTime: 200}
	ts3 := hlc.Timestamp{WallTime: 300}

	testCases := []struct {
		name           string
		storeLiveness  []*mockFabric
		expSupporting  bool
		expWithdrawnTS hlc.Timestamp
	}{
		{
			name: "single store supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(true, ts1),
			},
			expSupporting:  true,
			expWithdrawnTS: ts1,
		},
		{
			name: "single store not supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(false, ts2),
			},
			expSupporting:  false,
			expWithdrawnTS: ts2,
		},
		{
			name: "all stores supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(true, ts1),
				newMockFabric(true, ts2),
				newMockFabric(true, ts3),
			},
			expSupporting:  true,
			expWithdrawnTS: ts3,
		},
		{
			name: "one store not supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(true, ts1),
				newMockFabric(false, ts2),
				newMockFabric(true, ts1),
			},
			expSupporting:  false,
			expWithdrawnTS: ts2,
		},
		{
			name: "all stores not supporting",
			storeLiveness: []*mockFabric{
				newMockFabric(false, ts1),
				newMockFabric(false, ts2),
				newMockFabric(false, ts3),
			},
			expSupporting:  false,
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

			gotSupporting, gotWithdrawnTS := nc.IsSupporting(slpb.StoreIdent{NodeID: 1, StoreID: 1})
			require.Equal(t, tc.expSupporting, gotSupporting)
			require.Equal(t, tc.expWithdrawnTS, gotWithdrawnTS)
		})
	}
}

type mockFabric struct {
	supporting  bool
	withdrawnTS hlc.Timestamp
}

var _ Fabric = (*mockFabric)(nil)

func newMockFabric(supporting bool, withdrawnTS hlc.Timestamp) *mockFabric {
	return &mockFabric{
		supporting:  supporting,
		withdrawnTS: withdrawnTS,
	}
}

func (m *mockFabric) IsSupporting(id slpb.StoreIdent) (bool, hlc.Timestamp) {
	return m.supporting, m.withdrawnTS
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
