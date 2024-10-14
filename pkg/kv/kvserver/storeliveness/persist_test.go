// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"testing"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPersistReadAndWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()
	rm := slpb.RequesterMeta{MaxEpoch: 3, MaxRequested: hlc.Timestamp{WallTime: 100, Logical: 1}}
	sm := slpb.SupporterMeta{MaxWithdrawn: hlc.ClockTimestamp{WallTime: 200, Logical: 2}}
	ss1 := slpb.SupportState{
		Target:     slpb.StoreIdent{NodeID: 1, StoreID: 1},
		Epoch:      1,
		Expiration: hlc.Timestamp{WallTime: 300, Logical: 3},
	}
	ss2 := slpb.SupportState{
		Target:     slpb.StoreIdent{NodeID: 2, StoreID: 2},
		Epoch:      2,
		Expiration: hlc.Timestamp{WallTime: 400, Logical: 4},
	}

	// Write RequesterMeta, read it, and make sure it's identical to the original.
	if err := writeRequesterMeta(ctx, engine, rm); err != nil {
		t.Errorf("writing RequesterMeta failed: %v", err)
	}
	rmRead, err := readRequesterMeta(ctx, engine)
	if err != nil {
		t.Errorf("reading RequesterMeta failed: %v", err)
	}
	require.Equal(t, rm, rmRead)

	// Write SupporterMeta, read it, and make sure it's identical to the original.
	if err := writeSupporterMeta(ctx, engine, sm); err != nil {
		t.Errorf("writing SupporterMeta failed: %v", err)
	}
	smRead, err := readSupporterMeta(ctx, engine)
	if err != nil {
		t.Errorf("reading SupporterMeta failed: %v", err)
	}
	require.Equal(t, sm, smRead)

	// Write two SupporterStates, read them, and make sure they're identical to
	// the original ones.
	if err := writeSupportForState(ctx, engine, ss1); err != nil {
		t.Errorf("writing SupportState failed: %v", err)
	}
	if err := writeSupportForState(ctx, engine, ss2); err != nil {
		t.Errorf("writing SupportState failed: %v", err)
	}
	ssRead, err := readSupportForState(ctx, engine)
	if err != nil {
		t.Errorf("reading SupportState failed: %v", err)
	}
	require.Equal(t, ss1, ssRead[0])
	require.Equal(t, ss2, ssRead[1])
}
