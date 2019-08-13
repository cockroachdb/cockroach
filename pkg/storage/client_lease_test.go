// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestStoreRangeLease verifies that regular ranges (not some special ones at
// the start of the key space) get epoch-based range leases if enabled and
// expiration-based otherwise.
func TestStoreRangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "enableEpoch", func(t *testing.T, enableEpoch bool) {
		sc := storage.TestStoreConfig(nil)
		sc.TestingKnobs.DisableMergeQueue = true
		sc.EnableEpochRangeLeases = enableEpoch
		mtc := &multiTestContext{storeConfig: &sc}
		defer mtc.Stop()
		mtc.Start(t, 1)

		// NodeLivenessKeyMax is a static split point, so this is always
		// the start key of the first range that uses epoch-based
		// leases. Splitting on it here is redundant, but we want to include
		// it in our tests of lease types below.
		splitKeys := []roachpb.Key{
			keys.NodeLivenessKeyMax, roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c"),
		}
		for _, splitKey := range splitKeys {
			splitArgs := adminSplitArgs(splitKey)
			if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
				t.Fatal(pErr)
			}
		}

		rLeft := mtc.stores[0].LookupReplica(roachpb.RKeyMin)
		lease, _ := rLeft.GetLease()
		if lt := lease.Type(); lt != roachpb.LeaseExpiration {
			t.Fatalf("expected lease type expiration; got %d", lt)
		}

		// After the expiration, expect an epoch lease for all the ranges if
		// we've enabled epoch based range leases.
		for _, key := range splitKeys {
			repl := mtc.stores[0].LookupReplica(roachpb.RKey(key))
			lease, _ = repl.GetLease()
			if enableEpoch {
				if lt := lease.Type(); lt != roachpb.LeaseEpoch {
					t.Fatalf("expected lease type epoch; got %d", lt)
				}
			} else {
				if lt := lease.Type(); lt != roachpb.LeaseExpiration {
					t.Fatalf("expected lease type expiration; got %d", lt)
				}
			}
		}
	})
}

// TestStoreRangeLeaseSwitcheroo verifies that ranges can be switched
// between expiration and epoch and back.
func TestStoreRangeLeaseSwitcheroo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	sc.EnableEpochRangeLeases = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 1)

	splitKey := roachpb.Key("a")
	splitArgs := adminSplitArgs(splitKey)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Allow leases to expire and send commands to ensure we
	// re-acquire, then check types again.
	mtc.advanceClock(context.TODO())
	if _, err := mtc.dbs[0].Inc(context.TODO(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	// We started with epoch ranges enabled, so verify we have an epoch lease.
	repl := mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	lease, _ := repl.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseEpoch {
		t.Fatalf("expected lease type epoch; got %d", lt)
	}

	// Stop the store and reverse the epoch range lease setting.
	mtc.stopStore(0)
	sc.EnableEpochRangeLeases = false
	mtc.restartStore(0)

	mtc.advanceClock(context.TODO())
	if _, err := mtc.dbs[0].Inc(context.TODO(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	// Verify we end up with an expiration lease on restart.
	repl = mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	lease, _ = repl.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseExpiration {
		t.Fatalf("expected lease type expiration; got %d", lt)
	}

	// Now, one more time, switch back to epoch-based.
	mtc.stopStore(0)
	sc.EnableEpochRangeLeases = true
	mtc.restartStore(0)

	mtc.advanceClock(context.TODO())
	if _, err := mtc.dbs[0].Inc(context.TODO(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	// Verify we end up with an epoch lease on restart.
	repl = mtc.stores[0].LookupReplica(roachpb.RKey(splitKey))
	lease, _ = repl.GetLease()
	if lt := lease.Type(); lt != roachpb.LeaseEpoch {
		t.Fatalf("expected lease type epoch; got %d", lt)
	}
}

// TestStoreGossipSystemData verifies that the system-config and node-liveness
// data is gossiped at startup.
func TestStoreGossipSystemData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableMergeQueue = true
	sc.EnableEpochRangeLeases = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 1)

	splitKey := keys.SystemConfigSplitKey
	splitArgs := adminSplitArgs(splitKey)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}
	if _, err := mtc.dbs[0].Inc(context.TODO(), splitKey, 1); err != nil {
		t.Fatalf("failed to increment: %+v", err)
	}

	mtc.stopStore(0)

	getSystemConfig := func() *config.SystemConfig {
		systemConfig := mtc.gossips[0].GetSystemConfig()
		return systemConfig
	}
	getNodeLiveness := func() storagepb.Liveness {
		var liveness storagepb.Liveness
		if err := mtc.gossips[0].GetInfoProto(gossip.MakeNodeLivenessKey(1), &liveness); err == nil {
			return liveness
		}
		return storagepb.Liveness{}
	}

	// Clear the system-config and node liveness gossip data. This is necessary
	// because multiTestContext.restartStore reuse the Gossip structure.
	if err := mtc.gossips[0].AddInfoProto(
		gossip.KeySystemConfig, &config.SystemConfigEntries{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := mtc.gossips[0].AddInfoProto(
		gossip.MakeNodeLivenessKey(1), &storagepb.Liveness{}, 0); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		if !getSystemConfig().DefaultZoneConfig.Equal(sc.DefaultZoneConfig) {
			return errors.New("system config not empty")
		}
		if getNodeLiveness() != (storagepb.Liveness{}) {
			return errors.New("node liveness not empty")
		}
		return nil
	})

	// Restart the store and verify that both the system-config and node-liveness
	// data is gossiped.
	mtc.restartStore(0)
	testutils.SucceedsSoon(t, func() error {
		if !getSystemConfig().DefaultZoneConfig.Equal(sc.DefaultZoneConfig) {
			return errors.New("system config not gossiped")
		}
		if getNodeLiveness() == (storagepb.Liveness{}) {
			return errors.New("node liveness not gossiped")
		}
		return nil
	})
}

// TestGossipSystemConfigOnLeaseChange verifies that the system-config gets
// re-gossiped on lease transfer even if it hasn't changed. This helps prevent
// situations where a previous leaseholder can restart and not receive the
// system config because it was the original source of it within the gossip
// network.
func TestGossipSystemConfigOnLeaseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	const numStores = 3
	mtc.Start(t, numStores)

	rangeID := mtc.stores[0].LookupReplica(roachpb.RKey(keys.SystemConfigSpan.Key)).RangeID
	mtc.replicateRange(rangeID, 1, 2)

	initialStoreIdx := -1
	for i := range mtc.stores {
		if mtc.stores[i].Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			initialStoreIdx = i
		}
	}
	if initialStoreIdx == -1 {
		t.Fatalf("no store has gossiped system config; gossip contents: %+v", mtc.stores[0].Gossip().GetInfoStatus())
	}

	newStoreIdx := (initialStoreIdx + 1) % numStores
	mtc.transferLease(context.TODO(), rangeID, initialStoreIdx, newStoreIdx)

	testutils.SucceedsSoon(t, func() error {
		if mtc.stores[initialStoreIdx].Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			return errors.New("system config still most recently gossiped by original leaseholder")
		}
		if !mtc.stores[newStoreIdx].Gossip().InfoOriginatedHere(gossip.KeySystemConfig) {
			return errors.New("system config not most recently gossiped by new leaseholder")
		}
		return nil
	})
}

func TestGossipNodeLivenessOnLeaseChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	const numStores = 3
	mtc.Start(t, numStores)

	rangeID := mtc.stores[0].LookupReplica(roachpb.RKey(keys.NodeLivenessSpan.Key)).RangeID
	mtc.replicateRange(rangeID, 1, 2)

	// Turn off liveness heartbeats on all nodes to ensure that updates to node
	// liveness are not triggering gossiping.
	for i := range mtc.nodeLivenesses {
		mtc.nodeLivenesses[i].PauseHeartbeat(true)
	}

	nodeLivenessKey := gossip.MakeNodeLivenessKey(1)

	initialStoreIdx := -1
	for i := range mtc.stores {
		if mtc.stores[i].Gossip().InfoOriginatedHere(nodeLivenessKey) {
			initialStoreIdx = i
		}
	}
	if initialStoreIdx == -1 {
		t.Fatalf("no store has gossiped %s; gossip contents: %+v",
			nodeLivenessKey, mtc.stores[0].Gossip().GetInfoStatus())
	}
	log.Infof(context.Background(), "%s gossiped from n%d",
		nodeLivenessKey, mtc.stores[initialStoreIdx].Ident.NodeID)

	newStoreIdx := (initialStoreIdx + 1) % numStores
	mtc.transferLease(context.Background(), rangeID, initialStoreIdx, newStoreIdx)

	testutils.SucceedsSoon(t, func() error {
		if mtc.stores[initialStoreIdx].Gossip().InfoOriginatedHere(nodeLivenessKey) {
			return fmt.Errorf("%s still most recently gossiped by original leaseholder", nodeLivenessKey)
		}
		if !mtc.stores[newStoreIdx].Gossip().InfoOriginatedHere(nodeLivenessKey) {
			return fmt.Errorf("%s not most recently gossiped by new leaseholder", nodeLivenessKey)
		}
		return nil
	})
}
