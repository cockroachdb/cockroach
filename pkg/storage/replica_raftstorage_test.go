// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"math/rand"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const rangeID = 1
const keySize = 1 << 7  // 128 B
const valSize = 1 << 10 // 1 KiB

func fillTestRange(rep *Replica, size int64) error {
	src := rand.New(rand.NewSource(0))
	for i := int64(0); i < size/int64(keySize+valSize); i++ {
		key := keys.MakeRowSentinelKey(randutil.RandBytes(src, keySize))
		val := randutil.RandBytes(src, valSize)
		pArgs := putArgs(key, val)
		if _, pErr := client.SendWrappedWith(context.Background(), rep, roachpb.Header{
			RangeID: rangeID,
		}, &pArgs); pErr != nil {
			return pErr.GoError()
		}
	}
	rep.mu.Lock()
	after := rep.mu.state.Stats.Total()
	rep.mu.Unlock()
	if after < size {
		return errors.Errorf("range not full after filling: wrote %d, but range at %d", size, after)
	}
	return nil
}

func TestSkipLargeReplicaSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true

	const snapSize = 5 * (keySize + valSize)
	cfg := config.DefaultZoneConfig()
	cfg.RangeMaxBytes = snapSize
	defer config.TestingSetDefaultZoneConfig(cfg)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, &storeCfg)

	rep, err := store.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	rep.SetMaxBytes(snapSize)

	if _, pErr := rep.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	if err := fillTestRange(rep, snapSize); err != nil {
		t.Fatal(err)
	}

	if snap, err := rep.GetSnapshot(context.Background(), "test"); err != nil {
		t.Fatal(err)
	} else {
		snap.Close()
	}

	if err := fillTestRange(rep, snapSize*2); err != nil {
		t.Fatal(err)
	}

	const expected = "not generating test snapshot because replica is too large"
	if _, err := rep.GetSnapshot(context.Background(), "test"); !testutils.IsError(err, expected) {
		rep.mu.Lock()
		after := rep.mu.state.Stats.Total()
		rep.mu.Unlock()
		t.Fatalf(
			"snapshot of a very large range (%d / %d, needsSplit: %v, exceeds snap limit: %v) should fail but got %v",
			after, rep.GetMaxBytes(),
			rep.needsSplitBySize(), rep.exceedsDoubleSplitSizeRLocked(), err,
		)
	}
}
