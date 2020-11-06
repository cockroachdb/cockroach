// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// startTestWriter creates a writer which initiates a sequence of
// transactions, each which writes up to 10 times to random keys with
// random values. If not nil, txnChannel is written to non-blockingly
// every time a new transaction starts.
func startTestWriter(
	db *kv.DB,
	i int64,
	valBytes int32,
	wg *sync.WaitGroup,
	retries *int32,
	txnChannel chan struct{},
	done <-chan struct{},
	t *testing.T,
) {
	src := rand.New(rand.NewSource(i))
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()

	for j := 0; ; j++ {
		select {
		case <-done:
			return
		default:
			first := true
			err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
				if first && txnChannel != nil {
					select {
					case txnChannel <- struct{}{}:
					default:
					}
				} else if !first && retries != nil {
					atomic.AddInt32(retries, 1)
				}
				first = false
				for j := 0; j <= int(src.Int31n(10)); j++ {
					key := randutil.RandBytes(src, 10)
					val := randutil.RandBytes(src, int(src.Int31n(valBytes)))
					if err := txn.Put(ctx, key, val); err != nil {
						log.Infof(ctx, "experienced an error in routine %d: %s", i, err)
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Error(err)
			} else {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

// TestRangeSplitMeta executes various splits (including at meta addressing)
// and checks that all created intents are resolved. This includes both intents
// which are resolved synchronously with EndTxn and via RPC.
func TestRangeSplitMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()

	ctx := context.Background()

	splitKeys := []roachpb.RKey{roachpb.RKey("G"), keys.RangeMetaKey(roachpb.RKey("F")),
		keys.RangeMetaKey(roachpb.RKey("K")), keys.RangeMetaKey(roachpb.RKey("H"))}

	// Execute the consecutive splits.
	for _, splitRKey := range splitKeys {
		splitKey := roachpb.Key(splitRKey)
		log.Infof(ctx, "starting split at key %q...", splitKey)
		if err := s.DB.AdminSplit(ctx, splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
			t.Fatal(err)
		}
		log.Infof(ctx, "split at key %q complete", splitKey)
	}

	testutils.SucceedsSoon(t, func() error {
		if _, err := storage.MVCCScan(ctx, s.Eng, keys.LocalMax, roachpb.KeyMax, hlc.MaxTimestamp, storage.MVCCScanOptions{}); err != nil {
			return errors.Errorf("failed to verify no dangling intents: %s", err)
		}
		return nil
	})
}

// TestRangeSplitsWithConcurrentTxns does 5 consecutive splits while
// 10 concurrent goroutines are each running successive transactions
// composed of a random mix of puts.
func TestRangeSplitsWithConcurrentTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()

	// This channel shuts the whole apparatus down.
	done := make(chan struct{})
	txnChannel := make(chan struct{}, 1000)

	// Set five split keys, about evenly spaced along the range of random keys.
	splitKeys := []roachpb.Key{roachpb.Key("G"), roachpb.Key("R"), roachpb.Key("a"), roachpb.Key("l"), roachpb.Key("s")}

	// Start up the concurrent goroutines which run transactions.
	const concurrency = 10
	var retries int32
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go startTestWriter(s.DB, int64(i), 1<<7, &wg, &retries, txnChannel, done, t)
	}

	ctx := context.Background()
	// Execute the consecutive splits.
	for _, splitKey := range splitKeys {
		// Allow txns to start before initiating split.
		for i := 0; i < concurrency; i++ {
			<-txnChannel
		}
		log.Infof(ctx, "starting split at key %q...", splitKey)
		if pErr := s.DB.AdminSplit(context.Background(), splitKey, hlc.MaxTimestamp /* expirationTime */); pErr != nil {
			t.Error(pErr)
		}
		log.Infof(ctx, "split at key %q complete", splitKey)
	}

	close(done)
	wg.Wait()

	if retries != 0 {
		t.Errorf("expected no retries splitting a range with concurrent writes, "+
			"as range splits do not cause conflicts; got %d", retries)
	}
}

// TestRangeSplitsWithWritePressure sets the zone config max bytes for
// a range to 256K and writes data until there are five ranges.
func TestRangeSplitsWithWritePressure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Override default zone config.
	cfg := zonepb.DefaultZoneConfigRef()
	cfg.RangeMaxBytes = proto.Int64(1 << 18)

	// Manually create the local test cluster so that the split queue
	// is not disabled (LocalTestCluster disables it by default).
	s := &localtestcluster.LocalTestCluster{
		Cfg: kvserver.StoreConfig{
			DefaultZoneConfig: cfg,
		},
		StoreTestingKnobs: &kvserver.StoreTestingKnobs{
			DisableScanner: true,
		},
	}
	s.Start(t, testutils.NewNodeTestBaseContext(), InitFactoryForLocalTestCluster)

	// This is purely to silence log spam.
	config.TestingSetupZoneConfigHook(s.Stopper())
	defer s.Stop()

	// Start test writer write about a 32K/key so there aren't too many
	// writes necessary to split 5 ranges.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go startTestWriter(s.DB, int64(0), 1<<15, &wg, nil, nil, done, t)

	ctx := context.Background()

	// Check that we split 5 times in allotted time.
	testutils.SucceedsSoon(t, func() error {
		// Scan the txn records.
		rows, err := s.DB.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
		if err != nil {
			return errors.Errorf("failed to scan meta2 keys: %s", err)
		}
		if lr := len(rows); lr < 5 {
			return errors.Errorf("expected >= 5 scans; got %d", lr)
		}
		return nil
	})
	close(done)
	wg.Wait()

	// This write pressure test often causes splits while resolve
	// intents are in flight, causing them to fail with range key
	// mismatch errors. However, LocalSender should retry in these
	// cases. Check here via MVCC scan that there are no dangling write
	// intents. We do this using a SucceedsSoon construct to account
	// for timing of finishing the test writer and a possibly-ongoing
	// asynchronous split.
	testutils.SucceedsSoon(t, func() error {
		if _, err := storage.MVCCScan(ctx, s.Eng, keys.LocalMax, roachpb.KeyMax, hlc.MaxTimestamp, storage.MVCCScanOptions{}); err != nil {
			return errors.Errorf("failed to verify no dangling intents: %s", err)
		}
		return nil
	})
}

// TestRangeSplitsWithSameKeyTwice check that second range split
// on the same splitKey succeeds.
func TestRangeSplitsWithSameKeyTwice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDBWithKnobs(t, &kvserver.StoreTestingKnobs{
		DisableScanner:    true,
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	})
	defer s.Stop()

	ctx := context.Background()

	splitKey := roachpb.Key("aa")
	log.Infof(ctx, "starting split at key %q...", splitKey)
	if err := s.DB.AdminSplit(ctx, splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
	log.Infof(ctx, "split at key %q first time complete", splitKey)
	if err := s.DB.AdminSplit(ctx, splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
}

// TestSplitStickyBit checks that the sticky bit is set when performing a manual
// split. There are two cases to consider:
// 1. Range is split so sticky bit is updated on RHS.
// 2. Range is already split and split key is the start key of a range, so update
//    the sticky bit of that range, but no range is split.
func TestRangeSplitsStickyBit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDBWithKnobs(t, &kvserver.StoreTestingKnobs{
		DisableScanner:    true,
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	})
	defer s.Stop()

	ctx := context.Background()
	splitKey := roachpb.RKey("aa")
	descKey := keys.RangeDescriptorKey(splitKey)

	// Splitting range.
	if err := s.DB.AdminSplit(ctx, splitKey.AsRawKey(), hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// Checking sticky bit.
	var desc roachpb.RangeDescriptor
	err := s.DB.GetProto(ctx, descKey, &desc)
	if err != nil {
		t.Fatal(err)
	}
	if desc.GetStickyBit().IsEmpty() {
		t.Fatal("Sticky bit not set after splitting")
	}

	// Removing sticky bit.
	if err := s.DB.AdminUnsplit(ctx, splitKey.AsRawKey()); err != nil {
		t.Fatal(err)
	}

	// Splitting range.
	if err := s.DB.AdminSplit(ctx, splitKey.AsRawKey(), hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// Checking sticky bit.
	err = s.DB.GetProto(ctx, descKey, &desc)
	if err != nil {
		t.Fatal(err)
	}
	if desc.GetStickyBit().IsEmpty() {
		t.Fatal("Sticky bit not set after splitting")
	}
}
