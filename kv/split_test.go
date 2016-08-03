// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/retry"
)

// startTestWriter creates a writer which initiates a sequence of
// transactions, each which writes up to 10 times to random keys with
// random values. If not nil, txnChannel is written to non-blockingly
// every time a new transaction starts.
func startTestWriter(db *client.DB, i int64, valBytes int32, wg *sync.WaitGroup, retries *int32,
	txnChannel chan struct{}, done <-chan struct{}, t *testing.T) {
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
			err := db.Txn(func(txn *client.Txn) error {
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
					if err := txn.Put(key, val); err != nil {
						log.Infof(context.Background(), "experienced an error in routine %d: %s", i, err)
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
// which are resolved synchronously with EndTransaction and via RPC.
func TestRangeSplitMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := createTestDB(t)
	defer s.Stop()

	splitKeys := []roachpb.RKey{roachpb.RKey("G"), mustMeta(roachpb.RKey("F")),
		mustMeta(roachpb.RKey("K")), mustMeta(roachpb.RKey("H"))}

	// Execute the consecutive splits.
	for _, splitKey := range splitKeys {
		log.Infof(context.Background(), "starting split at key %q...", splitKey)
		if err := s.DB.AdminSplit(roachpb.Key(splitKey)); err != nil {
			t.Fatal(err)
		}
		log.Infof(context.Background(), "split at key %q complete", splitKey)
	}

	util.SucceedsSoon(t, func() error {
		if _, _, err := engine.MVCCScan(context.Background(), s.Eng, keys.LocalMax, roachpb.KeyMax, math.MaxInt64, hlc.MaxTimestamp, true, nil); err != nil {
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
	s, _ := createTestDB(t)
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

	// Execute the consecutive splits.
	for _, splitKey := range splitKeys {
		// Allow txns to start before initiating split.
		for i := 0; i < concurrency; i++ {
			<-txnChannel
		}
		log.Infof(context.Background(), "starting split at key %q...", splitKey)
		if pErr := s.DB.AdminSplit(splitKey); pErr != nil {
			t.Error(pErr)
		}
		log.Infof(context.Background(), "split at key %q complete", splitKey)
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
	// Override default zone config.
	cfg := config.DefaultZoneConfig()
	cfg.RangeMaxBytes = 1 << 18
	defer config.TestingSetDefaultZoneConfig(cfg)()

	dbCtx := client.DefaultDBContext()
	dbCtx.TxnRetryOptions = retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Multiplier:     2,
	}
	s, _ := createTestDBWithContext(t, dbCtx)
	// This is purely to silence log spam.
	config.TestingSetupZoneConfigHook(s.Stopper)
	defer s.Stop()

	// Start test writer write about a 32K/key so there aren't too many writes necessary to split 64K range.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go startTestWriter(s.DB, int64(0), 1<<15, &wg, nil, nil, done, t)

	// Check that we split 5 times in allotted time.
	util.SucceedsSoon(t, func() error {
		// Scan the txn records.
		rows, err := s.DB.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
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
	util.SucceedsSoon(t, func() error {
		if _, _, err := engine.MVCCScan(context.Background(), s.Eng, keys.LocalMax, roachpb.KeyMax, math.MaxInt64, hlc.MaxTimestamp, true, nil); err != nil {
			return errors.Errorf("failed to verify no dangling intents: %s", err)
		}
		return nil
	})
}

// TestRangeSplitsWithSameKeyTwice check that second range split
// on the same splitKey should not cause infinite retry loop.
func TestRangeSplitsWithSameKeyTwice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _ := createTestDB(t)
	defer s.Stop()

	splitKey := roachpb.Key("aa")
	log.Infof(context.Background(), "starting split at key %q...", splitKey)
	if err := s.DB.AdminSplit(splitKey); err != nil {
		t.Fatal(err)
	}
	log.Infof(context.Background(), "split at key %q first time complete", splitKey)
	ch := make(chan error)
	go func() {
		// should return error other than infinite loop
		ch <- s.DB.AdminSplit(splitKey)
	}()

	if err := <-ch; err == nil {
		t.Error("range split on same splitKey should fail")
	}
}
