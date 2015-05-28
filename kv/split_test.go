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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

// setTestRetryOptions sets client retry options for speedier testing.
func setTestRetryOptions() {
	client.DefaultTxnRetryOptions = retry.Options{
		Backoff:    1 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond,
		Constant:   2,
	}
}

// startTestWriter creates a writer which initiates a sequence of
// transactions, each which writes up to 10 times to random keys with
// random values. If not nil, txnChannel is written to every time a
// new transaction starts.
func startTestWriter(db *client.KV, i int64, valBytes int32, wg *sync.WaitGroup, retries *int32,
	txnChannel chan struct{}, done <-chan struct{}, t *testing.T) {
	src := rand.New(rand.NewSource(i))
	for j := 0; ; j++ {
		txnOpts := &client.TransactionOptions{Name: fmt.Sprintf("concurrent test %d:%d", i, j)}
		select {
		case <-done:
			if wg != nil {
				wg.Done()
			}
			return
		default:
			first := true
			err := db.RunTransaction(txnOpts, func(txn *client.Txn) error {
				if first && txnChannel != nil {
					txnChannel <- struct{}{}
				} else if !first && retries != nil {
					atomic.AddInt32(retries, 1)
				}
				first = false
				for j := 0; j <= int(src.Int31n(10)); j++ {
					key := util.RandBytes(src, 10)
					val := util.RandBytes(src, int(src.Int31n(valBytes)))
					if err := txn.Run(client.Put(key, val)); err != nil {
						log.Infof("experienced an error in routine %d: %s", i, err)
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

// TestRangeSplitsWithConcurrentTxns does 5 consecutive splits while
// 10 concurrent goroutines are each running successive transactions
// composed of a random mix of puts.
func TestRangeSplitsWithConcurrentTxns(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()

	// This channel shuts the whole apparatus down.
	done := make(chan struct{})
	txnChannel := make(chan struct{}, 1000)

	// Set five split keys, about evenly spaced along the range of random keys.
	splitKeys := []proto.Key{proto.Key("G"), proto.Key("R"), proto.Key("a"), proto.Key("l"), proto.Key("s")}

	// Start up the concurrent goroutines which run transactions.
	const concurrency = 10
	var retries int32
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go startTestWriter(s.KV, int64(i), 1<<7, &wg, &retries, txnChannel, done, t)
	}

	// Execute the consecutive splits.
	for _, splitKey := range splitKeys {
		// Allow txns to start before initiating split.
		for i := 0; i < concurrency; i++ {
			<-txnChannel
		}
		log.Infof("starting split at key %q...", splitKey)
		req := &proto.AdminSplitRequest{RequestHeader: proto.RequestHeader{Key: splitKey}, SplitKey: splitKey}
		resp := &proto.AdminSplitResponse{}
		if err := s.KV.Run(client.Call{Args: req, Reply: resp}); err != nil {
			t.Fatal(err)
		}
		log.Infof("split at key %q complete", splitKey)
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
	s := createTestDB(t)
	defer s.Stop()
	setTestRetryOptions()

	// Rewrite a zone config with low max bytes.
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{},
			{},
			{},
		},
		RangeMinBytes: 1 << 8,
		RangeMaxBytes: 1 << 18,
	}
	call := client.PutProto(keys.MakeKey(keys.ConfigZonePrefix, proto.KeyMin), zoneConfig)
	if err := s.KV.Run(call); err != nil {
		t.Fatal(err)
	}

	// Start test writer write about a 32K/key so there aren't too many writes necessary to split 64K range.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go startTestWriter(s.KV, int64(0), 1<<15, &wg, nil, nil, done, t)

	// Check that we split 5 times in allotted time.
	if err := util.IsTrueWithin(func() bool {
		// Scan the txn records.
		call := client.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
		resp := call.Reply.(*proto.ScanResponse)
		if err := s.KV.Run(call); err != nil {
			t.Fatalf("failed to scan meta2 keys: %s", err)
		}
		return len(resp.Rows) >= 5
	}, 6*time.Second); err != nil {
		t.Errorf("failed to split 5 times: %s", err)
	}
	close(done)
	wg.Wait()

	// This write pressure test often causes splits while resolve
	// intents are in flight, causing them to fail with range key
	// mismatch errors. However, LocalSender should retry in these
	// cases. Check here via MVCC scan that there are no dangling write
	// intents. We do this using an IsTrueWithin construct to account
	// for timing of finishing the test writer and a possibly-ongoing
	// asynchronous split.
	if err := util.IsTrueWithin(func() bool {
		if _, err := engine.MVCCScan(s.Eng, keys.LocalMax, proto.KeyMax, 0, proto.MaxTimestamp, true, nil); err != nil {
			log.Infof("mvcc scan should be clean: %s", err)
			return false
		}
		return true
	}, 500*time.Millisecond); err != nil {
		t.Error("failed to verify no dangling intents within 500ms")
	}
}

// TestRangeSplitsWithSameKeyTwice check that second range split
// on the same splitKey should not cause infinite retry loop.
func TestRangeSplitsWithSameKeyTwice(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()

	splitKey := proto.Key("aa")
	log.Infof("starting split at key %q...", splitKey)
	req := &proto.AdminSplitRequest{RequestHeader: proto.RequestHeader{Key: proto.Key("a")}, SplitKey: splitKey}
	resp := &proto.AdminSplitResponse{}
	if err := s.KV.Run(client.Call{Args: req, Reply: resp}); err != nil {
		t.Fatal(err)
	}
	log.Infof("split at key %q first time complete", splitKey)
	ch := make(chan struct{})
	go func() error {
		req := &proto.AdminSplitRequest{RequestHeader: proto.RequestHeader{Key: proto.Key("a")}, SplitKey: splitKey}
		resp := &proto.AdminSplitResponse{}
		// should return error other than infinite loop
		err := s.KV.Run(client.Call{Args: req, Reply: resp})
		if err == nil {
			errString := []byte("range split on same splitKey should failed on second time")
			t.Error(errString)
			return util.Error(errString)
		} else {
			close(ch)
			return err
		}
	}()

	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Error("range split on same splitKey is not returned in 500ms")
	}
}
