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

/* Package client_test tests clients against a fully-instantiated
cockroach cluster (a single node, but bootstrapped, gossiped, etc.).
*/
package client_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/retry"
	gogoproto "github.com/gogo/protobuf/proto"
)

// testUser has a permissions config for the `TestUser` prefix.
var testUser = server.TestUser

// notifyingSender is a sender which can set up a notification channel
// (on call to reset()) for clients which need to wait on a command
// being sent.
type notifyingSender struct {
	waiter  *sync.WaitGroup
	wrapped client.Sender
}

func (ss *notifyingSender) reset(waiter *sync.WaitGroup) {
	waiter.Add(1)
	ss.waiter = waiter
}

func (ss *notifyingSender) wait() {
	ss.waiter.Wait()
	ss.waiter = nil
}

func (ss *notifyingSender) Send(ctx context.Context, call proto.Call) {
	ss.wrapped.Send(ctx, call)
	if ss.waiter != nil {
		ss.waiter.Done()
	}
}

func createTestClient(addr string) *client.DB {
	return createTestClientFor(addr, testUser)
}

func createTestClientFor(addr, user string) *client.DB {
	db, err := client.Open("https://" + user + "@" + addr + "?certs=" + security.EmbeddedCertsDir)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

// createTestNotifyClient creates a new client which connects using an HTTP
// sender to the server at addr. It contains a waitgroup to allow waiting.
func createTestNotifyClient(addr string, priority int) (*client.DB, *notifyingSender) {
	db, err := client.Open(fmt.Sprintf("https://root@%s?certs=%s&priority=%d",
		addr, security.EmbeddedCertsDir, priority))
	if err != nil {
		log.Fatal(err)
	}
	sender := &notifyingSender{wrapped: db.Sender}
	db.Sender = sender
	return db, sender
}

// TestClientRetryNonTxn verifies that non-transactional client will
// succeed despite write/write and read/write conflicts. In the case
// where the non-transactional put can push the txn, we expect the
// transaction's value to be written after all retries are complete.
func TestClientRetryNonTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	s.SetRangeRetryOptions(retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		Multiplier:     2,
		MaxRetries:     1,
	})

	testCases := []struct {
		args        proto.Request
		isolation   proto.IsolationType
		canPush     bool
		expAttempts int
	}{
		// Write/write conflicts.
		{&proto.PutRequest{}, proto.SNAPSHOT, true, 2},
		{&proto.PutRequest{}, proto.SERIALIZABLE, true, 2},
		{&proto.PutRequest{}, proto.SNAPSHOT, false, 1},
		{&proto.PutRequest{}, proto.SERIALIZABLE, false, 1},
		// Read/write conflicts.
		{&proto.GetRequest{}, proto.SNAPSHOT, true, 1},
		{&proto.GetRequest{}, proto.SERIALIZABLE, true, 2},
		{&proto.GetRequest{}, proto.SNAPSHOT, false, 1},
		{&proto.GetRequest{}, proto.SERIALIZABLE, false, 1},
	}
	// Lay down a write intent using a txn and attempt to write to same
	// key. Try this twice--once with priorities which will allow the
	// intent to be pushed and once with priorities which will not.
	for i, test := range testCases {
		key := proto.Key(fmt.Sprintf("key-%d", i))
		txnPri := 1
		clientPri := 1
		if test.canPush {
			clientPri = 2
		} else {
			txnPri = 2
		}

		db, sender := createTestNotifyClient(s.ServingAddr(), -clientPri)

		// doneCall signals when the non-txn read or write has completed.
		doneCall := make(chan struct{})
		count := 0 // keeps track of retries
		err := db.Txn(func(txn *client.Txn) error {
			if test.isolation == proto.SNAPSHOT {
				txn.SetSnapshotIsolation()
			}
			txn.InternalSetPriority(int32(txnPri))

			count++
			// Lay down the intent.
			if err := txn.Put(key, "txn-value"); err != nil {
				return err
			}
			// The wait group lets us pause txn until after the non-txn method has run once.
			wg := sync.WaitGroup{}
			// On the first true, send the non-txn put or get.
			if count == 1 {
				// We use a "notifying" sender here, which allows us to know exactly when the
				// call has been processed; otherwise, we'd be dependent on timing.
				sender.reset(&wg)
				// We must try the non-txn put or get in a goroutine because
				// it might have to retry and will only succeed immediately in
				// the event we can push.
				go func() {
					var err error
					for i := 0; ; i++ {
						if _, ok := test.args.(*proto.GetRequest); ok {
							_, err = db.Get(key)
						} else {
							err = db.Put(key, "value")
						}
						if _, ok := err.(*proto.WriteIntentError); !ok {
							break
						}
					}
					close(doneCall)
					if err != nil {
						t.Fatalf("%d: expected success on non-txn call to %s; got %s", i, test.args.Method(), err)
					}
				}()
				sender.wait()
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%d: expected success writing transactionally; got %s", i, err)
		}

		// Make sure non-txn put or get has finished.
		<-doneCall

		// Get the current value to verify whether the txn happened first.
		gr, err := db.Get(key)
		if err != nil {
			t.Fatalf("%d: expected success getting %q: %s", i, key, err)
		}

		if _, isGet := test.args.(*proto.GetRequest); isGet || test.canPush {
			if !bytes.Equal(gr.ValueBytes(), []byte("txn-value")) {
				t.Errorf("%d: expected \"txn-value\"; got %q", i, gr.ValueBytes())
			}
		} else {
			if !bytes.Equal(gr.ValueBytes(), []byte("value")) {
				t.Errorf("%d: expected \"value\"; got %q", i, gr.ValueBytes())
			}
		}
		if count != test.expAttempts {
			t.Errorf("%d: expected %d attempt(s); got %d", i, test.expAttempts, count)
		}
	}
}

func setTxnRetryBackoff(backoff time.Duration) func() {
	savedBackoff := client.DefaultTxnRetryOptions.InitialBackoff
	client.DefaultTxnRetryOptions.InitialBackoff = backoff
	return func() {
		client.DefaultTxnRetryOptions.InitialBackoff = savedBackoff
	}
}

// TestClientRunTransaction verifies some simple transaction isolation
// semantics.
func TestClientRunTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	defer setTxnRetryBackoff(1 * time.Millisecond)()
	db := createTestClient(s.ServingAddr())

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		key := []byte(fmt.Sprintf("%s/key-%t", testUser, commit))

		// Use snapshot isolation so non-transactional read can always push.
		err := db.Txn(func(txn *client.Txn) error {
			txn.SetSnapshotIsolation()

			// Put transactional value.
			if err := txn.Put(key, value); err != nil {
				return err
			}
			// Attempt to read outside of txn.
			if gr, err := db.Get(key); err != nil {
				return err
			} else if gr.Value != nil {
				return util.Errorf("expected nil value; got %+v", gr.Value)
			}
			// Read within the transaction.
			if gr, err := txn.Get(key); err != nil {
				return err
			} else if gr.Value == nil || !bytes.Equal(gr.ValueBytes(), value) {
				return util.Errorf("expected value %q; got %q", value, gr.ValueBytes())
			}
			if !commit {
				return errors.New("purposefully failing transaction")
			}
			return nil
		})

		if commit != (err == nil) {
			t.Errorf("expected success? %t; got %s", commit, err)
		} else if !commit && err.Error() != "purposefully failing transaction" {
			t.Errorf("unexpected failure with !commit: %s", err)
		}

		// Verify the value is now visible on commit == true, and not visible otherwise.
		gr, err := db.Get(key)
		if commit {
			if err != nil || gr.Value == nil || !bytes.Equal(gr.ValueBytes(), value) {
				t.Errorf("expected success reading value: %+v, %s", gr.Value, err)
			}
		} else {
			if err != nil || gr.Value != nil {
				t.Errorf("expected success and nil value: %+v, %s", gr.Value, err)
			}
		}
	}
}

// TestClientGetAndPutProto verifies gets and puts of protobufs using the
// client's convenience methods.
func TestClientGetAndPutProto(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(s.ServingAddr())

	zoneConfig := &config.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{Attrs: []string{"dc1", "mem"}},
			{Attrs: []string{"dc2", "mem"}},
		},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
	}

	key := proto.Key(testUser + "/zone-config")
	if err := db.Put(key, zoneConfig); err != nil {
		t.Fatalf("unable to put proto: %s", err)
	}

	readZoneConfig := &config.ZoneConfig{}
	if err := db.GetProto(key, readZoneConfig); err != nil {
		t.Fatalf("unable to get proto: %v", err)
	}
	if !gogoproto.Equal(zoneConfig, readZoneConfig) {
		t.Errorf("expected %+v, but found %+v", zoneConfig, readZoneConfig)
	}
}

// TestClientGetAndPut verifies gets and puts of using the client's convenience
// methods.
func TestClientGetAndPut(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(s.ServingAddr())

	value := []byte("value")
	if err := db.Put(testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(testUser + "/key")
	if err != nil {
		t.Fatalf("unable to get value: %v", err)
	}
	zero := time.Time{}
	if zero == gr.Timestamp {
		t.Error("expected non-zero timestamp")
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
}

// TestClientEmptyValues verifies that empty values are preserved
// for both empty []byte and integer=0. This used to fail when we
// allowed the protobufs to be gob-encoded using the default go rpc
// gob codec because gob treats pointer values and non-pointer values
// as equivalent and elides zero-valued defaults on decode.
func TestClientEmptyValues(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(s.ServingAddr())

	if err := db.Put(testUser+"/a", []byte{}); err != nil {
		t.Error(err)
	}
	if gr, err := db.Get(testUser + "/a"); err != nil {
		t.Error(err)
	} else if bytes := gr.ValueBytes(); bytes == nil || len(bytes) != 0 {
		t.Errorf("expected non-nil empty byte slice; got %q", bytes)
	}

	if _, err := db.Inc(testUser+"/b", 0); err != nil {
		t.Error(err)
	}
	if gr, err := db.Get(testUser + "/b"); err != nil {
		t.Error(err)
	} else if gr.Value == nil {
		t.Errorf("expected non-nil integer")
	} else if gr.ValueInt() != 0 {
		t.Errorf("expected 0-valued integer, but got %d", gr.ValueInt())
	}
}

// TestClientBatch runs a batch of increment calls and then verifies the
// results.
func TestClientBatch(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(s.ServingAddr())

	keys := []proto.Key{}
	b := &client.Batch{}
	for i := 0; i < 10; i++ {
		key := proto.Key(fmt.Sprintf("%s/key %02d", testUser, i))
		keys = append(keys, key)
		b.Inc(key, int64(i))
	}

	if err := db.Run(b); err != nil {
		t.Error(err)
	}

	for i, result := range b.Results {
		if v := result.Rows[0].ValueInt(); v != int64(i) {
			t.Errorf("%d: expected %d; got %d", i, i, v)
		}
	}

	// Now try 2 scans.
	b = &client.Batch{}
	b.Scan(testUser+"/key 00", testUser+"/key 05", 0)
	b.Scan(testUser+"/key 05", testUser+"/key 10", 0)
	if err := db.Run(b); err != nil {
		t.Error(err)
	}

	scan1 := b.Results[0].Rows
	scan2 := b.Results[1].Rows
	if len(scan1) != 5 || len(scan2) != 5 {
		t.Errorf("expected scan results to include 5 and 5 rows; got %d and %d",
			len(scan1), len(scan2))
	}
	for i := 0; i < 5; i++ {
		if key := proto.Key(scan1[i].Key); !key.Equal(keys[i]) {
			t.Errorf("expected scan1 key %d to be %q; got %q", i, keys[i], key)
		}
		if val := scan1[i].ValueInt(); val != int64(i) {
			t.Errorf("expected scan1 result %d to be %d; got %d", i, i, val)
		}
		if key := proto.Key(scan2[i].Key); !key.Equal(keys[i+5]) {
			t.Errorf("expected scan2 key %d to be %q; got %q", i, keys[i+5], key)
		}
		if val := scan2[i].ValueInt(); val != int64(i+5) {
			t.Errorf("expected scan2 result %d to be %d; got %d", i, i+5, val)
		}
	}

	// Try 2 reverse scans.
	b = &client.Batch{}
	b.ReverseScan(testUser+"/key 00", testUser+"/key 05", 0)
	b.ReverseScan(testUser+"/key 05", testUser+"/key 10", 0)
	if err := db.Run(b); err != nil {
		t.Error(err)
	}

	revScan1 := b.Results[0].Rows
	revScan2 := b.Results[1].Rows
	expectedCount := 5
	rev1TopIndex := 4
	rev2TopIndex := 9
	if len(revScan1) != expectedCount || len(revScan2) != expectedCount {
		t.Errorf("expected reverse scan results to include 5 and 5 rows; got %d and %d",
			len(revScan1), len(revScan2))
	}
	for i := 0; i < expectedCount; i++ {
		if key := proto.Key(revScan1[i].Key); !key.Equal(keys[rev1TopIndex-i]) {
			t.Errorf("expected revScan1 key %d to be %q; got %q", i, keys[rev1TopIndex-i], key)
		}
		if val := revScan1[i].ValueInt(); val != int64(rev1TopIndex-i) {
			t.Errorf("expected revScan1 result %d to be %d; got %d", i, rev1TopIndex-i, val)
		}
		if key := proto.Key(revScan2[i].Key); !key.Equal(keys[rev2TopIndex-i]) {
			t.Errorf("expected revScan2 key %d to be %q; got %q", i, keys[rev2TopIndex-i], key)
		}
		if val := revScan2[i].ValueInt(); val != int64(rev2TopIndex-i) {
			t.Errorf("expected revScan2 result %d to be %d; got %d", i, rev2TopIndex-i, val)
		}
	}
}

// concurrentIncrements starts two Goroutines in parallel, both of which
// read the integers stored at the other's key and add it onto their own.
// It is checked that the outcome is serializable, i.e. exactly one of the
// two Goroutines (the later write) sees the previous write by the other.
// The isMultiphase option runs the transaction in multiple phases recreating
// the transaction from the transaction protobuf returned from the server.
func concurrentIncrements(db *client.DB, t *testing.T, isMultiphase bool) {
	// wgStart waits for all transactions to line up, wgEnd has the main
	// function wait for them to finish.
	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(2 + 1)
	wgEnd.Add(2)

	for i := 0; i < 2; i++ {
		go func(i int) {
			// Read the other key, write key i.
			readKey := []byte(fmt.Sprintf(testUser+"/value-%d", (i+1)%2))
			writeKey := []byte(fmt.Sprintf(testUser+"/value-%d", i))
			defer wgEnd.Done()
			wgStart.Done()
			// Wait until the other goroutines are running.
			wgStart.Wait()

			if isMultiphase {
				applyInc := func(txn *client.Txn) (error, *proto.Transaction) {
					txn.SetDebugName(fmt.Sprintf("test-%d", i))
					b := client.Batch{}
					// Retrieve the other key.
					b.Get(readKey)
					if err := txn.Run(&b); err != nil {
						return err, txn.GetProtoFromTxn()
					}
					otherValue := int64(0)
					gr := b.Results[0].Rows[0]
					if gr.Value != nil {
						otherValue = gr.ValueInt()
					}
					// New txn.
					txn = db.ReconstructTxn(*txn.GetProtoFromTxn())
					// Write our key.
					b = client.Batch{}
					b.Inc(writeKey, 1+otherValue)
					if err := txn.Run(&b); err != nil {
						return err, txn.GetProtoFromTxn()
					}
					// New txn.
					txn = db.ReconstructTxn(*txn.GetProtoFromTxn())
					err := txn.Commit(&client.Batch{})
					return err, txn.GetProtoFromTxn()
				}
				for r := retry.Start(client.DefaultTxnRetryOptions); r.Next(); {
					txn := db.ReconstructTxn(proto.Transaction{})
					if err, txnProto := applyInc(txn); err != nil {
						// New txn.
						txn = db.ReconstructTxn(*txnProto)
						if err := txn.Rollback(&client.Batch{}); err != nil {
							t.Error(err)
						} else {
							// retry
							continue
						}
					}
					// exit retry
					break
				}
			} else {
				if err := db.Txn(func(txn *client.Txn) error {
					txn.SetDebugName(fmt.Sprintf("test-%d", i))

					// Retrieve the other key.
					gr, err := txn.Get(readKey)
					if err != nil {
						return err
					}

					otherValue := int64(0)
					if gr.Value != nil {
						otherValue = gr.ValueInt()
					}

					_, err = txn.Inc(writeKey, 1+otherValue)
					return err
				}); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}

	// Kick the goroutines loose.
	wgStart.Done()
	// Wait for the goroutines to finish.
	wgEnd.Wait()
	// Verify that both keys contain something and, more importantly, that
	// one key actually contains the value of the first writer and not only
	// its own.
	total := int64(0)
	results := []int64(nil)
	for i := 0; i < 2; i++ {
		readKey := []byte(fmt.Sprintf(testUser+"/value-%d", i))
		gr, err := db.Get(readKey)
		if err != nil {
			log.Fatal(err)
		}
		if gr.Value == nil {
			t.Fatalf("unexpected empty key: %s=%v", readKey, gr.Value)
		}
		total += gr.ValueInt()
		results = append(results, gr.ValueInt())
	}

	// First writer should have 1, second one 2
	if total != 3 {
		t.Fatalf("got unserializable values %v", results)
	}
}

// TestConcurrentIncrements is a simple explicit test for serializability
// for the concrete situation described in:
// https://groups.google.com/forum/#!topic/cockroach-db/LdrC5_T0VNw
func TestConcurrentIncrements(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(s.ServingAddr())

	// Convenience loop: Crank up this number for testing this
	// more often. It'll increase test duration though.
	numTests := 6
	for k := 0; k < numTests; k++ {
		if err := db.DelRange(testUser+"/value-0", testUser+"/value-1x"); err != nil {
			t.Fatalf("%d: unable to clean up: %v", k, err)
		}
		concurrentIncrements(db, t, k < numTests/2 /*isMultiphase*/)
	}
}

// TestClientPermissions verifies permission enforcement.
// This relies on:
// - r/w permissions config for 'testUser' on the 'testUser' prefix.
// - permissive checks for 'root' on all paths
// - all users have client certs
// Detailed permissions checking (read-only, write-only, etc...) is done elsewhere,
// this is testing user/path setting by the client.
func TestClientPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	test := createTestClientFor(s.ServingAddr(), testUser)
	root := createTestClientFor(s.ServingAddr(), "root")

	testCases := []struct {
		path    string
		client  *client.DB
		success bool
	}{
		{"foo", test, false},
		{"foo", root, true},

		{testUser + "/foo", test, true},
		{testUser + "/foo", root, true},

		{testUser + "foo", test, true},
		{testUser + "foo", root, true},

		{testUser, test, true},
		{testUser, root, true},

		{"unknown/foo", test, false},
		{"unknown/foo", root, true},
	}

	value := []byte("value")
	for tcNum, tc := range testCases {
		err := tc.client.Put(tc.path, value)
		if err == nil != tc.success {
			t.Errorf("#%d: expected success=%t, got err=%s", tcNum, tc.success, err)
		}
		_, err = tc.client.Get(tc.path)
		if err == nil != tc.success {
			t.Errorf("#%d: expected success=%t, got err=%s", tcNum, tc.success, err)
		}
	}
}

const valueSize = 1 << 10

func setupClientBenchData(useRPC, useSSL bool, numVersions, numKeys int, b *testing.B) (
	*server.TestServer, *client.DB) {
	const cacheSize = 8 << 30 // 8 GB
	loc := fmt.Sprintf("client_bench_%d_%d", numVersions, numKeys)

	exists := true
	if _, err := os.Stat(loc); os.IsNotExist(err) {
		exists = false
	}

	s := &server.TestServer{}
	s.Ctx = server.NewTestContext()
	s.Ctx.ExperimentalRPCServer = true
	s.SkipBootstrap = exists
	if !useSSL {
		s.Ctx.Insecure = true
	}
	s.Ctx.Engines = []engine.Engine{engine.NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc, cacheSize)}
	if err := s.Start(); err != nil {
		b.Fatal(err)
	}

	var scheme string
	if useRPC {
		scheme = "rpcs"
	} else {
		scheme = "https"
	}

	db, err := client.Open(scheme + "://root@" + s.ServingAddr() + "?certs=" + s.Ctx.Certs)
	if err != nil {
		b.Fatal(err)
	}

	if exists {
		return s, db
	}

	rng, _ := randutil.NewPseudoRand()
	keys := make([]proto.Key, numKeys)
	nvs := make([]int, numKeys)
	for t := 1; t <= numVersions; t++ {
		batch := &client.Batch{}
		for i := 0; i < numKeys; i++ {
			if t == 1 {
				keys[i] = proto.Key(encoding.EncodeUvarint([]byte("key-"), uint64(i)))
				nvs[i] = int(rand.Int31n(int32(numVersions)) + 1)
			}
			// Only write values if this iteration is less than the random
			// number of versions chosen for this key.
			if t <= nvs[i] {
				batch.Put(proto.Key(keys[i]), randutil.RandBytes(rng, valueSize))
			}
			if (i+1)%1000 == 0 {
				if err := db.Run(batch); err != nil {
					b.Fatal(err)
				}
				batch = &client.Batch{}
			}
		}
		if len(batch.Results) != 0 {
			if err := db.Run(batch); err != nil {
				b.Fatal(err)
			}
		}
	}

	if r, ok := s.Ctx.Engines[0].(*engine.RocksDB); ok {
		r.CompactRange(nil, nil)
	}

	return s, db
}

// runClientScan first creates test data (and resets the benchmarking
// timer). It then performs b.N client scans in increments of numRows
// keys over all of the data, restarting at the beginning of the
// keyspace, as many times as necessary.
func runClientScan(useRPC, useSSL bool, numRows, numVersions int, b *testing.B) {
	const numKeys = 100000

	s, db := setupClientBenchData(useRPC, useSSL, numVersions, numKeys, b)
	defer s.Stop()

	b.SetBytes(int64(numRows * valueSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		startKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
		endKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
		for pb.Next() {
			// Choose a random key to start scan.
			keyIdx := rand.Int31n(int32(numKeys - numRows))
			startKey := proto.Key(encoding.EncodeUvarint(startKeyBuf, uint64(keyIdx)))
			endKey := proto.Key(encoding.EncodeUvarint(endKeyBuf, uint64(keyIdx)+uint64(numRows)))
			rows, err := db.Scan(startKey, endKey, int64(numRows))
			if err != nil {
				b.Fatalf("failed scan: %s", err)
			}
			if len(rows) != numRows {
				b.Fatalf("failed to scan: %d != %d", len(rows), numRows)
			}
		}
	})

	b.StopTimer()
}

func BenchmarkRPCSSLClientScan1Version1Row(b *testing.B) {
	runClientScan(true /* RPC */, true /* SSL */, 1, 1, b)
}

func BenchmarkRPCSSLClientScan1Version10Rows(b *testing.B) {
	runClientScan(true /* RPC */, true /* SSL */, 10, 1, b)
}

func BenchmarkRPCSSLClientScan1Version100Rows(b *testing.B) {
	runClientScan(true /* RPC */, true /* SSL */, 100, 1, b)
}

func BenchmarkRPCSSLClientScan1Version1000Rows(b *testing.B) {
	runClientScan(true /* RPC */, true /* SSL */, 1000, 1, b)
}

func BenchmarkHTTPSSLClientScan1Version1Row(b *testing.B) {
	runClientScan(false /* HTTP */, true /* SSL */, 1, 1, b)
}

func BenchmarkHTTPSSLClientScan1Version10Rows(b *testing.B) {
	runClientScan(false /* HTTP */, true /* SSL */, 10, 1, b)
}

func BenchmarkHTTPSSLClientScan1Version100Rows(b *testing.B) {
	runClientScan(false /* HTTP */, true /* SSL */, 100, 1, b)
}

func BenchmarkHTTPSSLClientScan1Version1000Rows(b *testing.B) {
	runClientScan(false /* HTTP */, true /* SSL */, 1000, 1, b)
}

func BenchmarkRPCNoSSLClientScan1Version1Row(b *testing.B) {
	runClientScan(true /* RPC */, false /* NoSSL */, 1, 1, b)
}

func BenchmarkRPCNoSSLClientScan1Version10Rows(b *testing.B) {
	runClientScan(true /* RPC */, false /* NoSSL */, 10, 1, b)
}

func BenchmarkRPCNoSSLClientScan1Version100Rows(b *testing.B) {
	runClientScan(true /* RPC */, false /* NoSSL */, 100, 1, b)
}

func BenchmarkRPCNoSSLClientScan1Version1000Rows(b *testing.B) {
	runClientScan(true /* RPC */, false /* NoSSL */, 1000, 1, b)
}

func BenchmarkHTTPNoSSLClientScan1Version1Row(b *testing.B) {
	runClientScan(false /* HTTP */, false /* NoSSL */, 1, 1, b)
}

func BenchmarkHTTPNoSSLClientScan1Version10Rows(b *testing.B) {
	runClientScan(false /* HTTP */, false /* NoSSL */, 10, 1, b)
}

func BenchmarkHTTPNoSSLClientScan1Version100Rows(b *testing.B) {
	runClientScan(false /* HTTP */, false /* NoSSL */, 100, 1, b)
}

func BenchmarkHTTPNoSSLClientScan1Version1000Rows(b *testing.B) {
	runClientScan(false /* HTTP */, false /* NoSSL */, 1000, 1, b)
}
