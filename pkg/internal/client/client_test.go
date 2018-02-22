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

/* Package client_test tests clients against a fully-instantiated
cockroach cluster (a single node, but bootstrapped, gossiped, etc.).
*/
package client_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// testUser has valid client certs.
var testUser = server.TestUser

var errInfo = testutils.MakeCaller(3, 2)

// checkKVs verifies that a KeyValue slice contains the expected keys and
// values. The values can be either integers or strings; the expected results
// are passed as alternating keys and values, e.g:
//   checkScanResult(t, result, key1, val1, key2, val2)
func checkKVs(t *testing.T, kvs []client.KeyValue, expected ...interface{}) {
	expLen := len(expected) / 2
	if expLen != len(kvs) {
		t.Errorf("%s: expected %d scan results, got %d", errInfo(), expLen, len(kvs))
		return
	}
	for i := 0; i < expLen; i++ {
		expKey := expected[2*i].(roachpb.Key)
		if key := kvs[i].Key; !key.Equal(expKey) {
			t.Errorf("%s: expected scan key %d to be %q; got %q", errInfo(), i, expKey, key)
		}
		switch expValue := expected[2*i+1].(type) {
		case int:
			if value, err := kvs[i].Value.GetInt(); err != nil {
				t.Errorf("%s: non-integer scan value %d: %q", errInfo(), i, kvs[i].Value)
			} else if value != int64(expValue) {
				t.Errorf("%s: expected scan value %d to be %d; got %d",
					errInfo(), i, expValue, value)
			}
		case string:
			if value := kvs[i].Value.String(); value != expValue {
				t.Errorf("%s: expected scan value %d to be %s; got %s",
					errInfo(), i, expValue, value)
			}
		default:
			t.Fatalf("unsupported type %T", expValue)
		}
	}
}

func createTestClient(t *testing.T, s serverutils.TestServerInterface) *client.DB {
	return s.DB()
}

// TestClientRetryNonTxn verifies that non-transactional client will
// succeed despite write/write and read/write conflicts. In the case
// where the non-transactional put can push the txn, we expect the
// transaction's value to be written after all retries are complete.
func TestClientRetryNonTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a command filter which tracks which one of our test keys have
	// been attempted to be pushed.
	mu := struct {
		syncutil.Mutex
		m map[string]struct{}
	}{
		m: make(map[string]struct{}),
	}
	filter := func(args storagebase.FilterArgs) *roachpb.Error {
		mu.Lock()
		defer mu.Unlock()
		pushArg, ok := args.Req.(*roachpb.PushTxnRequest)
		if !ok || !strings.HasPrefix(string(pushArg.PusheeTxn.Key), "key-") {
			return nil
		}
		mu.m[string(pushArg.PusheeTxn.Key)] = struct{}{}
		return nil
	}
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				EvalKnobs: batcheval.TestingKnobs{
					TestingEvalFilter: filter,
				},
			},
		},
	}
	s, _, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(context.TODO())

	testCases := []struct {
		args        roachpb.Request
		isolation   enginepb.IsolationType
		canPush     bool
		expAttempts int
	}{
		// Write/write conflicts.
		{&roachpb.PutRequest{}, enginepb.SNAPSHOT, true, 2},
		{&roachpb.PutRequest{}, enginepb.SERIALIZABLE, true, 2},
		{&roachpb.PutRequest{}, enginepb.SNAPSHOT, false, 1},
		{&roachpb.PutRequest{}, enginepb.SERIALIZABLE, false, 1},
		// Read/write conflicts.
		{&roachpb.GetRequest{}, enginepb.SNAPSHOT, true, 1},
		{&roachpb.GetRequest{}, enginepb.SERIALIZABLE, true, 1},
		{&roachpb.GetRequest{}, enginepb.SNAPSHOT, false, 1},
		{&roachpb.GetRequest{}, enginepb.SERIALIZABLE, false, 1},
	}
	// Lay down a write intent using a txn and attempt to access the same
	// key from our test client, with priorities set up so that the Push
	// succeeds iff the test dictates that it does.
	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		db := createTestClient(t, s)

		// doneCall signals when the non-txn read or write has completed.
		doneCall := make(chan error)
		count := 0 // keeps track of retries
		err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			if test.canPush {
				if err := txn.SetUserPriority(roachpb.MinUserPriority); err != nil {
					t.Fatal(err)
				}
			}
			if test.isolation == enginepb.SNAPSHOT {
				if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
					return err
				}
			}

			count++
			// Lay down the intent.
			if err := txn.Put(ctx, key, "txn-value"); err != nil {
				return err
			}
			// On the first iteration, send the non-txn put or get.
			if count == 1 {
				nonTxnCtx := context.TODO()

				// The channel lets us pause txn until after the non-txn
				// method has run once. Use a channel length of size 1 to
				// guarantee a notification through a non-blocking send.
				notify := make(chan struct{}, 1)
				// We must try the non-txn put or get in a goroutine because
				// it might have to retry and will only succeed immediately in
				// the event we can push.
				go func() {
					var err error
					if _, ok := test.args.(*roachpb.GetRequest); ok {
						_, err = db.Get(nonTxnCtx, key)
					} else {
						err = db.Put(nonTxnCtx, key, "value")
					}
					notify <- struct{}{}
					if err != nil {
						log.Errorf(context.TODO(), "error on non-txn request: %s", err)
					}
					doneCall <- errors.Wrapf(
						err, "%d: expected success on non-txn call to %s",
						i, test.args.Method())
				}()
				// Block until the non-transactional client has pushed us at
				// least once.
				testutils.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if _, ok := mu.m[string(key)]; ok {
						return nil
					}
					return errors.New("non-transactional client has not pushed txn yet")
				})
				if test.canPush {
					// The non-transactional operation has priority. Wait for it
					// so that it doesn't interrupt subsequent transaction
					// attempts.
					<-notify
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%d: expected success writing transactionally; got %s", i, err)
		}

		// Make sure non-txn put or get has finished.
		if err := <-doneCall; err != nil {
			t.Fatal(err)
		}

		// Get the current value to verify whether the txn happened first.
		gr, err := db.Get(context.TODO(), key)
		if err != nil {
			t.Fatalf("%d: expected success getting %q: %s", i, key, err)
		}

		if _, isGet := test.args.(*roachpb.GetRequest); isGet || test.canPush {
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

// TestClientRunTransaction verifies some simple transaction isolation
// semantics.
func TestClientRunTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		key := []byte(fmt.Sprintf("%s/key-%t", testUser, commit))

		// Use snapshot isolation so non-transactional read can always push.
		err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
				return err
			}

			// Put transactional value.
			if err := txn.Put(ctx, key, value); err != nil {
				return err
			}
			// Attempt to read outside of txn.
			if gr, err := db.Get(ctx, key); err != nil {
				return err
			} else if gr.Value != nil {
				return errors.Errorf("expected nil value; got %+v", gr.Value)
			}
			// Read within the transaction.
			if gr, err := txn.Get(ctx, key); err != nil {
				return err
			} else if gr.Value == nil || !bytes.Equal(gr.ValueBytes(), value) {
				return errors.Errorf("expected value %q; got %q", value, gr.ValueBytes())
			}
			if !commit {
				return errors.Errorf("purposefully failing transaction")
			}
			return nil
		})

		if commit != (err == nil) {
			t.Errorf("expected success? %t; got %v", commit, err)
		} else if !commit && !testutils.IsError(err, "purposefully failing transaction") {
			t.Errorf("unexpected failure with !commit: %v", err)
		}

		// Verify the value is now visible on commit == true, and not visible otherwise.
		gr, err := db.Get(context.TODO(), key)
		if commit {
			if err != nil || gr.Value == nil || !bytes.Equal(gr.ValueBytes(), value) {
				t.Errorf("expected success reading value: %+v, %v", gr.Value, err)
			}
		} else {
			if err != nil || gr.Value != nil {
				t.Errorf("expected success and nil value: %+v, %v", gr.Value, err)
			}
		}
	}
}

// TestClientRunConcurrentTransaction verifies some simple transaction isolation
// semantics while accessing the Txn object from multiple goroutines concurrently.
func TestClientRunConcurrentTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		keySuffixes := "abc"
		keys := make([][]byte, len(keySuffixes))
		for j, s := range keySuffixes {
			keys[j] = []byte(fmt.Sprintf("%s/key-%t/%s", testUser, commit, string(s)))
		}

		// Use snapshot isolation so non-transactional read can always push.
		err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
				return err
			}

			// We can't use errgroup here because we need to return any TxnAborted
			// errors if we see them.
			var wg sync.WaitGroup
			concErrs := make([]error, len(keys))
			for i, key := range keys {
				wg.Add(1)
				go func(i int, key []byte) {
					defer wg.Done()
					// Put transactional value.
					if err := txn.Put(ctx, key, value); err != nil {
						concErrs[i] = err
						return
					}
					// Attempt to read outside of txn. We need to guarantee that the
					// BeginTxnRequest has finished or we risk aborting the transaction.
					if gr, err := db.Get(ctx, key); err != nil {
						concErrs[i] = err
						return
					} else if gr.Value != nil {
						concErrs[i] = errors.Errorf("expected nil value; got %+v", gr.Value)
						return
					}
					// Read within the transaction.
					if gr, err := txn.Get(ctx, key); err != nil {
						concErrs[i] = err
						return
					} else if gr.Value == nil || !bytes.Equal(gr.ValueBytes(), value) {
						concErrs[i] = errors.Errorf("expected value %q; got %q", value, gr.ValueBytes())
						return
					}
				}(i, key)
			}
			wg.Wait()

			// Check for any TxnAborted errors before deferring to any other errors.
			var anyError error
			for _, err := range concErrs {
				if err != nil {
					anyError = err
					if _, ok := err.(*roachpb.HandledRetryableTxnError); ok {
						return err
					}
				}
			}
			if anyError != nil {
				return anyError
			}
			if !commit {
				return errors.Errorf("purposefully failing transaction")
			}
			return nil
		})

		if commit != (err == nil) {
			t.Errorf("expected success? %t; got %v", commit, err)
		} else if !commit && !testutils.IsError(err, "purposefully failing transaction") {
			t.Errorf("unexpected failure with !commit: %v", err)
		}

		// Verify the values are now visible on commit == true, and not visible otherwise.
		for _, key := range keys {
			gr, err := db.Get(context.TODO(), key)
			if commit {
				if err != nil || gr.Value == nil || !bytes.Equal(gr.ValueBytes(), value) {
					t.Errorf("expected success reading value: %+v, %v", gr.Value, err)
				}
			} else {
				if err != nil || gr.Value != nil {
					t.Errorf("expected success and nil value: %+v, %v", gr.Value, err)
				}
			}
		}
	}
}

// TestClientGetAndPutProto verifies gets and puts of protobufs using the
// client's convenience methods.
func TestClientGetAndPutProto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	zoneConfig := config.ZoneConfig{
		NumReplicas:   2,
		Constraints:   []config.Constraints{{Constraints: []config.Constraint{{Value: "mem"}}}},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
	}

	key := roachpb.Key(testUser + "/zone-config")
	if err := db.Put(context.TODO(), key, &zoneConfig); err != nil {
		t.Fatalf("unable to put proto: %s", err)
	}

	var readZoneConfig config.ZoneConfig
	if err := db.GetProto(context.TODO(), key, &readZoneConfig); err != nil {
		t.Fatalf("unable to get proto: %s", err)
	}
	if !proto.Equal(&zoneConfig, &readZoneConfig) {
		t.Errorf("expected %+v, but found %+v", zoneConfig, readZoneConfig)
	}
}

// TestClientGetAndPut verifies gets and puts of using the client's convenience
// methods.
func TestClientGetAndPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	value := []byte("value")
	if err := db.Put(context.TODO(), testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(context.TODO(), testUser+"/key")
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if gr.Value.Timestamp == (hlc.Timestamp{}) {
		t.Fatalf("expected non-zero timestamp; got empty")
	}
}

func TestClientPutInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	value := []byte("value")
	if err := db.PutInline(context.TODO(), testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(context.TODO(), testUser+"/key")
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if ts := gr.Value.Timestamp; ts != (hlc.Timestamp{}) {
		t.Fatalf("expected zero timestamp; got %s", ts)
	}
}

// TestClientEmptyValues verifies that empty values are preserved
// for both empty []byte and integer=0. This used to fail when we
// allowed the protobufs to be gob-encoded using the default go rpc
// gob codec because gob treats pointer values and non-pointer values
// as equivalent and elides zero-valued defaults on decode.
func TestClientEmptyValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	if err := db.Put(context.TODO(), testUser+"/a", []byte{}); err != nil {
		t.Error(err)
	}
	if gr, err := db.Get(context.TODO(), testUser+"/a"); err != nil {
		t.Error(err)
	} else if bytes := gr.ValueBytes(); bytes == nil || len(bytes) != 0 {
		t.Errorf("expected non-nil empty byte slice; got %q", bytes)
	}

	if _, err := db.Inc(context.TODO(), testUser+"/b", 0); err != nil {
		t.Error(err)
	}
	if gr, err := db.Get(context.TODO(), testUser+"/b"); err != nil {
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
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)
	ctx := context.TODO()

	keys := []roachpb.Key{}
	{
		b := &client.Batch{}
		for i := 0; i < 10; i++ {
			key := roachpb.Key(fmt.Sprintf("%s/key %02d", testUser, i))
			keys = append(keys, key)
			b.Inc(key, int64(i))
		}

		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}

		for i, result := range b.Results {
			if v := result.Rows[0].ValueInt(); v != int64(i) {
				t.Errorf("%d: expected %d; got %d", i, i, v)
			}
		}
	}

	// Now try 2 scans.
	{
		b := &client.Batch{}
		b.Scan(testUser+"/key 00", testUser+"/key 05")
		b.Scan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[0], 0, keys[1], 1, keys[2], 2, keys[3], 3, keys[4], 4)
		checkKVs(t, b.Results[1].Rows, keys[5], 5, keys[6], 6, keys[7], 7, keys[8], 8, keys[9], 9)
	}

	// Try a limited batch of 2 scans.
	{
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = 7
		b.Scan(testUser+"/key 00", testUser+"/key 05")
		b.Scan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[0], 0, keys[1], 1, keys[2], 2, keys[3], 3, keys[4], 4)
		checkKVs(t, b.Results[1].Rows, keys[5], 5, keys[6], 6)
	}

	// Try a limited batch of 2 scans.
	{
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = 7
		b.Scan(testUser+"/key 05", testUser+"/key 10")
		b.Scan(testUser+"/key 00", testUser+"/key 05")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[5], 5, keys[6], 6, keys[7], 7, keys[8], 8, keys[9], 9)
		checkKVs(t, b.Results[1].Rows, keys[0], 0, keys[1], 1)
	}

	// Try a limited batch of 2 scans.
	{
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = 3
		b.Scan(testUser+"/key 00", testUser+"/key 05")
		b.Scan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[0], 0, keys[1], 1, keys[2], 2)
		checkKVs(t, b.Results[1].Rows)
	}

	// Try 2 reverse scans.
	{
		b := &client.Batch{}
		b.ReverseScan(testUser+"/key 00", testUser+"/key 05")
		b.ReverseScan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		checkKVs(t, b.Results[1].Rows, keys[9], 9, keys[8], 8, keys[7], 7, keys[6], 6, keys[5], 5)
	}

	// Try a limited batch of 2 reverse scans.
	{
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = 7
		b.ReverseScan(testUser+"/key 00", testUser+"/key 05")
		b.ReverseScan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		checkKVs(t, b.Results[1].Rows, keys[9], 9, keys[8], 8)
	}

	// Try a limited batch of 2 reverse scans.
	{
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = 7
		b.ReverseScan(testUser+"/key 05", testUser+"/key 10")
		b.ReverseScan(testUser+"/key 00", testUser+"/key 05")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[9], 9, keys[8], 8, keys[7], 7, keys[6], 6, keys[5], 5)
		checkKVs(t, b.Results[1].Rows, keys[4], 4, keys[3], 3)
	}

	// Try a limited batch of 2 reverse scans.
	{
		b := &client.Batch{}
		b.Header.MaxSpanRequestKeys = 3
		b.ReverseScan(testUser+"/key 00", testUser+"/key 05")
		b.ReverseScan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(ctx, b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[4], 4, keys[3], 3, keys[2], 2)
		checkKVs(t, b.Results[1].Rows)
	}

	// Induce a non-transactional failure.
	{
		key := roachpb.Key("conditionalPut")
		if err := db.Put(context.TODO(), key, "hello"); err != nil {
			t.Fatal(err)
		}

		b := &client.Batch{}
		b.CPut(key, "goodbyte", nil) // should fail
		if err := db.Run(ctx, b); err == nil {
			t.Error("unexpected success")
		} else {
			var foundError bool
			for _, result := range b.Results {
				if result.Err != nil {
					foundError = true
					break
				}
			}
			if !foundError {
				t.Error("results did not contain an error")
			}
		}
	}

	// Induce a transactional failure.
	{
		key := roachpb.Key("conditionalPut")
		if err := db.Put(context.TODO(), key, "hello"); err != nil {
			t.Fatal(err)
		}

		b := &client.Batch{}
		b.CPut(key, "goodbyte", nil) // should fail
		if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return txn.Run(ctx, b)
		}); err == nil {
			t.Error("unexpected success")
		} else {
			var foundError bool
			for _, result := range b.Results {
				if result.Err != nil {
					foundError = true
					break
				}
			}
			if !foundError {
				t.Error("results did not contain an error")
			}
		}
	}
}

// concurrentIncrements starts two Goroutines in parallel, both of which
// read the integers stored at the other's key and add it onto their own.
// It is checked that the outcome is serializable, i.e. exactly one of the
// two Goroutines (the later write) sees the previous write by the other.
func concurrentIncrements(db *client.DB, t *testing.T) {
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

			if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
				txn.SetDebugName(fmt.Sprintf("test-%d", i))

				// Retrieve the other key.
				gr, err := txn.Get(ctx, readKey)
				if err != nil {
					return err
				}

				otherValue := int64(0)
				if gr.Value != nil {
					otherValue = gr.ValueInt()
				}

				_, err = txn.Inc(ctx, writeKey, 1+otherValue)
				return err
			}); err != nil {
				t.Error(err)
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
		gr, err := db.Get(context.TODO(), readKey)
		if err != nil {
			t.Fatal(err)
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
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	// Convenience loop: Crank up this number for testing this
	// more often. It'll increase test duration though.
	for k := 0; k < 5; k++ {
		if err := db.DelRange(context.TODO(), testUser+"/value-0", testUser+"/value-1x"); err != nil {
			t.Fatalf("%d: unable to clean up: %s", k, err)
		}
		concurrentIncrements(db, t)
	}
}

// TestReadConsistencyTypes tests that the methods that generate reads with
// different read consistency types generate outgoing requests with the
// corresponding read consistency type set.
func TestReadConsistencyTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.CONSISTENT,
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			// Mock out DistSender's sender function to check the read consistency for
			// outgoing BatchRequests and return an empty reply.
			factory := client.TxnSenderFactoryFunc(func(_ client.TxnType) client.TxnSender {
				return client.TxnSenderFunc(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
					if ba.ReadConsistency != rc {
						return nil, roachpb.NewErrorf("BatchRequest has unexpected ReadConsistency %s", ba.ReadConsistency)
					}
					return ba.CreateReply(), nil
				})
			})
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			db := client.NewDB(factory, clock)
			ctx := context.TODO()

			prepWithRC := func() *client.Batch {
				b := &client.Batch{}
				b.Header.ReadConsistency = rc
				return b
			}

			// Perform reads through the mocked sender function.
			{
				key := roachpb.Key([]byte("key"))
				b := prepWithRC()
				b.Get(key)
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}
			}

			{
				b := prepWithRC()
				key1 := roachpb.Key([]byte("key1"))
				key2 := roachpb.Key([]byte("key2"))
				b.Scan(key1, key2)
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}
			}

			{
				key := roachpb.Key([]byte("key"))
				b := &client.Batch{}
				b.Header.ReadConsistency = rc
				b.Get(key)
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestReadOnlyTxnObeysDeadline tests that read-only transactions obey the
// deadline. Read-only transactions have their EndTransaction elided, so the
// enforcement of the deadline is done in the client.
func TestReadOnlyTxnObeysDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	if err := db.Put(context.TODO(), "k", "v"); err != nil {
		t.Fatal(err)
	}

	txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)
	// Only snapshot transactions can observe deadline errors; serializable ones
	// get a restart error before the deadline check.
	if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
		t.Fatal(err)
	}
	opts := client.TxnExecOptions{
		AutoRetry:  false,
		AutoCommit: true,
	}
	if err := txn.Exec(
		context.TODO(), opts,
		func(ctx context.Context, txn *client.Txn, _ *client.TxnExecOptions) error {
			// Set a deadline, then set a higher commit timestamp for the txn.
			txn.UpdateDeadlineMaybe(ctx, s.Clock().Now())
			txn.Proto().Timestamp.Forward(s.Clock().Now())
			_, err := txn.Get(ctx, "k")
			return err
		}); !testutils.IsError(err, "deadline exceeded before transaction finalization") {
		// We test for TransactionAbortedError. If this was not a read-only txn,
		// the error returned by the server would have been different - a
		// TransactionStatusError. This inconsistency is unfortunate.
		t.Fatal(err)
	}
}

// TestTxn_ReverseScan a simple test for Txn.ReverseScan
func TestTxn_ReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := createTestClient(t, s)

	keys := []roachpb.Key{}
	b := &client.Batch{}
	for i := 0; i < 10; i++ {
		key := roachpb.Key(fmt.Sprintf("%s/key/%02d", testUser, i))
		keys = append(keys, key)
		b.Put(key, i)
	}
	if err := db.Run(context.TODO(), b); err != nil {
		t.Error(err)
	}

	err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		// Try reverse scans for all keys.
		{
			rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/10", 100)
			if err != nil {
				return err
			}
			checkKVs(t, rows,
				keys[9], 9, keys[8], 8, keys[7], 7, keys[6], 6, keys[5], 5,
				keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		}

		// Try reverse scans for half of the keys.
		{
			rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/05", 100)
			if err != nil {
				return err
			}
			checkKVs(t, rows, keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		}

		// Try limit maximum rows.
		{
			rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/05", 3)
			if err != nil {
				return err
			}
			checkKVs(t, rows, keys[4], 4, keys[3], 3, keys[2], 2)
		}

		// Try reverse scan with the same start and end key.
		{
			rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/00", 100)
			if len(rows) > 0 {
				t.Errorf("expected empty, got %v", rows)
			}
			if err == nil {
				t.Errorf("expected a truncation error, got %s", err)
			}
		}

		// Try reverse scan with non-existent key.
		{
			rows, err := txn.ReverseScan(ctx, testUser+"/key/aa", testUser+"/key/bb", 100)
			if err != nil {
				return err
			}
			if len(rows) > 0 {
				t.Errorf("expected empty, got %v", rows)
			}
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeIDAndObservedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Mock out sender function to check that created transactions
	// have the observed timestamp set for the configured node ID.
	factory := client.TxnSenderFactoryFunc(func(_ client.TxnType) client.TxnSender {
		return client.TxnSenderFunc(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			return ba.CreateReply(), nil
		})
	})

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	dbCtx := client.DefaultDBContext()
	dbCtx.NodeID = &base.NodeIDContainer{}
	db := client.NewDBWithContext(factory, clock, dbCtx)
	ctx := context.Background()

	// Verify direct creation of Txns.
	directCases := []struct {
		typ         client.TxnType
		nodeID      roachpb.NodeID
		expObserved bool
	}{
		{typ: client.RootTxn, nodeID: 0, expObserved: false},
		{typ: client.RootTxn, nodeID: 1, expObserved: true},
		{typ: client.LeafTxn, nodeID: 0, expObserved: false},
		{typ: client.LeafTxn, nodeID: 1, expObserved: false},
	}
	for i, test := range directCases {
		t.Run(fmt.Sprintf("direct-txn-%d", i), func(t *testing.T) {
			txn := client.NewTxn(db, test.nodeID, test.typ)
			if ots := txn.Proto().ObservedTimestamps; (len(ots) == 1 && ots[0].NodeID == test.nodeID) != test.expObserved {
				t.Errorf("expected observed ts %t; got %+v", test.expObserved, ots)
			}
		})
	}

	// Verify node ID container using DB.Txn().
	indirectCases := []struct {
		nodeID      roachpb.NodeID
		expObserved bool
	}{
		{nodeID: 0, expObserved: false},
		{nodeID: 1, expObserved: true},
	}
	for i, test := range indirectCases {
		t.Run(fmt.Sprintf("indirect-txn-%d", i), func(t *testing.T) {
			if test.nodeID != 0 {
				dbCtx.NodeID.Set(ctx, test.nodeID)
			}
			if err := db.Txn(
				ctx, func(_ context.Context, txn *client.Txn) error {
					if ots := txn.Proto().ObservedTimestamps; (len(ots) == 1 && ots[0].NodeID == test.nodeID) != test.expObserved {
						t.Errorf("expected observed ts %t; got %+v", test.expObserved, ots)
					}
					return nil
				},
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}
