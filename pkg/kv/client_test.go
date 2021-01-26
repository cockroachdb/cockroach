// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/* Package client_test tests clients against a fully-instantiated
cockroach cluster (a single node, but bootstrapped, gossiped, etc.).
*/
package kv_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// testUser has valid client certs.
var testUser = security.TestUser

// checkKVs verifies that a KeyValue slice contains the expected keys and
// values. The values can be either integers or strings; the expected results
// are passed as alternating keys and values, e.g:
//   checkScanResult(t, result, key1, val1, key2, val2)
func checkKVs(t *testing.T, kvs []kv.KeyValue, expected ...interface{}) {
	t.Helper()
	expLen := len(expected) / 2
	if expLen != len(kvs) {
		t.Errorf("expected %d scan results, got %d", expLen, len(kvs))
		return
	}
	for i := 0; i < expLen; i++ {
		expKey := expected[2*i].(roachpb.Key)
		if key := kvs[i].Key; !key.Equal(expKey) {
			t.Errorf("expected scan key %d to be %q; got %q", i, expKey, key)
		}
		switch expValue := expected[2*i+1].(type) {
		case int:
			if value, err := kvs[i].Value.GetInt(); err != nil {
				t.Errorf("non-integer scan value %d: %q", i, kvs[i].Value)
			} else if value != int64(expValue) {
				t.Errorf("expected scan value %d to be %d; got %d",
					i, expValue, value)
			}
		case string:
			if value := kvs[i].Value.String(); value != expValue {
				t.Errorf("expected scan value %d to be %s; got %s",
					i, expValue, value)
			}
		default:
			t.Fatalf("unsupported type %T", expValue)
		}
	}
}

func createTestClient(t *testing.T, s serverutils.TestServerInterface) *kv.DB {
	return s.DB()
}

// TestClientRetryNonTxn verifies that non-transactional client will
// succeed despite write/write and read/write conflicts. In the case
// where the non-transactional put can push the txn, we expect the
// transaction's value to be written after all retries are complete.
func TestClientRetryNonTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up a command filter which tracks which one of our test keys have
	// been attempted to be pushed.
	mu := struct {
		syncutil.Mutex
		m map[string]struct{}
	}{
		m: make(map[string]struct{}),
	}
	filter := func(args kvserverbase.FilterArgs) *roachpb.Error {
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
			Store: &kvserver.StoreTestingKnobs{
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingEvalFilter: filter,
				},
			},
		},
	}
	s, _, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(context.Background())

	testCases := []struct {
		args        roachpb.Request
		canPush     bool
		expAttempts int
	}{
		// Write/write conflicts.
		{&roachpb.PutRequest{}, true, 2},
		{&roachpb.PutRequest{}, false, 1},
		// Read/write conflicts.
		{&roachpb.GetRequest{}, true, 1},
		{&roachpb.GetRequest{}, false, 1},
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
		err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			if test.canPush {
				if err := txn.SetUserPriority(roachpb.MinUserPriority); err != nil {
					t.Fatal(err)
				}
			}
			count++
			// Lay down the intent.
			if err := txn.Put(ctx, key, "txn-value"); err != nil {
				return err
			}
			// On the first iteration, send the non-txn put or get.
			if count == 1 {
				nonTxnCtx := context.Background()

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
						log.Errorf(context.Background(), "error on non-txn request: %s", err)
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
		gr, err := db.Get(context.Background(), key)
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
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		key := []byte(fmt.Sprintf("%s/key-%t", testUser, commit))

		err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			// Put transactional value.
			if err := txn.Put(ctx, key, value); err != nil {
				return err
			}
			// Attempt to read in another txn.
			conflictTxn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
			conflictTxn.TestingSetPriority(enginepb.MaxTxnPriority)
			if gr, err := conflictTxn.Get(ctx, key); err != nil {
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
		gr, err := db.Get(context.Background(), key)
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

// TestClientGetAndPutProto verifies gets and puts of protobufs using the
// client's convenience methods.
func TestClientGetAndPutProto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	zoneConfig := zonepb.ZoneConfig{
		NumReplicas:   proto.Int32(2),
		Constraints:   []zonepb.ConstraintsConjunction{{Constraints: []zonepb.Constraint{{Value: "mem"}}}},
		RangeMinBytes: proto.Int64(1 << 10), // 1k
		RangeMaxBytes: proto.Int64(1 << 18), // 256k
	}

	key := roachpb.Key(testUser + "/zone-config")
	if err := db.Put(context.Background(), key, &zoneConfig); err != nil {
		t.Fatalf("unable to put proto: %s", err)
	}

	var readZoneConfig zonepb.ZoneConfig
	if err := db.GetProto(context.Background(), key, &readZoneConfig); err != nil {
		t.Fatalf("unable to get proto: %s", err)
	}
	if !zoneConfig.Equal(&readZoneConfig) {
		t.Errorf("expected %+v, but found %+v", zoneConfig, readZoneConfig)
	}
}

// TestClientGetAndPut verifies gets and puts of using the client's convenience
// methods.
func TestClientGetAndPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	value := []byte("value")
	if err := db.Put(context.Background(), testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(context.Background(), testUser+"/key")
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if gr.Value.Timestamp.IsEmpty() {
		t.Fatalf("expected non-zero timestamp; got empty")
	}
}

func TestClientPutInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	value := []byte("value")
	if err := db.PutInline(context.Background(), testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(context.Background(), testUser+"/key")
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if ts := gr.Value.Timestamp; !ts.IsEmpty() {
		t.Fatalf("expected zero timestamp; got %s", ts)
	}
}

func TestClientCPutInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)
	ctx := kv.CtxForCPutInline(context.Background())
	key := testUser + "/key"
	value := []byte("value")

	// Should fail on non-existent key with expected value.
	if err := db.CPutInline(ctx, key, value, []byte("foo")); err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Setting value when expecting nil should work.
	if err := db.CPutInline(ctx, key, value, nil); err != nil {
		t.Fatalf("unable to set value: %s", err)
	}
	gr, err := db.Get(ctx, key)
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if ts := gr.Value.Timestamp; !ts.IsEmpty() {
		t.Fatalf("expected zero timestamp; got %s", ts)
	}

	// Updating existing value with nil expected value should fail.
	if err := db.CPutInline(ctx, key, []byte("new"), nil); err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Updating value with other expected value should fail.
	if err := db.CPutInline(ctx, key, []byte("new"), []byte("foo")); err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Updating when given correct value should work.
	if err := db.CPutInline(ctx, key, []byte("new"), gr.Value.TagAndDataBytes()); err != nil {
		t.Fatalf("unable to update value: %s", err)
	}
	gr, err = db.Get(ctx, key)
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	} else if !bytes.Equal([]byte("new"), gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", []byte("new"), gr.ValueBytes())
	} else if ts := gr.Value.Timestamp; !ts.IsEmpty() {
		t.Fatalf("expected zero timestamp; got %s", ts)
	}

	// Deleting when given nil and correct expected value should work.
	if err := db.CPutInline(ctx, key, nil, gr.Value.TagAndDataBytes()); err != nil {
		t.Fatalf("unable to delete value: %s", err)
	}
	gr, err = db.Get(ctx, key)
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	} else if gr.Value != nil {
		t.Fatalf("expected deleted value; got %s", gr.ValueBytes())
	}
}

// TestClientEmptyValues verifies that empty values are preserved
// for both empty []byte and integer=0. This used to fail when we
// allowed the protobufs to be gob-encoded using the default go rpc
// gob codec because gob treats pointer values and non-pointer values
// as equivalent and elides zero-valued defaults on decode.
func TestClientEmptyValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	if err := db.Put(context.Background(), testUser+"/a", []byte{}); err != nil {
		t.Error(err)
	}
	if gr, err := db.Get(context.Background(), testUser+"/a"); err != nil {
		t.Error(err)
	} else if bytes := gr.ValueBytes(); bytes == nil || len(bytes) != 0 {
		t.Errorf("expected non-nil empty byte slice; got %q", bytes)
	}

	if _, err := db.Inc(context.Background(), testUser+"/b", 0); err != nil {
		t.Error(err)
	}
	if gr, err := db.Get(context.Background(), testUser+"/b"); err != nil {
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
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)
	ctx := context.Background()

	keys := []roachpb.Key{}
	{
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		b := &kv.Batch{}
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
		if err := db.Put(context.Background(), key, "hello"); err != nil {
			t.Fatal(err)
		}

		b := &kv.Batch{}
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
		if err := db.Put(context.Background(), key, "hello"); err != nil {
			t.Fatal(err)
		}

		b := &kv.Batch{}
		b.CPut(key, "goodbyte", nil) // should fail
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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
func concurrentIncrements(db *kv.DB, t *testing.T) {
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

			if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
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
		gr, err := db.Get(context.Background(), readKey)
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
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	// Convenience loop: Crank up this number for testing this
	// more often. It'll increase test duration though.
	for k := 0; k < 5; k++ {
		if err := db.DelRange(context.Background(), testUser+"/value-0", testUser+"/value-1x"); err != nil {
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
	defer log.Scope(t).Close(t)

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.CONSISTENT,
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			// Mock out DistSender's sender function to check the read consistency for
			// outgoing BatchRequests and return an empty reply.
			factory := kv.NonTransactionalFactoryFunc(
				func(_ context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if ba.ReadConsistency != rc {
						return nil, roachpb.NewErrorf("BatchRequest has unexpected ReadConsistency %s", ba.ReadConsistency)
					}
					return ba.CreateReply(), nil
				})

			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock, stopper)

			prepWithRC := func() *kv.Batch {
				b := &kv.Batch{}
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
				b := &kv.Batch{}
				b.Header.ReadConsistency = rc
				b.Get(key)
				if err := db.Run(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestTxn_ReverseScan a simple test for Txn.ReverseScan
func TestTxn_ReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := createTestClient(t, s)

	keys := []roachpb.Key{}
	b := &kv.Batch{}
	for i := 0; i < 10; i++ {
		key := roachpb.Key(fmt.Sprintf("%s/key/%02d", testUser, i))
		keys = append(keys, key)
		b.Put(key, i)
	}
	if err := db.Run(context.Background(), b); err != nil {
		t.Error(err)
	}

	// Try reverse scans for all keys.
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/10", 100)
		if err != nil {
			return err
		}
		checkKVs(t, rows,
			keys[9], 9, keys[8], 8, keys[7], 7, keys[6], 6, keys[5], 5,
			keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Try reverse scans for half of the keys.
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/05", 100)
		if err != nil {
			return err
		}
		checkKVs(t, rows, keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Try limit maximum rows.
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/05", 3)
		if err != nil {
			return err
		}
		checkKVs(t, rows, keys[4], 4, keys[3], 3, keys[2], 2)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Try reverse scan with the same start and end key.
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		rows, err := txn.ReverseScan(ctx, testUser+"/key/00", testUser+"/key/00", 100)
		if len(rows) > 0 {
			t.Errorf("expected empty, got %v", rows)
		}
		return err
	}); err != nil {
		if err == nil {
			t.Errorf("expected a truncation error, got %s", err)
		}
	}

	// Try reverse scan with non-existent key.
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		rows, err := txn.ReverseScan(ctx, testUser+"/key/aa", testUser+"/key/bb", 100)
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			t.Errorf("expected empty, got %v", rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNodeIDAndObservedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Mock out sender function to check that created transactions
	// have the observed timestamp set for the configured node ID.
	factory := kv.MakeMockTxnSenderFactory(
		func(_ context.Context, _ *roachpb.Transaction, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			return ba.CreateReply(), nil
		})

	setup := func(nodeID roachpb.NodeID) *kv.DB {
		clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
		dbCtx := kv.DefaultDBContext(stopper)
		var c base.NodeIDContainer
		if nodeID != 0 {
			c.Set(context.Background(), nodeID)
		}
		dbCtx.NodeID = base.NewSQLIDContainer(0, &c)

		db := kv.NewDBWithContext(testutils.MakeAmbientCtx(), factory, clock, dbCtx)
		return db
	}

	// Verify direct creation of Txns.
	directCases := []struct {
		typ         kv.TxnType
		nodeID      roachpb.NodeID
		expObserved bool
	}{
		{typ: kv.RootTxn, nodeID: 0, expObserved: false},
		{typ: kv.RootTxn, nodeID: 1, expObserved: true},
		{typ: kv.LeafTxn, nodeID: 0, expObserved: false},
		{typ: kv.LeafTxn, nodeID: 1, expObserved: false},
	}
	for i, test := range directCases {
		t.Run(fmt.Sprintf("direct-txn-%d", i), func(t *testing.T) {
			db := setup(test.nodeID)
			now := db.Clock().NowAsClockTimestamp()
			kvTxn := roachpb.MakeTransaction("unnamed", nil /*baseKey*/, roachpb.NormalUserPriority, now.ToTimestamp(), db.Clock().MaxOffset().Nanoseconds())
			txn := kv.NewTxnFromProto(ctx, db, test.nodeID, now, test.typ, &kvTxn)
			ots := txn.TestingCloneTxn().ObservedTimestamps
			if (len(ots) == 1 && ots[0].NodeID == test.nodeID) != test.expObserved {
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
			db := setup(test.nodeID)
			if err := db.Txn(
				ctx, func(_ context.Context, txn *kv.Txn) error {
					ots := txn.TestingCloneTxn().ObservedTimestamps
					if (len(ots) == 1 && ots[0].NodeID == test.nodeID) != test.expObserved {
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

// Test that a reader unblocks quickly after an intent is cleaned up.
func TestIntentCleanupUnblocksReaders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	key := roachpb.Key("a")

	// We're going to repeatedly lay down an intent and then race a reader with
	// the intent cleanup, and check that the reader was not blocked for too long.
	for i := 0; i < 100; i++ {
		block := make(chan struct{})
		done := make(chan error)
		var start time.Time
		go func() {
			// Wait for the writer to lay down its intent.
			<-block
			start = timeutil.Now()
			if _, err := db.Get(ctx, key); err != nil {
				done <- err
			}
			close(done)
		}()

		err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.Put(ctx, key, "txn-value"); err != nil {
				close(block)
				return err
			}
			// Unblock the writer.
			close(block)
			return fmt.Errorf("test err causing rollback")
		})
		if !testutils.IsError(err, "test err causing rollback") {
			t.Fatal(err)
		}

		// Wait for the reader.
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		dur := timeutil.Since(start)

		// It's likely that the get always takes less than 2s (that's when a txn is
		// considered abandoned by a pusher), so the timeout below needs to stay below
		// that for this test to be meaningful.
		if dur > 800*time.Millisecond {
			t.Fatalf("txn wasn't cleaned up. Get took: %s", dur)
		}
	}
}

// Test that a transaction can be rolled back even with a canceled context.
// This relies on custom code in txn.rollback(), otherwise RPCs can't be sent.
func TestRollbackWithCanceledContextBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	key := roachpb.Key("a")
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.Put(ctx, key, "txn-value"); err != nil {
			return err
		}
		cancel()
		return fmt.Errorf("test err")
	})
	if !testutils.IsError(err, "test err") {
		t.Fatal(err)
	}
	start := timeutil.Now()

	// Do a Get using a different ctx (not the canceled one), and check that it
	// didn't take too long - take that as proof that it was not blocked on
	// intents. If the Get would have been blocked, it would have had to wait for
	// the transaction abandon timeout before the Get would proceed.
	// The difficulty with this test is that the cleanup done by the rollback with
	// a canceled ctx is async, so we can't simply do a Get with READ_UNCOMMITTED
	// to see if there's an intent.
	// TODO(andrei): It'd be better to use tracing to verify that either we
	// weren't blocked on an intent or, if we were, that intent was cleaned up by
	// someone else than the would-be pusher fast. Similar in
	// TestSessionFinishRollsBackTxn.
	if _, err := db.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	dur := timeutil.Since(start)
	if dur > 500*time.Millisecond {
		t.Fatalf("txn wasn't cleaned up. Get took: %s", dur)
	}
}

// This test is like TestRollbackWithCanceledContextBasic, except that, instead
// of the ctx being canceled before the rollback is sent, this time it is
// canceled after the rollback is sent. So, the first rollback is expected to
// fail, and we're testing that a 2nd one is sent.
func TestRollbackWithCanceledContextInsidious(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Add a request filter which cancels the ctx when it sees a rollback.
	var storeKnobs kvserver.StoreTestingKnobs
	key := roachpb.Key("a")
	ctx, cancel := context.WithCancel(context.Background())
	var rollbacks int
	storeKnobs.TestingRequestFilter = func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if !ba.IsSingleEndTxnRequest() {
			return nil
		}
		et := ba.Requests[0].GetInner().(*roachpb.EndTxnRequest)
		if !et.Commit && et.Key.Equal(key) {
			rollbacks++
			cancel()
		}
		return nil
	}
	s, _, db := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	defer s.Stopper().Stop(context.Background())

	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.Put(ctx, key, "txn-value"); err != nil {
			return err
		}
		return fmt.Errorf("test err")
	})
	if !testutils.IsError(err, "test err") {
		t.Fatal(err)
	}
	start := timeutil.Now()

	// Do a Get using a different ctx (not the canceled one), and check that it
	// didn't take too long - take that as proof that it was not blocked on
	// intents. If the Get would have been blocked, it would have had to wait for
	// the transaction abandon timeout before the Get would proceed.
	// The difficulty with this test is that the cleanup done by the rollback with
	// a canceled ctx is async, so we can't simply do a Get with READ_UNCOMMITTED
	// to see if there's an intent.
	// TODO(andrei): It'd be better to use tracing to verify that either we
	// weren't blocked on an intent or, if we were, that intent was cleaned up by
	// someone else than the would-be pusher fast. Similar in
	// TestSessionFinishRollsBackTxn.
	if _, err := db.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	dur := timeutil.Since(start)
	if dur > 500*time.Millisecond {
		t.Fatalf("txn wasn't cleaned up. Get took: %s", dur)
	}
	if rollbacks != 2 {
		t.Fatalf("expected 2 rollbacks, got: %d", rollbacks)
	}
}
