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

/* Package client_test tests clients against a fully-instantiated
cockroach cluster (a single node, but bootstrapped, gossiped, etc.).
*/
package client_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
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

// notifyingSender is a sender which can set up a notification channel
// (on call to reset()) for clients which need to wait on a command
// being sent.
type notifyingSender struct {
	notify  chan struct{}
	wrapped client.Sender
}

func (ss *notifyingSender) reset(notify chan struct{}) {
	ss.notify = notify
}

func (ss *notifyingSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	br, pErr := ss.wrapped.Send(ctx, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(ss.wrapped, br))
	}

	select {
	case ss.notify <- struct{}{}:
	default:
	}

	return br, pErr
}

func createTestClient(t *testing.T, stopper *stop.Stopper, addr string) *client.DB {
	return createTestClientForUser(t, stopper, addr, security.NodeUser, client.DefaultDBContext())
}

func createTestClientForUser(
	t *testing.T,
	stopper *stop.Stopper,
	addr, user string,
	dbCtx client.DBContext,
) *client.DB {
	rpcContext := rpc.NewContext(&base.Context{
		User:       user,
		SSLCA:      filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert),
		SSLCert:    filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.crt", user)),
		SSLCertKey: filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.key", user)),
	}, nil, stopper)
	sender, err := client.NewSender(rpcContext, addr)
	if err != nil {
		t.Fatal(err)
	}
	return client.NewDBWithContext(sender, dbCtx)
}

// createTestNotifyClient creates a new client which connects using an HTTP
// sender to the server at addr. It contains a waitgroup to allow waiting.
func createTestNotifyClient(t *testing.T, stopper *stop.Stopper, addr string, priority roachpb.UserPriority) (*client.DB, *notifyingSender) {
	db := createTestClient(t, stopper, addr)
	sender := &notifyingSender{wrapped: db.GetSender()}
	dbCtx := client.DefaultDBContext()
	dbCtx.UserPriority = priority
	return client.NewDBWithContext(sender, dbCtx), sender
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
				TestingCommandFilter: filter,
			},
		},
	}
	s, _, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop()

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
		{&roachpb.GetRequest{}, enginepb.SERIALIZABLE, true, 2},
		{&roachpb.GetRequest{}, enginepb.SNAPSHOT, false, 1},
		{&roachpb.GetRequest{}, enginepb.SERIALIZABLE, false, 1},
	}
	// Lay down a write intent using a txn and attempt to access the same
	// key from our test client, with priorities set up so that the Push
	// succeeds iff the test dicates that it do.
	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		var txnPri int32 = 1
		var clientPri roachpb.UserPriority = 1
		if test.canPush {
			clientPri = 2
		} else {
			txnPri = 2
		}

		db, sender := createTestNotifyClient(t, s.Stopper(), s.ServingAddr(), -clientPri)

		// doneCall signals when the non-txn read or write has completed.
		doneCall := make(chan error)
		count := 0 // keeps track of retries
		err := db.Txn(func(txn *client.Txn) error {
			if test.isolation == enginepb.SNAPSHOT {
				if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
					return err
				}
			}
			txn.InternalSetPriority(txnPri)

			count++
			// Lay down the intent.
			if err := txn.Put(key, "txn-value"); err != nil {
				return err
			}
			// On the first true, send the non-txn put or get.
			if count == 1 {
				// We use a "notifying" sender here, which allows us to know exactly when the
				// call has been processed; otherwise, we'd be dependent on timing.
				// The channel lets us pause txn until after the non-txn method has run once.
				// Use a channel length of size 1 to guarantee a notification through a
				// non-blocking send.
				notify := make(chan struct{}, 1)
				sender.reset(notify)
				// We must try the non-txn put or get in a goroutine because
				// it might have to retry and will only succeed immediately in
				// the event we can push.
				go func() {
					var err error
					//for {
					if _, ok := test.args.(*roachpb.GetRequest); ok {
						_, err = db.Get(key)
					} else {
						err = db.Put(key, "value")
					}
					doneCall <- errors.Wrapf(
						err, "%d: expected success on non-txn call to %s",
						i, test.args.Method())
				}()
				// Block until the non-transactional client has pushed us at
				// least once.
				util.SucceedsSoon(t, func() error {
					mu.Lock()
					defer mu.Unlock()
					if _, ok := mu.m[string(key)]; ok {
						return nil
					}
					return errors.New("non-transactional client has not pushed txn yet")
				})
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
		gr, err := db.Get(key)
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
	defer s.Stopper().Stop()
	dbCtx := client.DefaultDBContext()
	dbCtx.TxnRetryOptions.InitialBackoff = 1 * time.Millisecond
	db := createTestClientForUser(t, s.Stopper(), s.ServingAddr(), security.NodeUser, dbCtx)

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		key := []byte(fmt.Sprintf("%s/key-%t", testUser, commit))

		// Use snapshot isolation so non-transactional read can always push.
		err := db.Txn(func(txn *client.Txn) error {
			if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
				return err
			}

			// Put transactional value.
			if err := txn.Put(key, value); err != nil {
				return err
			}
			// Attempt to read outside of txn.
			if gr, err := db.Get(key); err != nil {
				return err
			} else if gr.Value != nil {
				return errors.Errorf("expected nil value; got %+v", gr.Value)
			}
			// Read within the transaction.
			if gr, err := txn.Get(key); err != nil {
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
			t.Errorf("expected success? %t; got %s", commit, err)
		} else if !commit && !testutils.IsError(err, "purposefully failing transaction") {
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
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	zoneConfig := config.ZoneConfig{
		ReplicaAttrs: []roachpb.Attributes{
			{Attrs: []string{"dc1", "mem"}},
			{Attrs: []string{"dc2", "mem"}},
		},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
	}

	key := roachpb.Key(testUser + "/zone-config")
	if err := db.Put(key, &zoneConfig); err != nil {
		t.Fatalf("unable to put proto: %s", err)
	}

	var readZoneConfig config.ZoneConfig
	if err := db.GetProto(key, &readZoneConfig); err != nil {
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
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	value := []byte("value")
	if err := db.Put(testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(testUser + "/key")
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if gr.Value.Timestamp.Equal(hlc.ZeroTimestamp) {
		t.Fatalf("expected non-zero timestamp; got empty")
	}
}

func TestClientPutInline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	value := []byte("value")
	if err := db.PutInline(testUser+"/key", value); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}
	gr, err := db.Get(testUser + "/key")
	if err != nil {
		t.Fatalf("unable to get value: %s", err)
	}
	if !bytes.Equal(value, gr.ValueBytes()) {
		t.Errorf("expected values equal; %s != %s", value, gr.ValueBytes())
	}
	if ts := gr.Value.Timestamp; !ts.Equal(hlc.ZeroTimestamp) {
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
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

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
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	keys := []roachpb.Key{}
	{
		b := &client.Batch{}
		for i := 0; i < 10; i++ {
			key := roachpb.Key(fmt.Sprintf("%s/key %02d", testUser, i))
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
	}

	// Now try 2 scans.
	{
		b := &client.Batch{}
		b.Scan(testUser+"/key 00", testUser+"/key 05")
		b.Scan(testUser+"/key 05", testUser+"/key 10")
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
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
		if err := db.Run(b); err != nil {
			t.Error(err)
		}
		checkKVs(t, b.Results[0].Rows, keys[4], 4, keys[3], 3, keys[2], 2)
		checkKVs(t, b.Results[1].Rows)
	}

	// Induce a non-transactional failure.
	{
		key := roachpb.Key("conditionalPut")
		if err := db.Put(key, "hello"); err != nil {
			t.Fatal(err)
		}

		b := &client.Batch{}
		b.CPut(key, "goodbyte", nil) // should fail
		if err := db.Run(b); err == nil {
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
		if err := db.Put(key, "hello"); err != nil {
			t.Fatal(err)
		}

		b := &client.Batch{}
		b.CPut(key, "goodbyte", nil) // should fail
		if err := db.Txn(func(txn *client.Txn) error {
			return txn.Run(b)
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

			if err := db.Txn(func(txn *client.Txn) error {
				txn.SetDebugName(fmt.Sprintf("test-%d", i), 0)

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
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Convenience loop: Crank up this number for testing this
	// more often. It'll increase test duration though.
	for k := 0; k < 5; k++ {
		if err := db.DelRange(testUser+"/value-0", testUser+"/value-1x"); err != nil {
			t.Fatalf("%d: unable to clean up: %s", k, err)
		}
		concurrentIncrements(db, t)
	}
}

// TestClientPermissions verifies permission enforcement.
func TestClientPermissions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	// NodeUser certs are required for all KV operations.
	// RootUser has no KV privileges whatsoever.
	nodeClient := createTestClientForUser(t, s.Stopper(), s.ServingAddr(),
		security.NodeUser, client.DefaultDBContext())
	rootClient := createTestClientForUser(t, s.Stopper(), s.ServingAddr(),
		security.RootUser, client.DefaultDBContext())

	testCases := []struct {
		path    string
		client  *client.DB
		allowed bool
	}{
		{"foo", rootClient, false},
		{"foo", nodeClient, true},

		{testUser + "/foo", rootClient, false},
		{testUser + "/foo", nodeClient, true},

		{testUser + "foo", rootClient, false},
		{testUser + "foo", nodeClient, true},

		{testUser, rootClient, false},
		{testUser, nodeClient, true},

		{"unknown/foo", rootClient, false},
		{"unknown/foo", nodeClient, true},
	}

	value := []byte("value")
	const matchErr = "is not allowed"
	for tcNum, tc := range testCases {
		err := tc.client.Put(tc.path, value)
		if (err == nil) != tc.allowed || (!tc.allowed && !testutils.IsError(err, matchErr)) {
			t.Errorf("#%d: expected allowed=%t, got err=%s", tcNum, tc.allowed, err)
		}
		_, err = tc.client.Get(tc.path)
		if (err == nil) != tc.allowed || (!tc.allowed && !testutils.IsError(err, matchErr)) {
			t.Errorf("#%d: expected allowed=%t, got err=%s", tcNum, tc.allowed, err)
		}
	}
}

// TestInconsistentReads tests that the methods that generate inconsistent reads
// generate outgoing requests with an INCONSISTENT read consistency.
func TestInconsistentReads(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Mock out DistSender's sender function to check the read consistency for
	// outgoing BatchRequests and return an empty reply.
	var senderFn client.SenderFunc
	senderFn = func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if ba.ReadConsistency != roachpb.INCONSISTENT {
			return nil, roachpb.NewErrorf("BatchRequest has unexpected ReadConsistency %s",
				ba.ReadConsistency)
		}
		return ba.CreateReply(), nil
	}
	db := client.NewDB(senderFn)

	prepInconsistent := func() *client.Batch {
		var b client.Batch
		b.Header.ReadConsistency = roachpb.INCONSISTENT
		return &b
	}

	// Perform inconsistent reads through the mocked sender function.
	{
		key := roachpb.Key([]byte("key"))
		b := prepInconsistent()
		b.Get(key)
		if err := db.Run(b); err != nil {
			t.Fatal(err)
		}
	}

	{
		b := prepInconsistent()
		key1 := roachpb.Key([]byte("key1"))
		key2 := roachpb.Key([]byte("key2"))
		b.Scan(key1, key2)
		if err := db.Run(b); err != nil {
			t.Fatal(err)
		}
	}

	{
		key := roachpb.Key([]byte("key"))
		b := db.NewBatch()
		b.Header.ReadConsistency = roachpb.INCONSISTENT
		b.Get(key)
		if err := db.Run(b); err != nil {
			t.Fatal(err)
		}
	}
}

// TestReadOnlyTxnObeysDeadline tests that read-only transactions obey the
// deadline.
func TestReadOnlyTxnObeysDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	if err := db.Put("k", "v"); err != nil {
		t.Fatal(err)
	}

	// Use txn.Exec instead of db.Txn to disable auto retry.
	txn := client.NewTxn(context.TODO(), *db)
	if err := txn.Exec(client.TxnExecOptions{AutoRetry: false, AutoCommit: true}, func(txn *client.Txn, _ *client.TxnExecOptions) error {
		// Set deadline to sometime in the past.
		txn.UpdateDeadlineMaybe(hlc.Timestamp{WallTime: timeutil.Now().Add(-time.Second).UnixNano()})
		_, err := txn.Get("k")
		return err
	}); !testutils.IsError(err, "txn aborted") {
		t.Fatal(err)
	}
}

// TestTxn_ReverseScan a simple test for Txn.ReverseScan
func TestTxn_ReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	keys := []roachpb.Key{}
	b := &client.Batch{}
	for i := 0; i < 10; i++ {
		key := roachpb.Key(fmt.Sprintf("%s/key/%02d", testUser, i))
		keys = append(keys, key)
		b.Put(key, i)
	}
	if err := db.Run(b); err != nil {
		t.Error(err)
	}

	err := db.Txn(func(txn *client.Txn) error {
		// Try reverse scans for all keys.
		{
			rows, err := txn.ReverseScan(testUser+"/key/00", testUser+"/key/10", 100)
			if err != nil {
				return err
			}
			checkKVs(t, rows,
				keys[9], 9, keys[8], 8, keys[7], 7, keys[6], 6, keys[5], 5,
				keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		}

		// Try reverse scans for half of the keys.
		{
			rows, err := txn.ReverseScan(testUser+"/key/00", testUser+"/key/05", 100)
			if err != nil {
				return err
			}
			checkKVs(t, rows, keys[4], 4, keys[3], 3, keys[2], 2, keys[1], 1, keys[0], 0)
		}

		// Try limit maximum rows.
		{
			rows, err := txn.ReverseScan(testUser+"/key/00", testUser+"/key/05", 3)
			if err != nil {
				return err
			}
			checkKVs(t, rows, keys[4], 4, keys[3], 3, keys[2], 2)
		}

		// Try reverse scan with the same start and end key.
		{
			rows, err := txn.ReverseScan(testUser+"/key/00", testUser+"/key/00", 100)
			if len(rows) > 0 {
				t.Errorf("expected empty, got %v", rows)
			}
			if err == nil {
				t.Errorf("expected a truncation error, got %s", err)
			}
		}

		// Try reverse scan with non-existent key.
		{
			rows, err := txn.ReverseScan(testUser+"/key/aa", testUser+"/key/bb", 100)
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
