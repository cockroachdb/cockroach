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
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// notifyingSender is a sender which can set up a notification channel
// (on call to reset()) for clients which need to wait on a command
// being sent.
type notifyingSender struct {
	waiter  *sync.WaitGroup
	wrapped client.KVSender
}

func (ss *notifyingSender) reset(waiter *sync.WaitGroup) {
	waiter.Add(1)
	ss.waiter = waiter
}

func (ss *notifyingSender) wait() {
	ss.waiter.Wait()
	ss.waiter = nil
}

func (ss *notifyingSender) Send(call *client.Call) {
	ss.wrapped.Send(call)
	if ss.waiter != nil {
		ss.waiter.Done()
	}
}

func (ss *notifyingSender) Close() {}

func newNotifyingSender(wrapped client.KVSender) *notifyingSender {
	return &notifyingSender{
		wrapped: wrapped,
	}
}

// createTestClient creates a new KV client which connects using
// an HTTP sender to the server at addr.
func createTestClient(addr string) *client.KV {
	sender := newNotifyingSender(client.NewHTTPSender(addr, &http.Transport{
		TLSClientConfig: rpc.LoadInsecureTLSConfig().Config(),
	}))
	return client.NewKV(sender, nil)
}

// TestKVClientRetryNonTxn verifies that non-transactional client will
// succeed despite write/write and read/write conflicts. In the case
// where the non-transactional put can push the txn, we expect the
// transaction's value to be written after all retries are complete.
func TestKVClientRetryNonTxn(t *testing.T) {
	storage.RangeRetryOptions.Backoff = 1 * time.Millisecond
	storage.RangeRetryOptions.MaxAttempts = 2
	client.TxnRetryOptions.Backoff = 1 * time.Millisecond

	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestClient(s.HTTPAddr)
	kvClient.User = storage.UserRoot

	testCases := []struct {
		method      string
		isolation   proto.IsolationType
		canPush     bool
		expAttempts int
	}{
		// Write/write conflicts.
		{proto.Put, proto.SNAPSHOT, true, 2},
		{proto.Put, proto.SERIALIZABLE, true, 2},
		{proto.Put, proto.SNAPSHOT, false, 1},
		{proto.Put, proto.SERIALIZABLE, false, 1},
		// Read/write conflicts.
		{proto.Get, proto.SNAPSHOT, true, 1},
		{proto.Get, proto.SERIALIZABLE, true, 2},
		{proto.Get, proto.SNAPSHOT, false, 1},
		{proto.Get, proto.SERIALIZABLE, false, 1},
	}
	// Lay down a write intent using a txn and attempt to write to same
	// key. Try this twice--once with priorities which will allow the
	// intent to be pushed and once with priorities which will not.
	for i, test := range testCases {
		log.Infof("starting test case %d", i)
		key := proto.Key(fmt.Sprintf("key-%d", i))
		txnPri := int32(-1)
		clientPri := int32(-1)
		if test.canPush {
			clientPri = -2
		} else {
			txnPri = -2
		}
		kvClient.UserPriority = clientPri

		// doneCall signals when the non-txn read or write has completed.
		doneCall := make(chan struct{})
		count := 0 // keeps track of retries
		if err := kvClient.RunTransaction(&client.TransactionOptions{Isolation: test.isolation}, func(txn *client.KV) error {
			txn.UserPriority = txnPri
			count++
			// Lay down the intent.
			if err := txn.Call(proto.Put, proto.PutArgs(key, []byte("txn-value")), &proto.PutResponse{}); err != nil {
				return err
			}
			// The wait group lets us pause txn until after the non-txn method has run once.
			wg := sync.WaitGroup{}
			// On the first true, send the non-txn put or get.
			if count == 1 {
				// We use a "notifying" sender here, which allows us to know exactly when the
				// call has been processed; otherwise, we'd be dependent on timing.
				kvClient.Sender().(*notifyingSender).reset(&wg)
				// We must try the non-txn put or get in a goroutine because
				// it might have to retry and will only succeed immediately in
				// the event we can push.
				go func() {
					args, reply, err := proto.CreateArgsAndReply(test.method)
					if err != nil {
						t.Errorf("error creating args and reply for method %s: %s", test.method, err)
					}
					args.Header().Key = key
					if test.method == proto.Put {
						args.(*proto.PutRequest).Value.Bytes = []byte("value")
					}
					for i := 0; ; i++ {
						err = kvClient.Call(test.method, args, reply)
						if _, ok := err.(*proto.WriteIntentError); !ok {
							break
						}
					}
					close(doneCall)
					if err != nil {
						t.Fatalf("%d: expected success on non-txn call to %s; got %s", i, err, test.method)
					}
				}()
				kvClient.Sender().(*notifyingSender).wait()
			}
			return nil
		}); err != nil {
			t.Fatalf("%d: expected success writing transactionally; got %s", i, err)
		}

		// Make sure non-txn put or get has finished.
		<-doneCall

		// Get the current value to verify whether the txn happened first.
		getReply := &proto.GetResponse{}
		if err := kvClient.Call(proto.Get, proto.GetArgs(key), getReply); err != nil {
			t.Fatalf("%d: expected success getting %q: %s", i, key, err)
		}
		if test.canPush || test.method == proto.Get {
			if !bytes.Equal(getReply.Value.Bytes, []byte("txn-value")) {
				t.Errorf("%d: expected \"txn-value\"; got %q", i, getReply.Value.Bytes)
			}
		} else {
			if !bytes.Equal(getReply.Value.Bytes, []byte("value")) {
				t.Errorf("%d: expected \"value\"; got %q", i, getReply.Value.Bytes)
			}
		}
		if count != test.expAttempts {
			t.Errorf("%d: expected %d attempt(s); got %d", i, test.expAttempts, count)
		}
	}
}

// TestKVClientRunTransaction verifies some simple transaction isolation
// semantics.
func TestKVClientRunTransaction(t *testing.T) {
	client.TxnRetryOptions.Backoff = 1 * time.Millisecond

	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestClient(s.HTTPAddr)
	kvClient.User = storage.UserRoot

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		key := []byte(fmt.Sprintf("key-%t", commit))

		// Use snapshot isolation so non-transactional read can always push.
		err := kvClient.RunTransaction(&client.TransactionOptions{Isolation: proto.SNAPSHOT}, func(txn *client.KV) error {
			// Put transactional value.
			if err := txn.Call(proto.Put, proto.PutArgs(key, value), &proto.PutResponse{}); err != nil {
				return err
			}
			// Attempt to read outside of txn.
			gr := &proto.GetResponse{}
			if err := kvClient.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
				return err
			}
			if gr.Value != nil {
				return util.Errorf("expected nil value; got %+v", gr.Value)
			}
			// Read within the transaction.
			if err := txn.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
				return err
			}
			if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
				return util.Errorf("expected value %q; got %q", value, gr.Value.Bytes)
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
		gr := &proto.GetResponse{}
		err = kvClient.Call(proto.Get, proto.GetArgs(key), gr)
		if commit {
			if err != nil || gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
				t.Errorf("expected success reading value: %+v, %s", gr.Value, err)
			}
		} else {
			if err != nil || gr.Value != nil {
				t.Errorf("expected success and nil value: %+v, %s", gr.Value, err)
			}
		}
	}
}

// TestKVClientGetAndPutProto verifies gets and puts of protobufs using the
// KV client's convenience methods.
func TestKVClientGetAndPutProto(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestClient(s.HTTPAddr)
	kvClient.User = storage.UserRoot

	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			proto.Attributes{Attrs: []string{"dc1", "mem"}},
			proto.Attributes{Attrs: []string{"dc2", "mem"}},
		},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
	}

	key := proto.Key("zone-config")
	if err := kvClient.PutProto(key, zoneConfig); err != nil {
		t.Fatalf("unable to put proto: %s", err)
	}

	readZoneConfig := &proto.ZoneConfig{}
	ok, ts, err := kvClient.GetProto(key, readZoneConfig)
	if !ok || err != nil {
		t.Fatalf("unable to get proto ok? %t: %s", ok, err)
	}
	if ts.Equal(proto.MinTimestamp) {
		t.Error("expected non-zero timestamp")
	}
	if !gogoproto.Equal(zoneConfig, readZoneConfig) {
		t.Errorf("expected zone configs equal; %+v != %+v", zoneConfig, readZoneConfig)
	}
}

// TestKVClientGetAndPutGob verifies gets and puts of Go objects using the
// KV client's convenience methods.
func TestKVClientGetAndPutGob(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestClient(s.HTTPAddr)
	kvClient.User = storage.UserRoot

	obj := map[string]map[int]int{
		"foo": map[int]int{
			1: 100,
			2: 101,
		},
		"bar": map[int]int{
			3: 200,
			4: 201,
		},
	}

	key := proto.Key("gob-key")
	if err := kvClient.PutI(key, obj); err != nil {
		t.Fatalf("unable to put object: %s", err)
	}

	readObj := map[string]map[int]int{}
	ok, ts, err := kvClient.GetI(key, &readObj)
	if !ok || err != nil {
		t.Fatalf("unable to get object ok? %t: %s", ok, err)
	}
	if ts.Equal(proto.MinTimestamp) {
		t.Error("expected non-zero timestamp")
	}
	if !reflect.DeepEqual(obj, readObj) {
		t.Errorf("expected objects equal; %+v != %+v", obj, readObj)
	}
}

// TestKVClientEmptyValues verifies that empty values are preserved
// for both empty []byte and integer=0. This used to fail when we
// allowed the protobufs to be gob-encoded using the default go rpc
// gob codec because gob treats pointer values and non-pointer values
// as equivalent and elides zero-valued defaults on decode.
func TestKVClientEmptyValues(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestClient(s.HTTPAddr)
	kvClient.User = storage.UserRoot

	kvClient.Call(proto.Put, proto.PutArgs(proto.Key("a"), []byte{}), &proto.PutResponse{})
	kvClient.Call(proto.Put, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key("b"),
		},
		Value: proto.Value{
			Integer: gogoproto.Int64(0),
		},
	}, &proto.PutResponse{})

	getResp := &proto.GetResponse{}
	kvClient.Call(proto.Get, proto.GetArgs(proto.Key("a")), getResp)
	if bytes := getResp.Value.Bytes; bytes == nil || len(bytes) != 0 {
		t.Errorf("expected non-nil empty byte slice; got %q", bytes)
	}
	kvClient.Call(proto.Get, proto.GetArgs(proto.Key("b")), getResp)
	if intVal := getResp.Value.Integer; intVal == nil || *intVal != 0 {
		t.Errorf("expected non-nil 0-valued integer; got %p, %d", getResp.Value.Integer, getResp.Value.GetInteger())
	}
}

// TestKVClientPrepareAndFlush prepares a sequence of increment
// calls and then flushes them and verifies the results.
func TestKVClientPrepareAndFlush(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestClient(s.HTTPAddr)
	kvClient.User = storage.UserRoot

	replies := []*proto.IncrementResponse{}
	keys := []proto.Key{}
	for i := 0; i < 10; i++ {
		key := proto.Key(fmt.Sprintf("key %02d", i))
		keys = append(keys, key)
		reply := &proto.IncrementResponse{}
		replies = append(replies, reply)
		kvClient.Prepare(proto.Increment, proto.IncrementArgs(key, int64(i)), reply)
	}

	if err := kvClient.Flush(); err != nil {
		t.Fatal(err)
	}

	for i, reply := range replies {
		if reply.NewValue != int64(i) {
			t.Errorf("%d: expected %d; got %d", i, i, reply.NewValue)
		}
	}

	// Now try 2 scans.
	scan1 := &proto.ScanResponse{}
	scan2 := &proto.ScanResponse{}
	kvClient.Prepare(proto.Scan, proto.ScanArgs(proto.Key("key 00"), proto.Key("key 05"), 0), scan1)
	kvClient.Prepare(proto.Scan, proto.ScanArgs(proto.Key("key 05"), proto.Key("key 10"), 0), scan2)

	if err := kvClient.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(scan1.Rows) != 5 || len(scan2.Rows) != 5 {
		t.Errorf("expected scan results to include 5 and 5 rows; got %d and %d",
			len(scan1.Rows), len(scan2.Rows))
	}
	for i := 0; i < 5; i++ {
		if key := scan1.Rows[i].Key; !key.Equal(keys[i]) {
			t.Errorf("expected scan1 key %d to be %q; got %q", i, keys[i], key)
		}
		if val := scan1.Rows[i].Value.GetInteger(); val != int64(i) {
			t.Errorf("expected scan1 result %d to be %d; got %d", i, i, val)
		}

		if key := scan2.Rows[i].Key; !key.Equal(keys[i+5]) {
			t.Errorf("expected scan2 key %d to be %q; got %q", i, keys[i+5], key)
		}
		if val := scan2.Rows[i].Value.GetInteger(); val != int64(i+5) {
			t.Errorf("expected scan2 result %d to be %d; got %d", i, i+5, val)
		}
	}
}

// This is an example for using the Call() method to Put and then Get
// a value for a given key.
func ExampleKV_Call() {
	// Using built-in test server for this example code.
	serv := server.StartTestServer(nil)
	defer serv.Stop()

	// Replace with actual host:port address string (ex "localhost:8080") for server cluster.
	serverAddress := serv.HTTPAddr

	// Key Value Client initialization.
	sender := client.NewHTTPSender(serverAddress, &http.Transport{
		TLSClientConfig: rpc.LoadInsecureTLSConfig().Config(),
	})
	kvClient := client.NewKV(sender, nil)
	kvClient.User = storage.UserRoot

	key := proto.Key("a")
	value := []byte{1, 2, 3, 4}

	// Store test value.
	putResp := &proto.PutResponse{}
	if err := kvClient.Call(proto.Put, proto.PutArgs(key, value), putResp); err != nil {
		log.Fatal(err)
	}

	// Retrieve test value using same key.
	getResp := &proto.GetResponse{}
	if err := kvClient.Call(proto.Get, proto.GetArgs(key), getResp); err != nil {
		log.Fatal(err)
	}

	// Data validation.
	if getResp.Value == nil {
		log.Fatal("No value returned.")
	}
	if !bytes.Equal(value, getResp.Value.Bytes) {
		log.Fatal("Data mismatch on retrieved value.")
	}

	fmt.Println("Client example done.")
	// Output: Client example done.
}

// This is an example for using the Prepare() method to submit
// multiple Key Value API operations to be run in parallel. Flush() is
// then used to begin execution of all the prepared operations.
func ExampleKV_Prepare() {
	// Using built-in test server for this example code.
	serv := server.StartTestServer(nil)
	defer serv.Stop()

	// Replace with actual host:port address string (ex "localhost:8080") for server cluster.
	serverAddress := serv.HTTPAddr

	// Key Value Client initialization.
	sender := client.NewHTTPSender(serverAddress, &http.Transport{
		TLSClientConfig: rpc.LoadInsecureTLSConfig().Config(),
	})
	kvClient := client.NewKV(sender, nil)
	kvClient.User = storage.UserRoot

	// Insert test data.
	batchSize := 12
	keys := make([]string, batchSize)
	values := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = fmt.Sprintf("key-%03d", i)
		values[i] = []byte(fmt.Sprintf("value-%03d", i))

		putReq := proto.PutArgs(proto.Key(keys[i]), values[i])
		putResp := &proto.PutResponse{}
		kvClient.Prepare(proto.Put, putReq, putResp)
	}

	// Flush all puts for parallel execution.
	if err := kvClient.Flush(); err != nil {
		log.Fatal(err)
	}

	// Scan for the newly inserted rows in parallel.
	numScans := 3
	rowsPerScan := batchSize / numScans
	scanResponses := make([]proto.ScanResponse, numScans)
	for i := 0; i < numScans; i++ {
		firstKey := proto.Key(keys[i*rowsPerScan])
		lastKey := proto.Key(keys[((i+1)*rowsPerScan)-1])
		kvClient.Prepare(proto.Scan, proto.ScanArgs(firstKey, lastKey.Next(), int64(rowsPerScan)), &scanResponses[i])
	}
	// Flush all scans for parallel execution.
	if err := kvClient.Flush(); err != nil {
		log.Fatal(err)
	}

	// Check results which may be returned out-of-order from creation.
	var matchCount int
	for i := 0; i < numScans; i++ {
		for _, keyVal := range scanResponses[i].Rows {
			currKey := keyVal.Key
			currValue := keyVal.Value.Bytes
			for j, origKey := range keys {
				if bytes.Equal(currKey, proto.Key(origKey)) && bytes.Equal(currValue, values[j]) {
					matchCount++
				}
			}
		}
	}
	if matchCount != batchSize {
		log.Fatal("Data mismatch.")
	}

	fmt.Println("Prepare Flush example done.")
	// Output: Prepare Flush example done.
}

// This is an example for using the RunTransaction() method to submit
// multiple Key Value API operations inside a transaction.
func ExampleKV_RunTransaction() {
	// Using built-in test server for this example code.
	serv := server.StartTestServer(nil)
	defer serv.Stop()

	// Replace with actual host:port address string (ex "localhost:8080") for server cluster.
	serverAddress := serv.HTTPAddr

	// Key Value Client initialization.
	sender := client.NewHTTPSender(serverAddress, &http.Transport{
		TLSClientConfig: rpc.LoadInsecureTLSConfig().Config(),
	})
	kvClient := client.NewKV(sender, nil)
	kvClient.User = storage.UserRoot

	// Create test data.
	numKVPairs := 10
	keys := make([]string, numKVPairs)
	values := make([][]byte, numKVPairs)
	for i := 0; i < numKVPairs; i++ {
		keys[i] = fmt.Sprintf("testkey-%03d", i)
		values[i] = []byte(fmt.Sprintf("testvalue-%03d", i))
	}

	// Insert all KV pairs inside a transaction.
	putOpts := client.TransactionOptions{Name: "example put", Isolation: proto.SERIALIZABLE}
	err := kvClient.RunTransaction(&putOpts, func(txn *client.KV) error {
		for i := 0; i < numKVPairs; i++ {
			txn.Prepare(proto.Put, proto.PutArgs(proto.Key(keys[i]), values[i]), &proto.PutResponse{})
		}
		// Note that the KV client is flushed automatically on transaction
		// commit. Invoking Flush after individual API methods is only
		// required if the result needs to be received to take conditional
		// action.
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Read back KV pairs inside a transaction.
	getResponses := make([]proto.GetResponse, numKVPairs)
	getOpts := client.TransactionOptions{Name: "example get", Isolation: proto.SERIALIZABLE}
	err = kvClient.RunTransaction(&getOpts, func(txn *client.KV) error {
		for i := 0; i < numKVPairs; i++ {
			txn.Prepare(proto.Get, proto.GetArgs(proto.Key(keys[i])), &getResponses[i])
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Check results.
	for i, getResp := range getResponses {
		if getResp.Value == nil {
			log.Fatal("No value returned for ", keys[i])
		} else {
			if !bytes.Equal(values[i], getResp.Value.Bytes) {
				log.Fatal("Data mismatch for ", keys[i], ", got: ", getResp.Value.Bytes)
			}
		}
	}

	fmt.Println("Transaction example done.")
	// Output: Transaction example done.
}
