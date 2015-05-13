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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
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

func (ss *notifyingSender) Send(ctx context.Context, call client.Call) {
	ss.wrapped.Send(ctx, call)
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

// createTestNotifyClient creates a new KV client which connects using
// an HTTP sender to the server at addr.
// It contains a waitgroup to allow waiting.
func createTestNotifyClient(addr string) *client.KV {
	sender, err := client.NewHTTPSender(addr, testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	return client.NewKV(nil, newNotifyingSender(sender))
}

// TestKVClientRetryNonTxn verifies that non-transactional client will
// succeed despite write/write and read/write conflicts. In the case
// where the non-transactional put can push the txn, we expect the
// transaction's value to be written after all retries are complete.
func TestKVClientRetryNonTxn(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	s.SetRangeRetryOptions(util.RetryOptions{
		Backoff:     1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		Constant:    2,
		MaxAttempts: 2,
	})
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.User = storage.UserRoot

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
		txnOpts := &client.TransactionOptions{
			Isolation:    test.isolation,
			UserPriority: txnPri,
		}
		err := kvClient.RunTransaction(txnOpts,
			func(txn *client.Txn) error {
				count++
				// Lay down the intent.
				if err := txn.Run(client.Put(key, []byte("txn-value"))); err != nil {
					return err
				}
				// The wait group lets us pause txn until after the non-txn method has run once.
				wg := sync.WaitGroup{}
				// On the first true, send the non-txn put or get.
				if count == 1 {
					// We use a "notifying" sender here, which allows us to know exactly when the
					// call has been processed; otherwise, we'd be dependent on timing.
					kvClient.Sender.(*notifyingSender).reset(&wg)
					// We must try the non-txn put or get in a goroutine because
					// it might have to retry and will only succeed immediately in
					// the event we can push.
					go func() {
						args := gogoproto.Clone(test.args).(proto.Request)
						args.Header().Key = key
						if put, ok := args.(*proto.PutRequest); ok {
							put.Value.Bytes = []byte("value")
						}
						reply := args.CreateReply()
						var err error
						for i := 0; ; i++ {
							err = kvClient.Run(client.Call{Args: args, Reply: reply})
							if _, ok := err.(*proto.WriteIntentError); !ok {
								break
							}
						}
						close(doneCall)
						if err != nil {
							t.Fatalf("%d: expected success on non-txn call to %s; got %s", i, err, args.Method())
						}
					}()
					kvClient.Sender.(*notifyingSender).wait()
				}
				return nil
			})
		if err != nil {
			t.Fatalf("%d: expected success writing transactionally; got %s", i, err)
		}

		// Make sure non-txn put or get has finished.
		<-doneCall

		// Get the current value to verify whether the txn happened first.
		call := client.Get(key)
		getReply := call.Reply.(*proto.GetResponse)
		if err := kvClient.Run(call); err != nil {
			t.Fatalf("%d: expected success getting %q: %s", i, key, err)
		}

		if _, isGet := test.args.(*proto.GetRequest); isGet || test.canPush {
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
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.TxnRetryOptions.Backoff = 1 * time.Millisecond
	kvClient.User = storage.UserRoot

	for _, commit := range []bool{true, false} {
		value := []byte("value")
		key := []byte(fmt.Sprintf("key-%t", commit))

		// Use snapshot isolation so non-transactional read can always push.
		err := kvClient.RunTransaction(&client.TransactionOptions{Isolation: proto.SNAPSHOT},
			func(txn *client.Txn) error {
				// Put transactional value.
				if err := txn.Run(client.Put(key, value)); err != nil {
					return err
				}
				// Attempt to read outside of txn.
				call := client.Get(key)
				gr := call.Reply.(*proto.GetResponse)
				if err := kvClient.Run(call); err != nil {
					return err
				}
				if gr.Value != nil {
					return util.Errorf("expected nil value; got %+v", gr.Value)
				}
				// Read within the transaction.
				call = client.Get(key)
				gr = call.Reply.(*proto.GetResponse)
				if err := txn.Run(call); err != nil {
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
		call := client.Get(key)
		gr := call.Reply.(*proto.GetResponse)
		err = kvClient.Run(call)
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
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.User = storage.UserRoot

	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs: []proto.Attributes{
			{Attrs: []string{"dc1", "mem"}},
			{Attrs: []string{"dc2", "mem"}},
		},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
	}

	key := proto.Key("zone-config")
	if err := kvClient.Run(client.PutProto(key, zoneConfig)); err != nil {
		t.Fatalf("unable to put proto: %s", err)
	}

	readZoneConfig := &proto.ZoneConfig{}
	call := client.GetProto(key, readZoneConfig)
	if err := kvClient.Run(call); err != nil {
		t.Fatalf("unable to get proto: %v", err)
	}
	reply := call.Reply.(*proto.GetResponse)
	if reply.Timestamp.Equal(proto.ZeroTimestamp) {
		t.Error("expected non-zero timestamp")
	}
}

// TestKVClientGetAndPut verifies gets and puts of using the KV
// client's convenience methods.
func TestKVClientGetAndPut(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.User = storage.UserRoot

	key := proto.Key("key")
	value := []byte("value")
	if err := kvClient.Run(client.Put(key, value)); err != nil {
		t.Fatalf("unable to put value: %s", err)
	}

	call := client.Get(key)
	if err := kvClient.Run(call); err != nil {
		t.Fatalf("unable to get value: %v", err)
	}
	reply := call.Reply.(*proto.GetResponse)
	if reply.Timestamp.Equal(proto.ZeroTimestamp) {
		t.Error("expected non-zero timestamp")
	}
	if !bytes.Equal(value, reply.Value.Bytes) {
		t.Errorf("expected values equal; %+v != %+v", value, reply.Value.Bytes)
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
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.User = storage.UserRoot

	if err := kvClient.Run(client.Put(proto.Key("a"), []byte{})); err != nil {
		t.Error(err)
	}
	if err := kvClient.Run(client.Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key: proto.Key("b"),
			},
			Value: proto.Value{
				Integer: gogoproto.Int64(0),
			},
		},
		Reply: &proto.PutResponse{},
	}); err != nil {
		t.Error(err)
	}

	getCall := client.Get(proto.Key("a"))
	getResp := getCall.Reply.(*proto.GetResponse)
	if err := kvClient.Run(getCall); err != nil {
		t.Error(err)
	}
	if bytes := getResp.Value.Bytes; bytes == nil || len(bytes) != 0 {
		t.Errorf("expected non-nil empty byte slice; got %q", bytes)
	}
	getCall = client.Get(proto.Key("b"))
	getResp = getCall.Reply.(*proto.GetResponse)
	if err := kvClient.Run(getCall); err != nil {
		t.Error(err)
	}
	if intVal := getResp.Value.Integer; intVal == nil || *intVal != 0 {
		t.Errorf("expected non-nil 0-valued integer; got %p, %d", getResp.Value.Integer, getResp.Value.GetInteger())
	}
}

// TestKVClientBatch runs a batch of increment calls and then verifies
// the results.
func TestKVClientBatch(t *testing.T) {
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.User = storage.UserRoot

	keys := []proto.Key{}
	calls := []client.Call{}
	for i := 0; i < 10; i++ {
		key := proto.Key(fmt.Sprintf("key %02d", i))
		keys = append(keys, key)
		calls = append(calls, client.Increment(key, int64(i)))
	}

	if err := kvClient.Run(calls...); err != nil {
		t.Error(err)
	}

	for i, call := range calls {
		reply := call.Reply.(*proto.IncrementResponse)
		if reply.NewValue != int64(i) {
			t.Errorf("%d: expected %d; got %d", i, i, reply.NewValue)
		}
	}

	// Now try 2 scans.
	calls = []client.Call{
		client.Scan(proto.Key("key 00"), proto.Key("key 05"), 0),
		client.Scan(proto.Key("key 05"), proto.Key("key 10"), 0),
	}
	if err := kvClient.Run(calls...); err != nil {
		t.Error(err)
	}

	scan1 := calls[0].Reply.(*proto.ScanResponse)
	scan2 := calls[1].Reply.(*proto.ScanResponse)
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

// This is an example for using the Run() method to Put and then Get
// a value for a given key.
func ExampleKV_Run1() {
	// Using built-in test server for this example code.
	serv := server.StartTestServer(nil)
	defer serv.Stop()

	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(serv.ServingAddr(), testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
	kvClient.User = storage.UserRoot

	key := proto.Key("a")
	value := []byte{1, 2, 3, 4}

	// Store test value.
	if err := kvClient.Run(client.Put(key, value)); err != nil {
		log.Fatal(err)
	}

	// Retrieve test value using same key.
	getCall := client.Get(key)
	getResp := getCall.Reply.(*proto.GetResponse)
	if err := kvClient.Run(getCall); err != nil {
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

// This is an example for using the Run() method to submit
// multiple Key Value API operations to be run in parallel. Flush() is
// then used to begin execution of all the prepared operations.
func ExampleKV_RunMultiple() {
	// Using built-in test server for this example code.
	serv := server.StartTestServer(nil)
	defer serv.Stop()

	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(serv.ServingAddr(), testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
	kvClient.User = storage.UserRoot

	// Insert test data.
	batchSize := 12
	keys := make([]string, batchSize)
	values := make([][]byte, batchSize)
	calls := []client.Call{}
	for i := 0; i < batchSize; i++ {
		keys[i] = fmt.Sprintf("key-%03d", i)
		values[i] = []byte(fmt.Sprintf("value-%03d", i))
		calls = append(calls, client.Put(proto.Key(keys[i]), values[i]))
	}

	// Run all puts for parallel execution.
	if err := kvClient.Run(calls...); err != nil {
		log.Fatal(err)
	}

	// Scan for the newly inserted rows in parallel.
	numScans := 3
	rowsPerScan := batchSize / numScans
	calls = nil
	for i := 0; i < numScans; i++ {
		firstKey := proto.Key(keys[i*rowsPerScan])
		lastKey := proto.Key(keys[((i+1)*rowsPerScan)-1])
		calls = append(calls, client.Scan(firstKey, lastKey.Next(), int64(rowsPerScan)))
	}
	// Run all scans for parallel execution.
	if err := kvClient.Run(calls...); err != nil {
		log.Fatal(err)
	}

	// Check results which may be returned out-of-order from creation.
	var matchCount int
	for i := 0; i < numScans; i++ {
		scanResponse := calls[i].Reply.(*proto.ScanResponse)
		for _, keyVal := range scanResponse.Rows {
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

	// Key Value Client initialization.
	sender, err := client.NewHTTPSender(serv.ServingAddr(), testutils.NewTestBaseContext())
	if err != nil {
		log.Fatal(err)
	}
	kvClient := client.NewKV(nil, sender)
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
	putOpts := client.TransactionOptions{Name: "example put"}
	err = kvClient.RunTransaction(&putOpts, func(txn *client.Txn) error {
		for i := 0; i < numKVPairs; i++ {
			txn.Prepare(client.Put(proto.Key(keys[i]), values[i]))
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
	getResponses := make([]*proto.GetResponse, numKVPairs)
	getOpts := client.TransactionOptions{Name: "example get"}
	err = kvClient.RunTransaction(&getOpts, func(txn *client.Txn) error {
		for i := 0; i < numKVPairs; i++ {
			call := client.Get(proto.Key(keys[i]))
			getResponses[i] = call.Reply.(*proto.GetResponse)
			txn.Prepare(call)
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

// concurrentIncrements starts two Goroutines in parallel, both of which
// read the integers stored at the other's key and add it onto their own.
// It is checked that the outcome is serializable, i.e. exactly one of the
// two Goroutines (the later write) sees the previous write by the other.
func concurrentIncrements(kvClient *client.KV, t *testing.T) {
	// wgStart waits for all transactions to line up, wgEnd has the main
	// function wait for them to finish.
	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(2 + 1)
	wgEnd.Add(2)

	for i := 0; i < 2; i++ {
		go func(i int) {
			// Read the other key, write key i.
			readKey := []byte(fmt.Sprintf("value-%d", (i+1)%2))
			writeKey := []byte(fmt.Sprintf("value-%d", i))
			defer wgEnd.Done()
			wgStart.Done()
			// Wait until the other goroutines are running.
			wgStart.Wait()

			txnOpts := &client.TransactionOptions{
				Name: fmt.Sprintf("test-%d", i),
			}
			if err := kvClient.RunTransaction(txnOpts, func(txn *client.Txn) error {
				// Retrieve the other key.
				call := client.Get(readKey)
				gr := call.Reply.(*proto.GetResponse)
				if err := txn.Run(call); err != nil {
					return err
				}

				otherValue := int64(0)
				if gr.Value != nil && gr.Value.Integer != nil {
					otherValue = *gr.Value.Integer
				}

				if err := txn.Run(client.Increment(writeKey, 1+otherValue)); err != nil {
					return err
				}

				return nil
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
		readKey := []byte(fmt.Sprintf("value-%d", i))
		call := client.Get(readKey)
		gr := call.Reply.(*proto.GetResponse)
		if err := kvClient.Run(call); err != nil {
			log.Fatal(err)
		}
		if gr.Value == nil || gr.Value.Integer == nil {
			t.Fatalf("unexpected empty key: %s=%v", readKey, gr.Value)
		}
		total += *gr.Value.Integer
		results = append(results, *gr.Value.Integer)
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
	s := server.StartTestServer(t)
	defer s.Stop()
	kvClient := createTestNotifyClient(s.ServingAddr())
	kvClient.User = storage.UserRoot

	// Convenience loop: Crank up this number for testing this
	// more often. It'll increase test duration though.
	for k := 0; k < 5; k++ {
		if err := kvClient.Run(
			client.DeleteRange([]byte("value-0"), []byte("value-1x"))); err != nil {
			t.Fatalf("%d: unable to clean up: %v", k, err)
		}
		concurrentIncrements(kvClient, t)
	}
}

const valueSize = 1 << 10

func setupClientBenchData(useRPC, useSSL bool, numVersions, numKeys int, b *testing.B) (
	*server.TestServer, *client.KV) {
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
	s.Engines = []engine.Engine{engine.NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc, cacheSize)}
	if err := s.Start(); err != nil {
		b.Fatal(err)
	}

	var kv *client.KV
	if useRPC {
		sender, err := client.NewRPCSender(s.ServingAddr(), &s.Ctx.Context)
		if err != nil {
			b.Fatal(err)
		}
		kv = client.NewKV(nil, sender)
	} else {
		// The test context contains the security settings.
		sender, err := client.NewHTTPSender(s.ServingAddr(), &s.Ctx.Context)
		if err != nil {
			b.Fatal(err)
		}
		kv = client.NewKV(nil, sender)
	}
	kv.User = storage.UserRoot

	if exists {
		return s, kv
	}

	rng, _ := util.NewPseudoRand()
	keys := make([]proto.Key, numKeys)
	nvs := make([]int, numKeys)
	for t := 1; t <= numVersions; t++ {
		var calls []client.Call
		for i := 0; i < numKeys; i++ {
			if t == 1 {
				keys[i] = proto.Key(encoding.EncodeUvarint([]byte("key-"), uint64(i)))
				nvs[i] = int(rand.Int31n(int32(numVersions)) + 1)
			}
			// Only write values if this iteration is less than the random
			// number of versions chosen for this key.
			if t <= nvs[i] {
				call := client.Put(proto.Key(keys[i]), util.RandBytes(rng, valueSize))
				call.Args.Header().Timestamp = proto.Timestamp{WallTime: time.Now().UnixNano()}
				calls = append(calls, call)
			}
			if (i+1)%1000 == 0 {
				if err := kv.Run(calls...); err != nil {
					b.Fatal(err)
				}
				calls = nil
			}
		}
		if err := kv.Run(calls...); err != nil {
			b.Fatal(err)
		}
	}

	if r, ok := s.Engines[0].(*engine.RocksDB); ok {
		r.CompactRange(nil, nil)
	}

	return s, kv
}

// runClientScan first creates test data (and resets the benchmarking
// timer). It then performs b.N client scans in increments of numRows
// keys over all of the data, restarting at the beginning of the
// keyspace, as many times as necessary.
func runClientScan(useRPC, useSSL bool, numRows, numVersions int, b *testing.B) {
	const numKeys = 100000

	s, kv := setupClientBenchData(useRPC, useSSL, numVersions, numKeys, b)
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
			call := client.Scan(proto.Key(startKey), proto.Key(endKey), int64(numRows))
			call.Args.Header().Timestamp = proto.Timestamp{WallTime: time.Now().UnixNano()}
			resp := call.Reply.(*proto.ScanResponse)
			if err := kv.Run(call); err != nil {
				b.Fatalf("failed scan: %s", err)
			}
			if len(resp.Rows) != numRows {
				b.Fatalf("failed to scan: %d != %d", len(resp.Rows), numRows)
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
