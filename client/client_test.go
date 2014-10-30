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

// TestKVRetryOnSingleCall verifies that the client will retry as
// appropriate on write/write and read/write conflicts. In the case
// where the non-transactional put can push the txn, we expect the
// transaction's value to be written after all retries are complete.
func TestKVRetryOnSingleCall(t *testing.T) {
	client.TxnRetryOptions.Backoff = 1 * time.Millisecond

	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
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
		doneCall := make(chan struct{})
		kvClient.UserPriority = clientPri
		count := 0
		if err := kvClient.RunTransaction(&client.TransactionOptions{Isolation: test.isolation}, func(txn *client.KV) error {
			txn.UserPriority = txnPri
			count++
			if err := txn.Call(proto.Put, proto.PutArgs(key, []byte("txn-value")), &proto.PutResponse{}); err != nil {
				return err
			}
			wg := sync.WaitGroup{}
			if count == 1 {
				kvClient.Sender().(*notifyingSender).reset(&wg)
				go func() {
					if err := kvClient.Call(test.method, proto.PutArgs(key, []byte("value")), &proto.PutResponse{}); err != nil {
						t.Fatalf("%d: expected success on non-txn call to %s; got %s", i, err, test.method)
					}
					close(doneCall)
				}()
				kvClient.Sender().(*notifyingSender).wait()
			}
			return nil
		}); err != nil {
			t.Fatalf("%d: expected success writing transactionally; got %s", i, err)
		}

		<-doneCall
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

// TestKVRunTransaction verifies some simple transaction isolation
// semantics.
func TestKVRunTransaction(t *testing.T) {
	client.TxnRetryOptions.Backoff = 1 * time.Millisecond

	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
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

// TestKVGetAndPutProto verifies gets and puts of protobufs using the
// KV client's convenience methods.
func TestKVGetAndPutProto(t *testing.T) {
	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
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

// TestKVGetAndPutGob verifies gets and puts of Go objects using the
// KV client's convenience methods.
func TestKVGetAndPutGob(t *testing.T) {
	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
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
