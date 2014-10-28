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

package kv_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	yaml "gopkg.in/yaml.v1"
)

// TestKVDBCoverage verifies that all methods may be invoked on the
// key value database.
func TestKVDBCoverage(t *testing.T) {
	once.Do(func() { startServer(t) })

	kvClient := client.NewKV(serverAddr, rpc.LoadInsecureTLSConfig().Config())
	key := proto.Key("a")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Put first value at key.
	putReq := &proto.PutRequest{Value: proto.Value{Bytes: value1}}
	putReq.Key = key
	putResp := &proto.PutResponse{}
	if err := kvClient.Call(proto.Put, putReq, putResp); err != nil || putResp.Error != nil {
		t.Fatalf("%s, %s", err, putResp.GoError())
	}

	// Verify put with Contains.
	containsReq := &proto.ContainsRequest{}
	containsReq.Key = key
	containsResp := &proto.ContainsResponse{}
	if err := kvClient.Call(proto.Contains, containsReq, containsResp); err != nil || containsResp.Error != nil {
		t.Fatal("%s, %s", err, containsResp.GoError())
	}
	if !containsResp.Exists {
		t.Error("expected contains to be true")
	}

	// Conditional put should succeed, changing value1 to value2.
	cPutReq := &proto.ConditionalPutRequest{
		Value:    proto.Value{Bytes: value2},
		ExpValue: &proto.Value{Bytes: value1},
	}
	cPutReq.Key = key
	cPutResp := &proto.ConditionalPutResponse{}
	if err := kvClient.Call(proto.ConditionalPut, cPutReq, cPutResp); err != nil || cPutResp.Error != nil {
		t.Fatalf("%s, %s", err, cPutResp.GoError())
	}

	// Verify get by looking up conditional put value.
	getReq := &proto.GetRequest{}
	getReq.Key = key
	getResp := &proto.GetResponse{}
	if err := kvClient.Call(proto.Get, getReq, getResp); err != nil || getResp.Error != nil {
		t.Fatalf("%s, %s", err, getResp.GoError())
	}
	if !bytes.Equal(getResp.Value.Bytes, value2) {
		t.Errorf("expected get to return %q; got %q", value2, getResp.Value.Bytes)
	}

	// Increment.
	incrReq := &proto.IncrementRequest{Increment: 10}
	incrReq.Key = proto.Key("i")
	incrResp := &proto.IncrementResponse{}
	if err := kvClient.Call(proto.Increment, incrReq, incrResp); err != nil || incrResp.Error != nil {
		t.Fatalf("%s, %s", err, incrResp.GoError())
	}
	if incrResp.NewValue != incrReq.Increment {
		t.Errorf("expected increment new value of %d; got %d", incrReq.Increment, incrResp.NewValue)
	}

	// Delete conditional put value.
	delReq := &proto.DeleteRequest{}
	delReq.Key = key
	delResp := &proto.DeleteResponse{}
	if err := kvClient.Call(proto.Delete, delReq, delResp); err != nil || delResp.Error != nil {
		t.Fatalf("%s, %s", err, delResp.GoError())
	}
	if err := kvClient.Call(proto.Contains, containsReq, containsResp); err != nil || containsResp.Error != nil {
		t.Fatal("%s, %s", err, containsResp.GoError())
	}
	if containsResp.Exists {
		t.Error("expected contains to be false after delete")
	}

	// Put values in anticipation of scan & delete range.
	keyValues := []proto.KeyValue{
		{Key: proto.Key("a"), Value: proto.Value{Bytes: value1}},
		{Key: proto.Key("b"), Value: proto.Value{Bytes: value2}},
		{Key: proto.Key("c"), Value: proto.Value{Bytes: value3}},
	}
	for _, kv := range keyValues {
		putReq.Key, putReq.Value = kv.Key, kv.Value
		if err := kvClient.Call(proto.Put, putReq, putResp); err != nil || putResp.Error != nil {
			t.Fatalf("%s, %s", err, putResp.GoError())
		}
	}
	scanReq := &proto.ScanRequest{}
	scanReq.Key = proto.Key("a")
	scanReq.EndKey = proto.Key("c").Next()
	scanResp := &proto.ScanResponse{}
	if err := kvClient.Call(proto.Scan, scanReq, scanResp); err != nil || scanResp.Error != nil {
		t.Fatalf("%s, %s", err, scanResp.GoError())
	}
	if len(scanResp.Rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(scanResp.Rows))
	}
	for i, kv := range keyValues {
		if !bytes.Equal(scanResp.Rows[i].Value.Bytes, kv.Value.Bytes) {
			t.Errorf("%d: key %q, values %q != %q", i, kv.Key, scanResp.Rows[i].Value.Bytes, kv.Value.Bytes)
		}
	}

	deleteRangeReq := &proto.DeleteRangeRequest{}
	deleteRangeReq.Key = proto.Key("a")
	deleteRangeReq.EndKey = proto.Key("c").Next()
	deleteRangeResp := &proto.DeleteRangeResponse{}
	if err := kvClient.Call(proto.DeleteRange, deleteRangeReq, deleteRangeResp); err != nil || deleteRangeResp.Error != nil {
		t.Fatalf("%s, %s", err, deleteRangeResp.GoError())
	}
	if deleteRangeResp.NumDeleted != int64(len(keyValues)) {
		t.Fatalf("expected %d rows in deleterange; got %d", len(keyValues), deleteRangeResp.NumDeleted)
	}
}

// TestKVDBCanonicalMethod verifies that the method path is translated
// into canonical form when received by the server.
func TestKVDBCanonicalMethod(t *testing.T) {
	once.Do(func() { startServer(t) })

	putReq := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key("a"),
		},
		Value: proto.Value{Bytes: []byte("value")},
	}
	body, err := gogoproto.Marshal(putReq)
	if err != nil {
		t.Fatal(err)
	}
	// Send a Put request but with non-canonical capitalization.
	httpReq, err := http.NewRequest("POST", "http://"+serverAddr+kv.DBPrefix+"pUt", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	httpReq.Header.Add("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("HTTP response status code != 200; got %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)

	putResp := &proto.PutResponse{}
	if err := gogoproto.Unmarshal(body, putResp); err != nil {
		t.Fatal(err)
	}
	if putResp.Error != nil {
		t.Errorf("error on put: %s", putResp.GoError())
	}
}

// TestKVDBInternalMethods verifies no internal methods are available
// HTTP DB interface.
func TestKVDBInternalMethods(t *testing.T) {
	once.Do(func() { startServer(t) })

	testCases := []struct {
		method string
		args   proto.Request
		reply  proto.Response
	}{
		{proto.InternalRangeLookup, &proto.InternalRangeLookupRequest{}, &proto.InternalRangeLookupResponse{}},
		{proto.InternalEndTxn, &proto.InternalEndTxnRequest{}, &proto.InternalEndTxnResponse{}},
		{proto.InternalHeartbeatTxn, &proto.InternalHeartbeatTxnRequest{}, &proto.InternalHeartbeatTxnResponse{}},
		{proto.InternalPushTxn, &proto.InternalPushTxnRequest{}, &proto.InternalPushTxnResponse{}},
		{proto.InternalResolveIntent, &proto.InternalResolveIntentRequest{}, &proto.InternalResolveIntentResponse{}},
		{proto.InternalSnapshotCopy, &proto.InternalSnapshotCopyRequest{}, &proto.InternalSnapshotCopyResponse{}},
	}
	// Verify non-public methods experience bad request errors.
	kvClient := client.NewKV(serverAddr, rpc.LoadInsecureTLSConfig().Config())
	for i, test := range testCases {
		test.args.Header().Key = proto.Key("a")
		err := kvClient.Call(test.method, test.args, test.reply)
		if err == nil {
			t.Errorf("%d: unexpected success calling %s", i, test.method)
		} else if err.Error() != "404 Not Found" {
			t.Errorf("%d: expected 404; got %s", i, err)
		}
	}
}

// TestKVDBContentTypes verifies all combinations of request /
// response content encodings are supported.
func TestKVDBContentType(t *testing.T) {
	once.Do(func() { startServer(t) })

	putReq := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key("a"),
		},
		Value: proto.Value{Bytes: []byte("value")},
	}

	testCases := []struct {
		cType, accept string
	}{
		{"application/json", "application/json"},
		{"application/x-protobuf", "application/json"},
		{"text/yaml", "application/json"},
		{"application/json", "application/x-protobuf"},
		{"application/x-protobuf", "application/x-protobuf"},
		{"text/yaml", "application/x-protobuf"},
		{"application/json", "text/yaml"},
		{"application/x-protobuf", "text/yaml"},
		{"text/yaml", "text/yaml"},
	}
	for i, test := range testCases {
		var body []byte
		var err error
		switch test.cType {
		case "application/json":
			body, err = json.Marshal(putReq)
		case "application/x-protobuf":
			body, err = gogoproto.Marshal(putReq)
		case "text/yaml":
			body, err = yaml.Marshal(putReq)
		}
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		// Send a Put request but with non-canonical capitalization.
		httpReq, err := http.NewRequest("POST", "http://"+serverAddr+kv.DBPrefix+"Put", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		httpReq.Header.Add("Content-Type", test.cType)
		httpReq.Header.Add("Accept", test.accept)
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("%d: HTTP response status code != 200; got %d", i, resp.StatusCode)
		}
		if cType := resp.Header.Get("Content-Type"); cType != test.accept {
			t.Errorf("%d: expected content type %s; got %s", i, test.accept, cType)
		}
	}
}

// TestKVDBTransaction verifies that transactions work properly over
// the KV DB endpoint.
func TestKVDBTransaction(t *testing.T) {
	once.Do(func() { startServer(t) })
	kvClient := client.NewKV(serverAddr, rpc.LoadInsecureTLSConfig().Config())

	key := proto.Key("db-txn-test")
	value := []byte("value")
	// Use snapshot isolation so non-transactional read can always push.
	txnOpts := &client.TransactionOptions{
		Name:         "test",
		User:         storage.UserRoot,
		UserPriority: 1,
		Isolation:    proto.SNAPSHOT,
	}
	err := kvClient.RunTransaction(txnOpts, func(txn *client.KV) error {
		pr := &proto.PutResponse{}
		if err := txn.Call(proto.Put, proto.PutArgs(key, value), pr); err != nil {
			t.Fatal(err)
		}

		// Attempt to read outside of txn.
		gr := &proto.GetResponse{}
		if err := kvClient.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
			t.Fatal(err)
		}
		if gr.Value != nil {
			t.Errorf("expected nil value; got %+v", gr.Value)
		}

		// Read within the transaction.
		if err := txn.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
			t.Fatal(err)
		}
		if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
			t.Errorf("expected value %q; got %q", value, gr.Value.Bytes)
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected success on commit; got %s", err)
	}

	// Verify the value is now visible on commit == true, and not visible otherwise.
	gr := &proto.GetResponse{}
	if err = kvClient.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
		t.Errorf("expected success reading value; got %s", err)
	}
	if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
		t.Errorf("expected value %q; got %q", value, gr.Value.Bytes)
	}
}
