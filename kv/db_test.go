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
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
	yaml "gopkg.in/yaml.v1"
)

func createTestClient(addr string) *client.KV {
	transport := &http.Transport{TLSClientConfig: rpc.LoadInsecureTLSConfig().Config()}
	return client.NewKV(nil, client.NewHTTPSender(addr, transport))
}

// TestKVDBCoverage verifies that all methods may be invoked on the
// key value database.
func TestKVDBCoverage(t *testing.T) {
	addr, _, stopper := startServer(t)
	defer stopper.Stop()

	kvClient := createTestClient(addr)
	key := proto.Key("a")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Put first value at key.
	putReq := &proto.PutRequest{Value: proto.Value{Bytes: value1}}
	putReq.Key = key
	putResp := &proto.PutResponse{}
	if err := kvClient.Call(putReq, putResp); err != nil || putResp.Error != nil {
		t.Fatalf("%s, %s", err, putResp.GoError())
	}

	// Verify put with Contains.
	containsReq := &proto.ContainsRequest{}
	containsReq.Key = key
	containsResp := &proto.ContainsResponse{}
	if err := kvClient.Call(containsReq, containsResp); err != nil || containsResp.Error != nil {
		t.Fatalf("%s, %s", err, containsResp.GoError())
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
	if err := kvClient.Call(cPutReq, cPutResp); err != nil || cPutResp.Error != nil {
		t.Fatalf("%s, %s", err, cPutResp.GoError())
	}

	// Verify get by looking up conditional put value.
	getReq := &proto.GetRequest{}
	getReq.Key = key
	getResp := &proto.GetResponse{}
	if err := kvClient.Call(getReq, getResp); err != nil || getResp.Error != nil {
		t.Fatalf("%s, %s", err, getResp.GoError())
	}
	if !bytes.Equal(getResp.Value.Bytes, value2) {
		t.Errorf("expected get to return %q; got %q", value2, getResp.Value.Bytes)
	}

	// Increment.
	incrReq := &proto.IncrementRequest{Increment: 10}
	incrReq.Key = proto.Key("i")
	incrResp := &proto.IncrementResponse{}
	if err := kvClient.Call(incrReq, incrResp); err != nil || incrResp.Error != nil {
		t.Fatalf("%s, %s", err, incrResp.GoError())
	}
	if incrResp.NewValue != incrReq.Increment {
		t.Errorf("expected increment new value of %d; got %d", incrReq.Increment, incrResp.NewValue)
	}

	// Delete conditional put value.
	delReq := &proto.DeleteRequest{}
	delReq.Key = key
	delResp := &proto.DeleteResponse{}
	if err := kvClient.Call(delReq, delResp); err != nil || delResp.Error != nil {
		t.Fatalf("%s, %s", err, delResp.GoError())
	}
	if err := kvClient.Call(containsReq, containsResp); err != nil || containsResp.Error != nil {
		t.Fatalf("%s, %s", err, containsResp.GoError())
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
		if err := kvClient.Call(putReq, putResp); err != nil || putResp.Error != nil {
			t.Fatalf("%s, %s", err, putResp.GoError())
		}
	}
	scanReq := &proto.ScanRequest{}
	scanReq.Key = proto.Key("a")
	scanReq.EndKey = proto.Key("c").Next()
	scanResp := &proto.ScanResponse{}
	if err := kvClient.Call(scanReq, scanResp); err != nil || scanResp.Error != nil {
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
	if err := kvClient.Call(deleteRangeReq, deleteRangeResp); err != nil || deleteRangeResp.Error != nil {
		t.Fatalf("%s, %s", err, deleteRangeResp.GoError())
	}
	if deleteRangeResp.NumDeleted != int64(len(keyValues)) {
		t.Fatalf("expected %d rows in deleterange; got %d", len(keyValues), deleteRangeResp.NumDeleted)
	}
}

// TestKVDBInternalMethods verifies no internal methods are available
// HTTP DB interface.
func TestKVDBInternalMethods(t *testing.T) {
	addr, _, stopper := startServer(t)
	defer stopper.Stop()

	testCases := []struct {
		args  proto.Request
		reply proto.Response
	}{
		{&proto.InternalRangeLookupRequest{}, &proto.InternalRangeLookupResponse{}},
		{&proto.InternalGCRequest{}, &proto.InternalGCResponse{}},
		{&proto.InternalHeartbeatTxnRequest{}, &proto.InternalHeartbeatTxnResponse{}},
		{&proto.InternalPushTxnRequest{}, &proto.InternalPushTxnResponse{}},
		{&proto.InternalResolveIntentRequest{}, &proto.InternalResolveIntentResponse{}},
		{&proto.InternalMergeRequest{}, &proto.InternalMergeResponse{}},
		{&proto.InternalTruncateLogRequest{}, &proto.InternalTruncateLogResponse{}},
	}
	// Verify non-public methods experience bad request errors.
	kvClient := createTestClient(addr)
	for i, test := range testCases {
		test.args.Header().Key = proto.Key("a")
		err := kvClient.Call(test.args, test.reply)
		if err == nil {
			t.Errorf("%d: unexpected success calling %s", i, test.args.Method())
		} else if err.Error() != "404 Not Found" {
			t.Errorf("%d: expected 404; got %s", i, err)
		}
	}
}

// TestKVDBEndTransactionWithTriggers verifies that triggers are
// disallowed on call to EndTransaction.
func TestKVDBEndTransactionWithTriggers(t *testing.T) {
	addr, _, stopper := startServer(t)
	defer stopper.Stop()

	kvClient := createTestClient(addr)
	txnOpts := &client.TransactionOptions{Name: "test"}
	err := kvClient.RunTransaction(txnOpts, func(txn *client.KV) error {
		// Make an EndTransaction request which would fail if not
		// stripped. In this case, we set the start key to "bar" for a
		// split of the default range; start key must be "" in this case.
		return txn.Call(&proto.EndTransactionRequest{
			RequestHeader: proto.RequestHeader{Key: proto.Key("foo")},
			Commit:        true,
			InternalCommitTrigger: &proto.InternalCommitTrigger{
				SplitTrigger: &proto.SplitTrigger{
					UpdatedDesc: proto.RangeDescriptor{StartKey: proto.Key("bar")},
				},
			},
		}, &proto.EndTransactionResponse{})
	})
	if err == nil {
		t.Errorf("expected 400 bad request error on commit")
	}
}

// TestKVDBContentTypes verifies all combinations of request /
// response content encodings are supported.
func TestKVDBContentType(t *testing.T) {
	addr, _, stopper := startServer(t)
	defer stopper.Stop()

	putReq := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key("a"),
		},
		Value: proto.Value{Bytes: []byte("value")},
	}

	testCases := []struct {
		cType, accept, expCType string
		expErr                  bool
	}{
		{util.JSONContentType, util.JSONContentType, util.JSONContentType, false},
		{util.ProtoContentType, util.JSONContentType, util.JSONContentType, false},
		{util.YAMLContentType, util.JSONContentType, "", true},
		{util.JSONContentType, util.ProtoContentType, util.ProtoContentType, false},
		{util.ProtoContentType, util.ProtoContentType, util.ProtoContentType, false},
		{util.YAMLContentType, util.ProtoContentType, "", true},
		{util.JSONContentType, util.YAMLContentType, util.JSONContentType, false},
		{util.ProtoContentType, util.YAMLContentType, util.ProtoContentType, false},
		{util.YAMLContentType, util.YAMLContentType, "", true},
		{util.JSONContentType, "", util.JSONContentType, false},
		{util.ProtoContentType, "", util.ProtoContentType, false},
		{util.YAMLContentType, "", "", true},
	}
	for i, test := range testCases {
		var body []byte
		var err error
		switch test.cType {
		case util.JSONContentType:
			body, err = json.Marshal(putReq)
		case util.ProtoContentType:
			body, err = gogoproto.Marshal(putReq)
		case util.YAMLContentType:
			body, err = yaml.Marshal(putReq)
		}
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		// Send a Put request but with non-canonical capitalization.
		httpReq, err := http.NewRequest("POST", "http://"+addr+kv.DBPrefix+"Put", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		httpReq.Header.Add(util.ContentTypeHeader, test.cType)
		if test.accept != "" {
			httpReq.Header.Add(util.AcceptHeader, test.accept)
		}
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		if !test.expErr && resp.StatusCode != 200 {
			t.Fatalf("%d: HTTP response status code != 200; got %d", i, resp.StatusCode)
		} else if test.expErr {
			if resp.StatusCode != 400 {
				t.Fatalf("%d: expected client request error; got %d", i, resp.StatusCode)
			}
			continue
		}
		if cType := resp.Header.Get(util.ContentTypeHeader); cType != test.expCType {
			t.Errorf("%d: expected content type %s; got %s", i, test.expCType, cType)
		}
	}
}

// TestKVDBTransaction verifies that transactions work properly over
// the KV DB endpoint.
func TestKVDBTransaction(t *testing.T) {
	addr, _, stopper := startServer(t)
	defer stopper.Stop()

	kvClient := createTestClient(addr)

	key := proto.Key("db-txn-test")
	value := []byte("value")
	// Use snapshot isolation so non-transactional read can always push.
	txnOpts := &client.TransactionOptions{
		Name:      "test",
		Isolation: proto.SNAPSHOT,
	}
	err := kvClient.RunTransaction(txnOpts, func(txn *client.KV) error {
		pr := &proto.PutResponse{}
		if err := txn.Call(proto.PutArgs(key, value), pr); err != nil {
			t.Fatal(err)
		}

		// Attempt to read outside of txn.
		gr := &proto.GetResponse{}
		if err := kvClient.Call(proto.GetArgs(key), gr); err != nil {
			t.Fatal(err)
		}
		if gr.Value != nil {
			t.Errorf("expected nil value; got %+v", gr.Value)
		}

		// Read within the transaction.
		if err := txn.Call(proto.GetArgs(key), gr); err != nil {
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

	// Verify the value is now visible after commit.
	gr := &proto.GetResponse{}
	if err = kvClient.Call(proto.GetArgs(key), gr); err != nil {
		t.Errorf("expected success reading value; got %s", err)
	}
	if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
		t.Errorf("expected value %q; got %q", value, gr.Value.Bytes)
	}
}
