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
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
	yaml "gopkg.in/yaml.v1"
)

func createTestClient(t *testing.T, addr string) *client.DB {
	db, err := client.Open("https://root@" + addr + "?certs=" + security.EmbeddedCertsDir)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestKVDBCoverage verifies that all methods may be invoked on the
// key value database.
func TestKVDBCoverage(t *testing.T) {
	s := startServer(t)
	defer s.Stop()

	db := createTestClient(t, s.ServingAddr())
	key := proto.Key("a")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Put first value at key.
	if err := db.Put(key, value1); err != nil {
		t.Fatal(err)
	}

	// Verify put.
	if gr, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Error("expected key to exist after delete")
	}

	// Conditional put should succeed, changing value1 to value2.
	if err := db.CPut(key, value2, value1); err != nil {
		t.Fatal(err)
	}

	// Verify get by looking up conditional put value.
	if gr, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(gr.ValueBytes(), value2) {
		t.Errorf("expected get to return %q; got %q", value2, gr.ValueBytes())
	}

	// Increment.
	if ir, err := db.Inc("i", 10); err != nil {
		t.Fatal(err)
	} else if ir.ValueInt() != 10 {
		t.Errorf("expected increment new value of %d; got %d", 10, ir.ValueInt())
	}

	// Delete conditional put value.
	if err := db.Del(key); err != nil {
		t.Fatal(err)
	}
	if gr, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Error("expected key to not exist after delete")
	}

	// Put values in anticipation of scan & delete range.
	keyValues := []proto.KeyValue{
		{Key: proto.Key("a"), Value: proto.Value{Bytes: value1}},
		{Key: proto.Key("b"), Value: proto.Value{Bytes: value2}},
		{Key: proto.Key("c"), Value: proto.Value{Bytes: value3}},
	}
	for _, kv := range keyValues {
		if err := db.Put(kv.Key, kv.Value.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	if rows, err := db.Scan("a", "d", 0); err != nil {
		t.Fatal(err)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			if !bytes.Equal(rows[i].ValueBytes(), kv.Value.Bytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[i].ValueBytes(), kv.Value.Bytes)
			}
		}
	}

	if err := db.DelRange("a", "c"); err != nil {
		t.Fatal(err)
	}
}

// TestKVDBInternalMethods verifies no internal methods are available
// HTTP DB interface.
func TestKVDBInternalMethods(t *testing.T) {
	s := startServer(t)
	defer s.Stop()

	testCases := []struct {
		args  proto.Request
		reply proto.Response
	}{
		{&proto.InternalRangeLookupRequest{}, &proto.InternalRangeLookupResponse{}},
		{&proto.InternalGCRequest{}, &proto.InternalGCResponse{}},
		{&proto.InternalHeartbeatTxnRequest{}, &proto.InternalHeartbeatTxnResponse{}},
		{&proto.InternalPushTxnRequest{}, &proto.InternalPushTxnResponse{}},
		{&proto.InternalResolveIntentRequest{}, &proto.InternalResolveIntentResponse{}},
		{&proto.InternalResolveIntentRangeRequest{}, &proto.InternalResolveIntentRangeResponse{}},
		{&proto.InternalMergeRequest{}, &proto.InternalMergeResponse{}},
		{&proto.InternalTruncateLogRequest{}, &proto.InternalTruncateLogResponse{}},
	}
	// Verify non-public methods experience bad request errors.
	db := createTestClient(t, s.ServingAddr())
	for i, test := range testCases {
		test.args.Header().Key = proto.Key("a")
		if proto.IsRange(test.args) {
			test.args.Header().EndKey = test.args.Header().Key.Next()
		}
		b := &client.Batch{}
		b.InternalAddCall(client.Call{Args: test.args, Reply: test.reply})
		err := db.Run(b)
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
	s := startServer(t)
	defer s.Stop()

	db := createTestClient(t, s.ServingAddr())
	err := db.Tx(func(tx *client.Tx) error {
		// Make an EndTransaction request which would fail if not
		// stripped. In this case, we set the start key to "bar" for a
		// split of the default range; start key must be "" in this case.
		b := &client.Batch{}
		b.InternalAddCall(client.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: proto.Key("foo")},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					SplitTrigger: &proto.SplitTrigger{
						UpdatedDesc: proto.RangeDescriptor{StartKey: proto.Key("bar")},
					},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return tx.Run(b)
	})
	if err == nil {
		t.Errorf("expected 400 bad request error on commit")
	}
}

// TestKVDBContentTypes verifies all combinations of request /
// response content encodings are supported.
func TestKVDBContentType(t *testing.T) {
	s := startServer(t)
	defer s.Stop()

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
		httpReq, err := http.NewRequest("POST", testContext.RequestScheme()+"://"+s.ServingAddr()+kv.DBPrefix+"Put",
			bytes.NewReader(body))
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		httpReq.Header.Add(util.ContentTypeHeader, test.cType)
		if test.accept != "" {
			httpReq.Header.Add(util.AcceptHeader, test.accept)
		}
		resp, err := httpDoReq(testContext, httpReq)
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
	s := startServer(t)
	defer s.Stop()

	db := createTestClient(t, s.ServingAddr())

	key := proto.Key("db-txn-test")
	value := []byte("value")
	err := db.Tx(func(tx *client.Tx) error {
		// Use snapshot isolation so non-transactional read can always push.
		tx.SetSnapshotIsolation()

		if err := tx.Put(key, value); err != nil {
			t.Fatal(err)
		}

		// Attempt to read outside of txn.
		if gr, err := db.Get(key); err != nil {
			t.Fatal(err)
		} else if gr.Exists() {
			t.Errorf("expected nil value; got %+v", gr.Value)
		}

		// Read within the transaction.
		if gr, err := tx.Get(key); err != nil {
			t.Fatal(err)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
			t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected success on commit; got %s", err)
	}

	// Verify the value is now visible after commit.
	if gr, err := db.Get(key); err != nil {
		t.Errorf("expected success reading value; got %s", err)
	} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
		t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
	}
}
