// Copyright 2015 The Cockroach Authors.
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
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// NOTE: these tests are in package kv_test to avoid a circular
// dependency between the server and kv packages. These tests rely on
// starting a TestServer, which creates a "real" node and employs a
// distributed sender server-side.

// TestRangeLookupWithOpenTransaction verifies that range lookups are
// done in such a way (e.g. using inconsistent reads) that they
// proceed in the event that a write intent is extant at the meta
// index record being read.
func TestRangeLookupWithOpenTransaction(t *testing.T) {
	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	sender := client.NewHTTPSender(s.HTTPAddr, &http.Transport{
		TLSClientConfig: rpc.LoadInsecureTLSConfig().Config(),
	})
	db := client.NewKV(sender, nil)
	db.User = storage.UserRoot

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax)
	now := s.Clock().Now()
	txn := proto.NewTransaction("txn", proto.Key("foobar"), 0, proto.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(s.Engine, nil, key, now, txn, &proto.RangeDescriptor{}); err != nil {
		t.Fatal(err)
	}

	// Now, with an intent pending, attempt (asynchronously) to read
	// from an arbitrary key. This will cause the distributed sender to
	// do a range lookup, which will encounter the intent. We're
	// verifying here that the range lookup doesn't fail with a write
	// intent error. If it did, it would go into a deadloop attempting
	// to push the transaction, which in turn requires another range
	// lookup, etc, ad nauseam.
	success := make(chan struct{})
	go func() {
		if err := db.Call(proto.Get, proto.GetArgs(proto.Key("a")), &proto.GetResponse{}); err != nil {
			t.Fatal(err)
		}
		close(success)
	}()

	select {
	case <-success:
		// Hurrah!
	case <-time.After(1 * time.Second):
		t.Errorf("get request did not succeed in face of range metadata intent")
	}
}
