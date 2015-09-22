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
// Author: Andrew Bonventre (andybons@gmail.com)

package server

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	gogoproto "github.com/gogo/protobuf/proto"
)

var testContext = NewTestContext()
var nodeTestBaseContext = testutils.NewNodeTestBaseContext()

// TestInitEngine tests whether the data directory string is parsed correctly.
func TestInitEngine(t *testing.T) {
	defer leaktest.AfterTest(t)
	tmp := util.CreateNTempDirs(t, "_server_test", 5)
	defer util.CleanupDirs(tmp)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	testCases := []struct {
		key       string           // data directory
		expAttrs  proto.Attributes // attributes for engine
		wantError bool             // do we expect an error from this key?
		isMem     bool             // is the engine in-memory?
	}{
		{"mem=1000", proto.Attributes{Attrs: []string{"mem"}}, false, true},
		{"ssd=1000", proto.Attributes{Attrs: []string{"ssd"}}, false, true},
		{fmt.Sprintf("ssd=%s", tmp[0]), proto.Attributes{Attrs: []string{"ssd"}}, false, false},
		{fmt.Sprintf("hdd=%s", tmp[1]), proto.Attributes{Attrs: []string{"hdd"}}, false, false},
		{fmt.Sprintf("mem=%s", tmp[2]), proto.Attributes{Attrs: []string{"mem"}}, false, false},
		{fmt.Sprintf("abc=%s", tmp[3]), proto.Attributes{Attrs: []string{"abc"}}, false, false},
		{fmt.Sprintf("hdd:7200rpm=%s", tmp[4]), proto.Attributes{Attrs: []string{"hdd", "7200rpm"}}, false, false},
		{"", proto.Attributes{}, true, false},
		{"  ", proto.Attributes{}, true, false},
		{"arbitrarystring", proto.Attributes{}, true, false},
		{"mem=", proto.Attributes{}, true, false},
		{"ssd=", proto.Attributes{}, true, false},
		{"hdd=", proto.Attributes{}, true, false},
	}
	for _, spec := range testCases {
		ctx := NewContext()
		ctx.Stores, ctx.GossipBootstrap = spec.key, SelfGossipAddr
		if err := ctx.InitStores(stopper); err == nil {
			engines := ctx.Engines
			if spec.wantError {
				t.Fatalf("invalid engine spec '%v' erroneously accepted: %+v", spec.key, spec)
			}
			if len(engines) != 1 {
				t.Fatalf("unexpected number of engines: %d: %+v", len(engines), spec)
			}
			e := engines[0]
			if e.Attrs().SortedString() != spec.expAttrs.SortedString() {
				t.Errorf("wrong engine attributes, expected %v but got %v: %+v", spec.expAttrs, e.Attrs(), spec)
			}
			_, ok := e.(engine.InMem)
			if spec.isMem != ok {
				t.Errorf("expected in memory? %t, got %t: %+v", spec.isMem, ok, spec)
			}
		} else if !spec.wantError {
			t.Errorf("expected no error, got %v: %+v", err, spec)
		}
	}
}

// TestInitEngines tests whether multiple engines specified as a
// single comma-separated list are parsed correctly.
func TestInitEngines(t *testing.T) {
	defer leaktest.AfterTest(t)
	tmp := util.CreateNTempDirs(t, "_server_test", 2)
	defer util.CleanupDirs(tmp)

	ctx := NewContext()
	ctx.Stores = fmt.Sprintf("mem=1000,mem:ddr3=1000,ssd=%s,hdd:7200rpm=%s", tmp[0], tmp[1])
	ctx.GossipBootstrap = SelfGossipAddr
	expEngines := []struct {
		attrs proto.Attributes
		isMem bool
	}{
		{proto.Attributes{Attrs: []string{"mem"}}, true},
		{proto.Attributes{Attrs: []string{"mem", "ddr3"}}, true},
		{proto.Attributes{Attrs: []string{"ssd"}}, false},
		{proto.Attributes{Attrs: []string{"hdd", "7200rpm"}}, false},
	}

	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := ctx.InitStores(stopper); err != nil {
		t.Fatal(err)
	}

	engines := ctx.Engines
	if len(engines) != len(expEngines) {
		t.Errorf("number of engines parsed %d != expected %d", len(engines), len(expEngines))
	}
	for i, e := range engines {
		if e.Attrs().SortedString() != expEngines[i].attrs.SortedString() {
			t.Errorf("wrong engine attributes, expected %v but got %v: %+v", expEngines[i].attrs, e.Attrs(), expEngines[i])
		}
		_, ok := e.(engine.InMem)
		if expEngines[i].isMem != ok {
			t.Errorf("expected in memory? %t, got %t: %+v", expEngines[i].isMem, ok, expEngines[i])
		}
	}
}

// TestSelfBootstrap verifies operation when no bootstrap hosts have
// been specified.
func TestSelfBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	s.Stop()
}

// TestHealth verifies that health endpoint return "ok".
func TestHealth(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()
	url := testContext.HTTPRequestScheme() + "://" + s.ServingAddr() + healthPath
	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("error requesting health at %s: %s", url, err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not read response body: %s", err)
	}
	expected := "ok"
	if !strings.Contains(string(b), expected) {
		t.Errorf("expected body to contain %q, got %q", expected, string(b))
	}
}

// TestPlainHTTPServer verifies that we can serve plain http and talk to it.
// This is controlled by -cert=""
func TestPlainHTTPServer(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Create a custom context. The default one has a default --certs value.
	ctx := NewContext()
	ctx.Addr = "127.0.0.1:0"
	ctx.Insecure = true
	// TestServer.Start does not override the context if set.
	s := &TestServer{Ctx: ctx}
	if err := s.Start(); err != nil {
		t.Fatalf("could not start plain http server: %v", err)
	}
	defer s.Stop()

	// Get a plain http client using the same context.
	if ctx.HTTPRequestScheme() != "http" {
		t.Fatalf("expected context.HTTPRequestScheme == \"http\", got: %s", ctx.HTTPRequestScheme())
	}
	url := ctx.HTTPRequestScheme() + "://" + s.ServingAddr() + healthPath
	httpClient, err := ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("error requesting health at %s: %s", url, err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not read response body: %s", err)
	}
	expected := "ok"
	if !strings.Contains(string(b), expected) {
		t.Errorf("expected body to contain %q, got %q", expected, string(b))
	}

	// Try again with a https client (testContext is one)
	if testContext.HTTPRequestScheme() != "https" {
		t.Fatalf("expected context.HTTPRequestScheme == \"http\", got: %s", testContext.HTTPRequestScheme())
	}
	url = testContext.HTTPRequestScheme() + "://" + s.ServingAddr() + healthPath
	httpClient, err = ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err = httpClient.Get(url)
	if err == nil {
		t.Fatalf("unexpected success fetching %s", url)
	}
}

// TestAcceptEncoding hits the health endpoint while explicitly
// disabling decompression on a custom client's Transport and setting
// it conditionally via the request's Accept-Encoding headers.
func TestAcceptEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()
	client, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		acceptEncoding string
		newReader      func(io.Reader) io.Reader
	}{
		{"",
			func(b io.Reader) io.Reader {
				return b
			},
		},
		{util.GzipEncoding,
			func(b io.Reader) io.Reader {
				r, err := gzip.NewReader(b)
				if err != nil {
					t.Fatalf("could not create new gzip reader: %s", err)
				}
				return r
			},
		},
		{util.SnappyEncoding,
			func(b io.Reader) io.Reader {
				return snappy.NewReader(b)
			},
		},
	}
	for _, d := range testData {
		req, err := http.NewRequest("GET", testContext.HTTPRequestScheme()+"://"+s.ServingAddr()+healthPath, nil)
		if err != nil {
			t.Fatalf("could not create request: %s", err)
		}
		if d.acceptEncoding != "" {
			req.Header.Set(util.AcceptEncodingHeader, d.acceptEncoding)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("could not make request to %s: %s", req.URL, err)
		}
		defer resp.Body.Close()
		if ce := resp.Header.Get(util.ContentEncodingHeader); ce != d.acceptEncoding {
			t.Fatalf("unexpected content encoding: '%s' != '%s'", ce, d.acceptEncoding)
		}
		r := d.newReader(resp.Body)
		b, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatalf("could not read '%s' response body: %s", d.acceptEncoding, err)
		}
		expected := "ok"
		if !strings.Contains(string(b), expected) {
			t.Errorf("expected body to contain %q, got %q", expected, b)
		}
	}
}

// TestMultiRangeScanDeleteRange tests that commands which access multiple
// ranges are carried out properly.
func TestMultiRangeScanDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()
	ds := kv.NewDistSender(&kv.DistSenderContext{Clock: s.Clock()}, s.Gossip())
	tds := kv.NewTxnCoordSender(ds, s.Clock(), testContext.Linearizable, nil, s.stopper)

	if err := s.node.ctx.DB.AdminSplit("m"); err != nil {
		t.Fatal(err)
	}
	writes := []proto.Key{proto.Key("a"), proto.Key("z")}
	get := proto.Call{
		Args: &proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key: writes[0],
			},
		},
		Reply: &proto.GetResponse{},
	}
	get.Args.Header().EndKey = writes[len(writes)-1]
	tds.Send(context.Background(), get)
	if err := get.Reply.Header().GoError(); err == nil {
		t.Errorf("able to call Get with a key range: %v", get)
	}
	var call proto.Call
	for i, k := range writes {
		call = proto.PutCall(k, proto.Value{Bytes: k})
		tds.Send(context.Background(), call)
		if err := call.Reply.Header().GoError(); err != nil {
			t.Fatal(err)
		}
		scan := proto.ScanCall(writes[0], writes[len(writes)-1].Next(), 0)
		// The Put ts may have been pushed by tsCache,
		// so make sure we see their values in our Scan.
		scan.Args.Header().Timestamp = call.Reply.Header().Timestamp
		tds.Send(context.Background(), scan)
		if err := scan.Reply.Header().GoError(); err != nil {
			t.Fatal(err)
		}
		if scan.Reply.Header().Txn != nil {
			// This was the other way around at some point in the past.
			// Same below for Delete, etc.
			t.Errorf("expected no transaction in response header")
		}
		if rows := scan.Reply.(*proto.ScanResponse).Rows; len(rows) != i+1 {
			t.Fatalf("expected %d rows, but got %d", i+1, len(rows))
		}
	}

	del := proto.Call{
		Args: &proto.DeleteRangeRequest{
			RequestHeader: proto.RequestHeader{
				Key:       writes[0],
				EndKey:    proto.Key(writes[len(writes)-1]).Next(),
				Timestamp: call.Reply.Header().Timestamp,
			},
		},
		Reply: &proto.DeleteRangeResponse{},
	}
	tds.Send(context.Background(), del)
	if err := del.Reply.Header().GoError(); err != nil {
		t.Fatal(err)
	}
	if del.Reply.Header().Txn != nil {
		t.Errorf("expected no transaction in response header")
	}
	if n := del.Reply.(*proto.DeleteRangeResponse).NumDeleted; n != int64(len(writes)) {
		t.Errorf("expected %d keys to be deleted, but got %d instead",
			len(writes), n)
	}

	scan := proto.ScanCall(writes[0], writes[len(writes)-1].Next(), 0)
	scan.Args.Header().Timestamp = del.Reply.Header().Timestamp
	scan.Args.Header().Txn = &proto.Transaction{Name: "MyTxn"}
	tds.Send(context.Background(), scan)
	if err := scan.Reply.Header().GoError(); err != nil {
		t.Fatal(err)
	}
	if txn := scan.Reply.Header().Txn; txn == nil || txn.Name != "MyTxn" {
		t.Errorf("wanted Txn to persist, but it changed to %v", txn)
	}
	if rows := scan.Reply.(*proto.ScanResponse).Rows; len(rows) > 0 {
		t.Fatalf("scan after delete returned rows: %v", rows)
	}
}

// TestMultiRangeScanWithMaxResults tests that commands which access multiple
// ranges with MaxResults parameter are carried out properly.
func TestMultiRangeScanWithMaxResults(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		splitKeys []proto.Key
		keys      []proto.Key
	}{
		{[]proto.Key{proto.Key("m")},
			[]proto.Key{proto.Key("a"), proto.Key("z")}},
		{[]proto.Key{proto.Key("h"), proto.Key("q")},
			[]proto.Key{proto.Key("b"), proto.Key("f"), proto.Key("k"),
				proto.Key("r"), proto.Key("w"), proto.Key("y")}},
	}

	for i, tc := range testCases {
		s := StartTestServer(t)
		ds := kv.NewDistSender(&kv.DistSenderContext{Clock: s.Clock()}, s.Gossip())
		tds := kv.NewTxnCoordSender(ds, s.Clock(), testContext.Linearizable, nil, s.stopper)

		for _, sk := range tc.splitKeys {
			if err := s.node.ctx.DB.AdminSplit(sk); err != nil {
				t.Fatal(err)
			}
		}

		var call proto.Call
		for _, k := range tc.keys {
			call = proto.PutCall(k, proto.Value{Bytes: k})
			tds.Send(context.Background(), call)
			if err := call.Reply.Header().GoError(); err != nil {
				t.Fatal(err)
			}
		}

		// Try every possible ScanRequest startKey.
		for start := 0; start < len(tc.keys); start++ {
			// Try every possible maxResults, from 1 to beyond the size of key array.
			for maxResults := 1; maxResults <= len(tc.keys)-start+1; maxResults++ {
				scan := proto.ScanCall(tc.keys[start], tc.keys[len(tc.keys)-1].Next(),
					int64(maxResults))
				scan.Args.Header().Timestamp = call.Reply.Header().Timestamp
				tds.Send(context.Background(), scan)
				if err := scan.Reply.Header().GoError(); err != nil {
					t.Fatal(err)
				}
				rows := scan.Reply.(*proto.ScanResponse).Rows
				if start+maxResults <= len(tc.keys) && len(rows) != maxResults {
					t.Errorf("%d: start=%s: expected %d rows, but got %d", i, tc.keys[start], maxResults, len(rows))
				} else if start+maxResults == len(tc.keys)+1 && len(rows) != maxResults-1 {
					t.Errorf("%d: expected %d rows, but got %d", i, maxResults-1, len(rows))
				}
			}
		}
		defer s.Stop()
	}
}

func TestSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

	// sendURL sends a request to the server and returns a StatusCode
	sendURL := func(t *testing.T, command string, body []byte) int {
		url := fmt.Sprintf("%s://%s%s%s?certs=test_certs",
			testContext.HTTPRequestScheme(),
			s.ServingAddr(),
			driver.Endpoint,
			command)
		httpClient, err := testContext.GetHTTPClient()
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptEncodingHeader, util.SnappyEncoding)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	// Use the sql administrator (root user).
	body, err := gogoproto.Marshal(&driver.Request{User: security.RootUser})
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		command       string
		body          []byte
		expStatusCode int
	}{
		// Bad command.
		{"Execu", []byte(""), http.StatusNotFound},
		// Request with garbage payload.
		{"Execute", []byte("garbage"), http.StatusBadRequest},
		// Valid request.
		{"Execute", body, http.StatusOK},
	}
	for tcNum, test := range testCases {
		statusCode := sendURL(t, test.command, test.body)
		if statusCode != test.expStatusCode {
			t.Fatalf("#%d: Expected status: %d, received status %d", tcNum, test.expStatusCode, statusCode)
		}
	}
}

func TestSystemDBGossip(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := StartTestServer(t)
	defer s.Stop()

	resultChan := make(chan ([]byte))
	var count int32
	db := s.db
	key := sql.MakeDescMetadataKey(keys.MaxReservedDescID)
	valAt := func(i int) *sql.DatabaseDescriptor {
		return &sql.DatabaseDescriptor{Name: "foo", ID: sql.ID(i)}
	}

	// Register a callback for gossip updates.
	s.Gossip().RegisterCallback(gossip.KeySystemConfig, func(_ string, content []byte) {
		newCount := atomic.AddInt32(&count, 1)
		if newCount != 2 {
			// RegisterCallback calls us right away with the contents,
			// so ignore the very first call.
			// We also only want the first value of all our writes.
			return
		}
		resultChan <- content
	})

	// The span only gets gossiped when it first shows up, or when
	// the EndTransaction trigger is set.

	// Try a plain KV write first.
	if err := db.Put(key, valAt(0)); err != nil {
		t.Fatal(err)
	}

	// Now do it as part of a transaction, but without the trigger set.
	if err := db.Txn(func(txn *client.Txn) error {
		return txn.Put(key, valAt(1))
	}); err != nil {
		t.Fatal(err)
	}

	// This time mark the transaction as having a SystemDB trigger.
	if err := db.Txn(func(txn *client.Txn) error {
		txn.SetSystemDBTrigger()
		return txn.Put(key, valAt(2))
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for the callback.
	var b []byte
	select {
	case b = <-resultChan:
		break
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive gossip callback")
	}

	// Now check the gossip callback.
	var val *proto.Value
	systemConfig := &config.SystemConfig{}
	if err := gogoproto.Unmarshal(b, systemConfig); err != nil {
		t.Fatal(err)
	}

	for _, kv := range systemConfig.GetValues() {
		if bytes.Equal(key, kv.GetKey()) {
			val = &kv.Value
		}
	}
	if val == nil {
		t.Fatal("key not found in gossiped info")
	}

	// Make sure the returned value is valAt(2).
	var got sql.DatabaseDescriptor
	if err := gogoproto.Unmarshal(val.Bytes, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 2 {
		t.Fatalf("mismatch: expected %+v, got %+v", valAt(2), got)
	}
}
