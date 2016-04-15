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
// Author: Andrew Bonventre (andybons@gmail.com)

package server

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/tracing"
)

var nodeTestBaseContext = testutils.NewNodeTestBaseContext()

// TestSelfBootstrap verifies operation when no bootstrap hosts have
// been specified.
func TestSelfBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()
}

// TestHealth verifies that health endpoint return "ok".
func TestHealth(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()
	u := s.Ctx.HTTPRequestScheme() + "://" + s.HTTPAddr() + healthPath
	httpClient, err := s.Ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpClient.Get(u)
	if err != nil {
		t.Fatal(err)
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
	defer leaktest.AfterTest(t)()
	// Create a custom context. The default one uses embedded certs.
	ctx := NewContext()
	ctx.Addr = "127.0.0.1:0"
	ctx.HTTPAddr = "127.0.0.1:0"
	ctx.Insecure = true
	s := TestServer{Ctx: ctx}
	if err := s.Start(); err != nil {
		t.Fatalf("could not start plain http server: %v", err)
	}
	defer s.Stop()

	httpClient, err := ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	httpURL := "http://" + s.HTTPAddr() + healthPath
	if resp, err := httpClient.Get(httpURL); err != nil {
		t.Fatalf("error requesting health at %s: %s", httpURL, err)
	} else {
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("could not read response body: %s", err)
		}
		if expected := "ok"; !strings.Contains(string(b), expected) {
			t.Errorf("expected body to contain %q, got %q", expected, string(b))
		}
	}

	httpsURL := "https://" + s.HTTPAddr() + healthPath
	if _, err := httpClient.Get(httpsURL); err == nil {
		t.Fatalf("unexpected success fetching %s", httpsURL)
	}
}

func TestSecureHTTPRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	httpClient, err := s.Ctx.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	origURL := "http://" + s.HTTPAddr()
	expURL := url.URL{Scheme: "https", Host: s.HTTPAddr(), Path: "/"}

	if resp, err := httpClient.Get(origURL); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		// TODO(tamird): s/308/http.StatusPermanentRedirect/ when it exists.
		if resp.StatusCode != 308 {
			t.Errorf("expected status code %d; got %d", 308, resp.StatusCode)
		}
		if redirectURL, err := resp.Location(); err != nil {
			t.Error(err)
		} else if a, e := redirectURL.String(), expURL.String(); a != e {
			t.Errorf("expected location %s; got %s", e, a)
		}
	}

	if resp, err := httpClient.Post(origURL, "text/plain; charset=utf-8", nil); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		// TODO(tamird): s/308/http.StatusPermanentRedirect/ when it exists.
		if resp.StatusCode != 308 {
			t.Errorf("expected status code %d; got %d", 308, resp.StatusCode)
		}
		if redirectURL, err := resp.Location(); err != nil {
			t.Error(err)
		} else if a, e := redirectURL.String(), expURL.String(); a != e {
			t.Errorf("expected location %s; got %s", e, a)
		}
	}
}

// TestAcceptEncoding hits the health endpoint while explicitly
// disabling decompression on a custom client's Transport and setting
// it conditionally via the request's Accept-Encoding headers.
func TestAcceptEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()
	client, err := s.Ctx.GetHTTPClient()
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
		req, err := http.NewRequest("GET", s.Ctx.HTTPRequestScheme()+"://"+s.HTTPAddr()+healthPath, nil)
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
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()
	retryOpts := kv.GetDefaultDistSenderRetryOptions()
	retryOpts.Closer = s.stopper.ShouldDrain()
	ds := kv.NewDistSender(&kv.DistSenderContext{
		Clock:           s.Clock(),
		RPCContext:      s.RPCContext(),
		RPCRetryOptions: &retryOpts,
	}, s.Gossip())
	tds := kv.NewTxnCoordSender(ds, s.Clock(), s.Ctx.Linearizable, tracing.NewTracer(),
		s.stopper, kv.NewTxnMetrics(metric.NewRegistry()))

	if err := s.node.ctx.DB.AdminSplit("m"); err != nil {
		t.Fatal(err)
	}
	writes := []roachpb.Key{roachpb.Key("a"), roachpb.Key("z")}
	get := &roachpb.GetRequest{
		Span: roachpb.Span{Key: writes[0]},
	}
	get.EndKey = writes[len(writes)-1]
	if _, err := client.SendWrapped(tds, nil, get); err == nil {
		t.Errorf("able to call Get with a key range: %v", get)
	}
	var delTS roachpb.Timestamp
	for i, k := range writes {
		put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
		reply, err := client.SendWrapped(tds, nil, put)
		if err != nil {
			t.Fatal(err)
		}
		scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next(), 0)
		reply, err = client.SendWrapped(tds, nil, scan)
		if err != nil {
			t.Fatal(err)
		}
		sr := reply.(*roachpb.ScanResponse)
		if sr.Txn != nil {
			// This was the other way around at some point in the past.
			// Same below for Delete, etc.
			t.Errorf("expected no transaction in response header")
		}
		if rows := sr.Rows; len(rows) != i+1 {
			t.Fatalf("expected %d rows, but got %d", i+1, len(rows))
		}
	}

	del := &roachpb.DeleteRangeRequest{
		Span: roachpb.Span{
			Key:    writes[0],
			EndKey: roachpb.Key(writes[len(writes)-1]).Next(),
		},
		ReturnKeys: true,
	}
	reply, err := client.SendWrappedWith(tds, nil, roachpb.Header{Timestamp: delTS}, del)
	if err != nil {
		t.Fatal(err)
	}
	dr := reply.(*roachpb.DeleteRangeResponse)
	if dr.Txn != nil {
		t.Errorf("expected no transaction in response header")
	}
	if !reflect.DeepEqual(dr.Keys, writes) {
		t.Errorf("expected %d keys to be deleted, but got %d instead", writes, dr.Keys)
	}

	scan := roachpb.NewScan(writes[0], writes[len(writes)-1].Next(), 0)
	txn := &roachpb.Transaction{Name: "MyTxn"}
	reply, err = client.SendWrappedWith(tds, nil, roachpb.Header{Txn: txn}, scan)
	if err != nil {
		t.Fatal(err)
	}
	sr := reply.(*roachpb.ScanResponse)
	if txn := sr.Txn; txn == nil || txn.Name != "MyTxn" {
		t.Errorf("wanted Txn to persist, but it changed to %v", txn)
	}
	if rows := sr.Rows; len(rows) > 0 {
		t.Fatalf("scan after delete returned rows: %v", rows)
	}
}

// TestMultiRangeScanWithMaxResults tests that commands which access multiple
// ranges with MaxResults parameter are carried out properly.
func TestMultiRangeScanWithMaxResults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		splitKeys []roachpb.Key
		keys      []roachpb.Key
	}{
		{[]roachpb.Key{roachpb.Key("m")},
			[]roachpb.Key{roachpb.Key("a"), roachpb.Key("z")}},
		{[]roachpb.Key{roachpb.Key("h"), roachpb.Key("q")},
			[]roachpb.Key{roachpb.Key("b"), roachpb.Key("f"), roachpb.Key("k"),
				roachpb.Key("r"), roachpb.Key("w"), roachpb.Key("y")}},
	}

	for i, tc := range testCases {
		s := StartTestServer(t)
		defer s.Stop()
		retryOpts := kv.GetDefaultDistSenderRetryOptions()
		retryOpts.Closer = s.stopper.ShouldDrain()
		ds := kv.NewDistSender(&kv.DistSenderContext{
			Clock:           s.Clock(),
			RPCContext:      s.RPCContext(),
			RPCRetryOptions: &retryOpts,
		}, s.Gossip())
		tds := kv.NewTxnCoordSender(ds, s.Clock(), s.Ctx.Linearizable, tracing.NewTracer(),
			s.stopper, kv.NewTxnMetrics(metric.NewRegistry()))

		for _, sk := range tc.splitKeys {
			if err := s.node.ctx.DB.AdminSplit(sk); err != nil {
				t.Fatal(err)
			}
		}

		for _, k := range tc.keys {
			put := roachpb.NewPut(k, roachpb.MakeValueFromBytes(k))
			if _, err := client.SendWrapped(tds, nil, put); err != nil {
				t.Fatal(err)
			}
		}

		// Try every possible ScanRequest startKey.
		for start := 0; start < len(tc.keys); start++ {
			// Try every possible maxResults, from 1 to beyond the size of key array.
			for maxResults := 1; maxResults <= len(tc.keys)-start+1; maxResults++ {
				scan := roachpb.NewScan(tc.keys[start], tc.keys[len(tc.keys)-1].Next(),
					int64(maxResults))
				reply, err := client.SendWrapped(tds, nil, scan)
				if err != nil {
					t.Fatal(err)
				}
				rows := reply.(*roachpb.ScanResponse).Rows
				if start+maxResults <= len(tc.keys) && len(rows) != maxResults {
					t.Errorf("%d: start=%s: expected %d rows, but got %d", i, tc.keys[start], maxResults, len(rows))
				} else if start+maxResults == len(tc.keys)+1 && len(rows) != maxResults-1 {
					t.Errorf("%d: expected %d rows, but got %d", i, maxResults-1, len(rows))
				}
			}
		}
	}
}

func TestSystemConfigGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := StartTestServer(t)
	defer s.Stop()

	db := s.db
	key := sql.MakeDescMetadataKey(keys.MaxReservedDescID)
	valAt := func(i int) *sql.DatabaseDescriptor {
		return &sql.DatabaseDescriptor{Name: "foo", ID: sql.ID(i)}
	}

	// Register a callback for gossip updates.
	resultChan := s.Gossip().RegisterSystemConfigChannel()

	// The span gets gossiped when it first shows up.
	select {
	case <-resultChan:

	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive gossip message")
	}

	// Try a plain KV write first.
	if err := db.Put(key, valAt(0)); err != nil {
		t.Fatal(err)
	}

	// Now do it as part of a transaction, but without the trigger set.
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		return txn.Put(key, valAt(1))
	}); pErr != nil {
		t.Fatal(pErr)
	}

	// Gossip channel should be dormant.
	// TODO(tschottdorf): This test is likely flaky. Why can't some other
	// process trigger gossip? It seems that a new leader lease being
	// acquired will gossip a new system config since the hash changed and fail
	// the test (seen in practice during some buggy WIP).
	var systemConfig config.SystemConfig
	select {
	case <-resultChan:
		systemConfig, _ = s.gossip.GetSystemConfig()
		t.Fatalf("unexpected message received on gossip channel: %v", systemConfig)

	case <-time.After(50 * time.Millisecond):
	}

	// This time mark the transaction as having a Gossip trigger.
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		txn.SetSystemConfigTrigger()
		return txn.Put(key, valAt(2))
	}); pErr != nil {
		t.Fatal(pErr)
	}

	// New system config received.
	select {
	case <-resultChan:
		systemConfig, _ = s.gossip.GetSystemConfig()

	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not receive gossip message")
	}

	// Now check the new config.
	var val *roachpb.Value
	for _, kv := range systemConfig.Values {
		if bytes.Equal(key, kv.Key) {
			val = &kv.Value
			break
		}
	}
	if val == nil {
		t.Fatal("key not found in gossiped info")
	}

	// Make sure the returned value is valAt(2).
	got := new(sql.DatabaseDescriptor)
	if err := val.GetProto(got); err != nil {
		t.Fatal(err)
	}
	if expected := valAt(2); !reflect.DeepEqual(got, expected) {
		t.Fatalf("mismatch: expected %+v, got %+v", *expected, *got)
	}
}

func checkOfficialize(t *testing.T, network, oldAddrString, newAddrString, expAddrString string) {
	resolvedAddr := util.NewUnresolvedAddr(network, newAddrString)

	if unresolvedAddr, err := officialAddr(oldAddrString, resolvedAddr); err != nil {
		t.Fatal(err)
	} else if retAddrString := unresolvedAddr.String(); retAddrString != expAddrString {
		t.Errorf("officialAddr(%s, %s) was %s; expected %s", oldAddrString, newAddrString, retAddrString, expAddrString)
	}
}

func TestOfficializeAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}

	for _, network := range []string{"tcp", "tcp4", "tcp6"} {
		checkOfficialize(t, network, "hellow.world:0", "127.0.0.1:1234", "hellow.world:1234")
		checkOfficialize(t, network, "hellow.world:1234", "127.0.0.1:2345", "hellow.world:1234")
		checkOfficialize(t, network, ":1234", "127.0.0.1:2345", net.JoinHostPort(hostname, "1234"))
		checkOfficialize(t, network, ":0", "127.0.0.1:2345", net.JoinHostPort(hostname, "2345"))
	}
}
