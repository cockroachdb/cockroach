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
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)
// Author: Andrew Bonventre (andybons@gmail.com)

// Package kv_test contains the HTTP tests for the KV API. It's here because if this was in the kv
// package there would be a circular dependency between package rest and package server.
// TODO(andybons): create mock DB to eliminate the circular dependency.
package kv_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	. "github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	gogoproto "github.com/gogo/protobuf/proto"
)

// startServer returns the server, server address and a KV client for
// access to the underlying database. The server should be closed by
// the caller.
func startServer(t *testing.T) (string, *httptest.Server, *client.KV) {
	// Initialize engine, store, and localDB.
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	db, err := server.BootstrapCluster("test-cluster", e, server.NewContext())
	if err != nil {
		t.Fatalf("could not bootstrap test cluster: %s", err)
	}
	mux := http.NewServeMux()
	mux.Handle(RESTPrefix, NewRESTServer(db))
	mux.Handle(DBPrefix, NewDBServer(db.Sender()))
	server := httptest.NewServer(mux)
	addr := server.Listener.Addr().String()
	return addr, server, db
}

// HTTP methods, defined in RFC 2616.
const (
	methodGet     = "GET"
	methodPut     = "PUT"
	methodPost    = "POST"
	methodDelete  = "DELETE"
	methodHead    = "HEAD"
	methodPatch   = "PATCH"
	methodOptions = "OPTIONS"
)

type protoResp struct {
	Value proto.Value `json:"value"`
}

func TestMethods(t *testing.T) {
	addr, server, _ := startServer(t)
	defer server.Close()

	testKey, testVal := "Hello, 世界", "世界 is cool"
	testCases := []struct {
		method, key string
		body        io.Reader
		statusCode  int
		resp        []byte
	}{
		// The order of the operations within these groups must be preserved.
		// Test basic CRUD; PUT, GET, DELETE, etc.
		{methodHead, testKey, nil, http.StatusNotFound, nil},
		{methodGet, testKey, nil, http.StatusNotFound, nil},
		{methodPut, testKey, strings.NewReader(testVal), http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusOK, nil},
		{methodGet, testKey, nil, http.StatusOK, []byte(testVal)},
		{methodDelete, testKey, nil, http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusNotFound, nil},
		{methodGet, testKey, nil, http.StatusNotFound, nil},
		// Test that POST behaves just like PUT.
		{methodPost, testKey, strings.NewReader(testVal), http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusOK, nil},
		{methodGet, testKey, nil, http.StatusOK, []byte(testVal)},
		// Test that unsupported methods are not acceptable.
		{methodPatch, testKey, nil, http.StatusMethodNotAllowed, nil},
		{methodOptions, testKey, nil, http.StatusMethodNotAllowed, nil},
		// Test that empty keys are not acceptable, but empty values are.
		{methodGet, "", nil, http.StatusBadRequest, nil},
		{methodPost, "", nil, http.StatusBadRequest, nil},
		{methodPut, testKey, nil, http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusOK, nil},
		{methodGet, testKey, nil, http.StatusOK, nil},
		{methodDelete, testKey, nil, http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusNotFound, nil},
		{methodGet, testKey, nil, http.StatusNotFound, nil},
	}
	for _, tc := range testCases {
		resp, err := httpDo(addr, tc.method, EntryPrefix+tc.key, tc.body)
		if err != nil {
			t.Errorf("[%s] %s: error making request: %s", tc.method, tc.key, err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != tc.statusCode {
			t.Errorf("[%s] %s: expected status code to be %d; got %d", tc.method, tc.key, tc.statusCode, resp.StatusCode)
			continue
		}
		if tc.method == methodHead ||
			resp.StatusCode == http.StatusMethodNotAllowed ||
			resp.StatusCode == http.StatusBadRequest {
			continue
		}
		var pr protoResp
		if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
			t.Errorf("[%s] %s: could not json decode response body: %s", tc.method, tc.key, err)
			continue
		}
		if !bytes.Equal(pr.Value.Bytes, tc.resp) {
			t.Errorf("[%s] %s: response bytes not equal: expected %q, got %q", tc.method, tc.key, string(tc.resp), string(pr.Value.Bytes))
			continue
		}
		contentType := resp.Header.Get("Content-Type")
		if tc.method == methodGet &&
			tc.statusCode == http.StatusOK &&
			contentType != "application/json" {
			t.Errorf("[%s] %s: unexpected content type %s", tc.method, tc.key, contentType)
			continue
		}
	}
}

func TestRange(t *testing.T) {
	addr, server, _ := startServer(t)
	defer server.Close()

	// Create range of keys (with counters interspersed).
	baseURL := "http://" + addr
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%.2d", i)
		val := fmt.Sprintf("value_%.2d", i)
		prefix := EntryPrefix
		// Intersperse counters every tenth key.
		if i > 0 && i%10 == 0 {
			prefix = CounterPrefix
			val = strconv.Itoa(i)
		}
		postURL(baseURL+prefix+key, strings.NewReader(val), t)
	}
	// Query subset of that range.
	start, end := 5, 25
	url := fmt.Sprintf("%s%s?start=key_%.2d&end=key_%.2d", baseURL, RangePrefix, start, end)
	var scan proto.ScanResponse
	if err := json.NewDecoder(strings.NewReader(getURL(url, t))).Decode(&scan); err != nil {
		t.Errorf("unable to decode JSON into proto.ScanResponse: %s", err)
	}
	for i, row := range scan.Rows {
		n := i + start
		if !verifyRangeRowIsGood(i, n, row, t) {
			continue
		}
	}
	// Query limit of that range.
	start, end = 5, 99
	limit := 25
	url = fmt.Sprintf("%s%s?start=key_%.2d&end=key_%.2d&limit=%d", baseURL, RangePrefix, start, end, limit)
	scan = proto.ScanResponse{}
	if err := json.NewDecoder(strings.NewReader(getURL(url, t))).Decode(&scan); err != nil {
		t.Errorf("unable to decode JSON into proto.ScanResponse: %s", err)
	}
	if len(scan.Rows) != limit {
		t.Errorf("expected number of rows returned to be %d; got %d", limit, len(scan.Rows))
	}
	for i, row := range scan.Rows {
		n := i + start
		if !verifyRangeRowIsGood(i, n, row, t) {
			continue
		}
	}
	// Delete limit of that range. Start: 5, end: 99, limit: 25 –> keys 5-30 deleted.
	path := fmt.Sprintf("%s?start=key_%.2d&end=key_%.2d&limit=%d", RangePrefix, start, end, limit)
	resp, err := httpDo(addr, methodDelete, path, nil)
	if err != nil {
		t.Errorf("error attempting to delete range: %s", err)
	}
	defer resp.Body.Close()
	checkStatus(resp, t)
	// Query remaining range.
	start, end = 0, 99
	url = fmt.Sprintf("%s%s?start=key_%.2d&end=key_%.2d", baseURL, RangePrefix, start, end)
	scan = proto.ScanResponse{}
	if err := json.NewDecoder(strings.NewReader(getURL(url, t))).Decode(&scan); err != nil {
		t.Errorf("unable to decode JSON into proto.ScanResponse: %s", err)
	}
	numRows := end - limit
	if len(scan.Rows) != numRows {
		t.Errorf("expected number of rows returned to be %d; got %d", numRows, len(scan.Rows))
	}
	for i, row := range scan.Rows {
		// 25 keys were deleted after the 5th key. Change the offset appropriately.
		if i >= 5 {
			start = limit
		}
		n := i + start
		if !verifyRangeRowIsGood(i, n, row, t) {
			continue
		}
	}
	// Delete remaining range.
	start, end = 0, 99
	path = fmt.Sprintf("%s?start=key_%.2d&end=key_%.2d", RangePrefix, start, end)
	resp, err = httpDo(addr, methodDelete, path, nil)
	if err != nil {
		t.Errorf("error attempting to delete range: %s", err)
	}
	defer resp.Body.Close()
	checkStatus(resp, t)

	// Query key range.
	scan = proto.ScanResponse{}
	if err := json.NewDecoder(strings.NewReader(getURL(url, t))).Decode(&scan); err != nil {
		t.Errorf("unable to decode JSON into proto.ScanResponse: %s", err)
	}
	if len(scan.Rows) != 0 {
		t.Errorf("expected zero rows in response, got %d:", len(scan.Rows))
		for _, row := range scan.Rows {
			t.Logf("%q -> %q", string(row.Key), string(row.Value.Bytes))
		}
	}
}

// verifyRangeRowIsGood tests whether a row at a given index i holds the
// appropriate values key_<n> -> value_<n> or key_<n> -> <counter value n>
// in the case where n is greater than zero and a multiple of ten. This
// is used in tandem with TestRange.
func verifyRangeRowIsGood(i, n int, row proto.KeyValue, t *testing.T) bool {
	if n > 0 && n%10 == 0 {
		if row.Value.Integer == nil {
			t.Errorf("expected row %d counter value (key=%q) to have non-nil Integer field", i, string(row.Key))
			return false
		}
		if *row.Value.Integer != int64(n) {
			t.Errorf("expected row %d counter value (key=%q) in scan to be %d; got %d", i, string(row.Key), n, row.Value.Integer)
			return false
		}
		return true
	}
	expected := fmt.Sprintf("value_%.2d", n)
	if string(row.Value.Bytes) != expected {
		t.Errorf("expected row %d value (key=%q) in scan to be %q; got %q", i, string(row.Key), expected, string(row.Value.Bytes))
		return false
	}
	return true
}

func checkStatus(resp *http.Response, t *testing.T) {
	if resp.StatusCode == http.StatusOK {
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("could not read response body: %s", err)
	}
	t.Errorf("expected 200 OK; got %d: %s", resp.StatusCode, string(b))
}

func TestIncrement(t *testing.T) {
	addr, server, _ := startServer(t)
	defer server.Close()

	testKey := "Hello, 世界"
	testCases := []struct {
		method, key     string
		val, statusCode int
		resp            int64
	}{
		// The order of the operations within these groups must be preserved.
		{methodPost, "", 0, http.StatusBadRequest, 0},
		{methodHead, testKey, 0, http.StatusNotFound, 0},
		{methodPut, testKey, 0, http.StatusMethodNotAllowed, 0},
		{methodGet, testKey, 0, http.StatusOK, 0},
		{methodPost, testKey, 2, http.StatusOK, 2},
		{methodGet, testKey, 0, http.StatusOK, 2},
		{methodHead, testKey, 0, http.StatusOK, 0},
		{methodPost, testKey, -3, http.StatusOK, -1},
		{methodPost, testKey, 0, http.StatusOK, -1},
		{methodDelete, testKey, 0, http.StatusOK, 0},
		{methodHead, testKey, 0, http.StatusNotFound, 0},
	}
	for _, tc := range testCases {
		var body io.Reader
		if tc.statusCode == http.StatusOK && tc.method == methodPost {
			body = strings.NewReader(strconv.Itoa(tc.val))
		}
		resp, err := httpDo(addr, tc.method, CounterPrefix+tc.key, body)
		if err != nil {
			t.Errorf("[%s] %s: error making request: %s", tc.method, tc.key, err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != tc.statusCode {
			t.Errorf("[%s] %s: expected status code to be %d; got %d", tc.method, tc.key, tc.statusCode, resp.StatusCode)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			continue
		}
		// Responses are empty for HEAD and DELETE requests.
		if tc.method != methodGet && tc.method != methodPost {
			continue
		}
		var incResp proto.IncrementResponse
		if err := json.NewDecoder(resp.Body).Decode(&incResp); err != nil {
			t.Errorf("[%s] %s: could not decode response body: %s", tc.method, tc.key, err)
			continue
		}
		if incResp.NewValue != tc.resp {
			t.Errorf("[%s] %s: expected response to be %d; got %d", tc.method, tc.key, tc.resp, incResp.NewValue)
			continue
		}
	}
}

func TestMixingCounters(t *testing.T) {
	addr, server, _ := startServer(t)
	defer server.Close()

	entryKey := "value"
	counterKey := "counter"

	testCases := []struct {
		method             string
		counter, increment bool
		statusCode         int
	}{
		// The order of the operations within these groups must be preserved.

		// First, post the counter and entry keys.
		{methodPost, true, true, http.StatusOK},
		{methodPost, false, false, http.StatusOK},

		// Now, verify we can increment counter and update entry key.
		{methodPost, true, true, http.StatusOK},
		{methodPost, false, false, http.StatusOK},

		// Now, try to reverse both. Setting counter key as entry is fine,
		// but trying to increment entry key should fail.
		{methodPost, true, false, http.StatusOK},
		{methodPost, false, true, http.StatusBadRequest},
	}
	for _, tc := range testCases {
		var body io.Reader
		key := entryKey
		prefix := EntryPrefix
		if tc.counter {
			key = counterKey
		}
		if tc.increment {
			prefix = CounterPrefix
		}
		if tc.statusCode == http.StatusOK && tc.method == methodPost {
			body = strings.NewReader("1")
		}
		resp, err := httpDo(addr, tc.method, prefix+key, body)
		if err != nil {
			t.Errorf("[%s] %s: error making request: %s", tc.method, key, err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != tc.statusCode {
			t.Errorf("[%s] %s: expected status code to be %d; got %d", tc.method, key, tc.statusCode, resp.StatusCode)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			continue
		}
	}
}

// TestSystemKeys makes sure that the internal system keys are
// accessible through the HTTP API.
// TODO(spencer): we need to ensure proper permissions through the
// HTTP API.
func TestSystemKeys(t *testing.T) {
	addr, server, _ := startServer(t)
	defer server.Close()

	// Compute expected system key.
	desc := &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: engine.KeyMin,
		EndKey:   engine.KeyMax,
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
	protoBytes, err := gogoproto.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}

	// Manipulate the meta1 key.
	metaKey := engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax)
	encMeta1Key := url.QueryEscape(string(metaKey))
	url := "http://" + addr + EntryPrefix + encMeta1Key
	resp := getURL(url, t)
	var pr protoResp
	if err := json.Unmarshal([]byte(resp), &pr); err != nil {
		t.Fatalf("could not unmarshal response %q: %s", resp, err)
	}
	if !bytes.Equal(pr.Value.Bytes, protoBytes) {
		t.Fatalf("expected %q; got %q", string(protoBytes), pr.Value.Bytes)
	}
	val := "Hello, 世界"
	postURL(url, strings.NewReader(val), t)
	resp = getURL(url, t)
	pr = protoResp{}
	if err := json.Unmarshal([]byte(resp), &pr); err != nil {
		t.Fatalf("could not unmarshal response %q: %s", resp, err)
	}
	if string(pr.Value.Bytes) != val {
		t.Fatalf("expected %q; got %q", val, string(pr.Value.Bytes))
	}
}

func TestKeysAndBodyArePreserved(t *testing.T) {
	addr, server, db := startServer(t)
	defer server.Close()

	encKey := "%00some%2Fkey%20that%20encodes%E4%B8%96%E7%95%8C"
	encBody := "%00some%2FBODY%20that%20encodes"
	url := "http://" + addr + EntryPrefix + encKey
	postURL(url, strings.NewReader(encBody), t)
	resp := getURL(url, t)
	var pr protoResp
	if err := json.Unmarshal([]byte(resp), &pr); err != nil {
		t.Fatalf("could not unmarshal response %q: %s", resp, err)
	}
	if !bytes.Equal([]byte(encBody), pr.Value.Bytes) {
		t.Fatalf("expected body to be %q; got %q", encBody, string(pr.Value.Bytes))
	}
	gr := &proto.GetResponse{}
	if err := db.Call(proto.Get, &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:  proto.Key("\x00some/key that encodes世界"),
			User: storage.UserRoot,
		},
	}, gr); err != nil {
		t.Errorf("unable to fetch values from local db: %s", err)
	}
	if !bytes.Equal(gr.Value.Bytes, []byte(encBody)) {
		t.Errorf("expected %q; got %q", encBody, gr.Value.Bytes)
	}
}

func postURL(url string, body io.Reader, t *testing.T) {
	resp, err := http.Post(url, "text/plain", body)
	defer resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected OK status code; got %d", resp.StatusCode)
	}
}

func getURL(url string, t *testing.T) string {
	resp, err := http.Get(url)
	defer resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK status code; got %d: %q", resp.StatusCode, string(b))
	}
	return string(b)
}

func httpDo(addr, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, "http://"+addr+path, body)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// statusText appends a new line because go's default http error writer adds a new line.
func statusText(status int) string {
	return http.StatusText(status) + "\n"
}
