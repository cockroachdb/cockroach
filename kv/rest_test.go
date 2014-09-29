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
	"sync"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

var (
	testDB     *kv.DB
	serverAddr string
	once       sync.Once
)

func startServer(t *testing.T) {
	// Initialize engine, store, and localDB.
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	db, err := server.BootstrapCluster("test-cluster", e)
	if err != nil {
		t.Fatalf("could not bootstrap test cluster: %v", err)
	}
	server := httptest.NewServer(kv.NewRESTServer(db))
	serverAddr = server.Listener.Addr().String()
	testDB = db
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

func TestMethods(t *testing.T) {
	once.Do(func() { startServer(t) })
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
		{methodGet, testKey, nil, http.StatusNotFound, []byte(statusText(http.StatusNotFound))},
		{methodPut, testKey, strings.NewReader(testVal), http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusOK, nil},
		{methodGet, testKey, nil, http.StatusOK, []byte(testVal)},
		{methodDelete, testKey, nil, http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusNotFound, nil},
		{methodGet, testKey, nil, http.StatusNotFound, []byte(statusText(http.StatusNotFound))},
		// Test that POST behaves just like PUT.
		{methodPost, testKey, strings.NewReader(testVal), http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusOK, nil},
		{methodGet, testKey, nil, http.StatusOK, []byte(testVal)},
		// Test that unsupported methods are not acceptable.
		{methodPatch, testKey, nil, http.StatusMethodNotAllowed, []byte(statusText(http.StatusMethodNotAllowed))},
		{methodOptions, testKey, nil, http.StatusMethodNotAllowed, []byte(statusText(http.StatusMethodNotAllowed))},
		// Test that empty keys are not acceptable, but empty values are.
		{methodGet, "", nil, http.StatusBadRequest, []byte("empty key not allowed\n")},
		{methodPost, "", nil, http.StatusBadRequest, []byte("empty key not allowed\n")},
		{methodPut, testKey, nil, http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusOK, nil},
		{methodGet, testKey, nil, http.StatusOK, nil},
		{methodDelete, testKey, nil, http.StatusOK, nil},
		{methodHead, testKey, nil, http.StatusNotFound, nil},
		{methodGet, testKey, nil, http.StatusNotFound, []byte(statusText(http.StatusNotFound))},
	}
	for _, tc := range testCases {
		resp, err := httpDo(tc.method, kv.EntryPrefix+tc.key, tc.body)
		if err != nil {
			t.Errorf("[%s] %s: error making request: %v", tc.method, tc.key, err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != tc.statusCode {
			t.Errorf("[%s] %s: expected status code to be %d; got %d", tc.method, tc.key, tc.statusCode, resp.StatusCode)
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("[%s] %s: could not read response body: %v", tc.method, tc.key, err)
			continue
		}
		if !bytes.Equal(b, tc.resp) {
			t.Errorf("[%s] %s: response bytes not equal: expected %q, got %q", tc.method, tc.key, string(tc.resp), string(b))
			continue
		}
		contentType := resp.Header.Get("Content-Type")
		if tc.method == methodGet &&
			tc.statusCode == http.StatusOK &&
			contentType != "application/octet-stream" {
			t.Errorf("[%s] %s: unexpected content type %s", tc.method, tc.key, contentType)
			continue
		}
	}
}

func TestRange(t *testing.T) {
	// Create range of keys (with counters interspersed).
	baseURL := "http://" + serverAddr
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%.2d", i)
		val := fmt.Sprintf("value_%.2d", i)
		prefix := kv.EntryPrefix
		// Intersperse counters every tenth key.
		if i%10 == 0 {
			prefix = kv.CounterPrefix
			val = strconv.Itoa(i)
		}
		postURL(baseURL+prefix+key, strings.NewReader(val), t)
	}
	// Query subset of that range.
	start, end := 5, 25
	url := fmt.Sprintf("%s%s?start=key_%.2d&end=key_%.2d", baseURL, kv.RangePrefix, start, end)
	s := getURL(url, t)
	var scan proto.ScanResponse
	if err := json.NewDecoder(strings.NewReader(s)).Decode(&scan); err != nil {
		t.Errorf("unable to decode JSON into proto.ScanResponse: %v", err)
	}
	for i, row := range scan.Rows {
		n := i + start
		if n%10 == 0 {
			// A counter is expected in this case.
			// TODO(andybons): No info is returned in the struct in this case
			// that would indicate it’s a counter.
			continue
		}
		expected := fmt.Sprintf("value_%.2d", n)
		if string(row.Value.Bytes) != expected {
			t.Errorf("expected row %d value (key=%q) in scan to be %q; got %q", i, string(row.Key), expected, string(row.Value.Bytes))
		}
	}
	// TODO(andybons):
	// Query limit of that range.
	// Delete limit of that range.
	// Query remaining range.
	// Delete remaining range.
	// Query key range.
}

func TestIncrement(t *testing.T) {
	once.Do(func() { startServer(t) })
	testKey := "Hello, 世界"
	testCases := []struct {
		method, key           string
		val, statusCode, resp int
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
		resp, err := httpDo(tc.method, kv.CounterPrefix+tc.key, body)
		if err != nil {
			t.Errorf("[%s] %s: error making request: %v", tc.method, tc.key, err)
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
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("[%s] %s: could not read response body: %v", tc.method, tc.key, err)
			continue
		}
		// Responses are empty for HEAD and DELETE requests.
		if tc.method != methodGet && tc.method != methodPost {
			continue
		}
		i, err := strconv.Atoi(string(b))
		if err != nil {
			t.Errorf("[%s] %s: could not convert body %s to int: %v", tc.method, tc.key, string(b), err)
			continue
		}
		if i != tc.resp {
			t.Errorf("[%s] %s: expected response to be %d; got %d", tc.method, tc.key, tc.resp, i)
			continue
		}
	}
}

// TestSystemKeys makes sure that the internal system keys are
// accessible through the HTTP API.
// TODO(spencer): we need to ensure proper permissions through the
// HTTP API.
func TestSystemKeys(t *testing.T) {
	// Compute expected system key.
	desc := &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: engine.KeyMin,
		EndKey:   engine.KeyMax,
		Replicas: []proto.Replica{
			proto.Replica{
				NodeID:  1,
				StoreID: 1,
				RangeID: 1,
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
	url := "http://" + serverAddr + kv.EntryPrefix + encMeta1Key
	resp := getURL(url, t)
	if resp != string(protoBytes) {
		t.Fatalf("expected %q; got %q", string(protoBytes), resp)
	}
	val := "Hello, 世界"
	postURL(url, strings.NewReader(val), t)
	resp = getURL(url, t)
	if resp != val {
		t.Fatalf("expected %q; got %q", val, resp)
	}
}

func TestKeysAndBodyArePreserved(t *testing.T) {
	once.Do(func() { startServer(t) })
	encKey := "%00some%2Fkey%20that%20encodes%E4%B8%96%E7%95%8C"
	encBody := "%00some%2FBODY%20that%20encodes"
	url := "http://" + serverAddr + kv.EntryPrefix + encKey
	postURL(url, strings.NewReader(encBody), t)
	val := getURL(url, t)
	if encBody != val {
		t.Fatalf("expected body to be %s; got %s", encBody, val)
	}
	gr := <-testDB.Get(&proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:  engine.Key("\x00some/key that encodes世界"),
			User: storage.UserRoot,
		},
	})
	if gr.Error != nil {
		t.Errorf("unable to fetch values from local db: %v", gr.Error)
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
	once.Do(func() { startServer(t) })
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

func httpDo(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, "http://"+serverAddr+path, body)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// statusText appends a new line because go's default http error writer adds a new line.
func statusText(status int) string {
	return http.StatusText(status) + "\n"
}
