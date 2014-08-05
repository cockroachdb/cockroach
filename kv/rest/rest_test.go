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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)

// Package rest_test contains the HTTP tests for the KV API. It's here because if this was in the rest
// package there would be a circular dependency between package rest and package server.
package rest_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/kv/rest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/golang/glog"
)

type TestRequest struct {
	method, key, body, url string
}

type TestResponse struct {
	status   int
	body, ct string
}

type RequestResponse struct {
	request  TestRequest
	response TestResponse
}

func NewResponse(status int, args ...string) TestResponse {
	resp := TestResponse{status, "", "text/plain; charset=utf-8"}
	switch len(args) {
	case 2:
		resp.ct = args[1]
		fallthrough
	case 1:
		resp.body = args[0]
		fallthrough
	case 0:
		break
	default:
		panic("Expected at most 2 arguments to NewResponse")
	}
	return resp
}

func NewRequest(args ...string) TestRequest {
	req := TestRequest{"GET", "", "", rest.EntryPrefix}
	switch len(args) {
	case 4:
		req.url = args[3]
		fallthrough
	case 3:
		req.body = args[2]
		fallthrough
	case 2:
		req.key = args[1]
		fallthrough
	case 1:
		req.method = args[0]
		fallthrough
	case 0:
		break
	default:
		panic("Expected at most 4 arguments to NewRequest")
	}
	return req
}

func TestHeadAndPut(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("HEAD", "my_key"),
			NewResponse(404),
		},
		{
			NewRequest("PUT", "my_key", "is cool"),
			NewResponse(200),
		},
		{
			NewRequest("HEAD", "my_key"),
			NewResponse(200),
		},
	})
}

func TestGetAndPut(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("GET", "my_key"),
			NewResponse(404, statusText(404)),
		},
		{
			NewRequest("PUT", "my_key", "is pretty cool"),
			NewResponse(200),
		},
		{
			NewRequest("GET", "my_key"),
			NewResponse(200, "is pretty cool", "application/octet-stream"),
		},
	})
}

func TestDelete(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("PUT", "my_key", "is super cool"),
			NewResponse(200),
		},
		{
			NewRequest("DELETE", "my_key"),
			NewResponse(200),
		},
		{
			NewRequest("HEAD", "my_key"),
			NewResponse(404),
		},
	})
}

func TestUnsupportedVerbs(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("PATCH", "my_key", ""),
			NewResponse(405, statusText(405)),
		},
		{
			NewRequest("OPTIONS", "", ""),
			NewResponse(405, statusText(405)),
		},
	})
}

func TestPost(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("HEAD", "my_key"),
			NewResponse(404),
		},
		{
			NewRequest("GET", "my_key"),
			NewResponse(404, statusText(404)),
		},
		{
			NewRequest("POST", "my_key", "is totes cool"),
			NewResponse(200),
		},
		{
			NewRequest("HEAD", "my_key"),
			NewResponse(200),
		},
		{
			NewRequest("GET", "my_key"),
			NewResponse(200, "is totes cool", "application/octet-stream"),
		},
	})
}

func TestNonAsciiKeys(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("HEAD", "Hello, 世界"),
			NewResponse(404),
		},
		{
			NewRequest("GET", "Hello, 世界"),
			NewResponse(404, statusText(404)),
		},
		{
			NewRequest("PUT", "Hello, 世界", "is nonascii cool"),
			NewResponse(200),
		},
		{
			NewRequest("HEAD", "Hello, 世界"),
			NewResponse(200),
		},
		{
			NewRequest("GET", "Hello, 世界"),
			NewResponse(200, "is nonascii cool", "application/octet-stream"),
		},
		{
			NewRequest("DELETE", "Hello, 世界"),
			NewResponse(200),
		},
		{
			NewRequest("HEAD", "Hello, 世界"),
			NewResponse(404),
		},
		{
			NewRequest("GET", "Hello, 世界"),
			NewResponse(404, statusText(404)),
		},
		{
			NewRequest("HEAD", "Hello, 世界"),
			NewResponse(404),
		},
	})
}

func TestEmptyKeysAndValues(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("GET"),
			NewResponse(400, "empty key not allowed\n"),
		},
		{
			NewRequest("POST"),
			NewResponse(400, "empty key not allowed\n"),
		},
		{
			NewRequest("POST", "emptea"),
			NewResponse(200),
		},
		{
			NewRequest("GET", "emptea"),
			NewResponse(200, "", "application/octet-stream"),
		},
		{
			NewRequest("DELETE", "emptea"),
			NewResponse(200),
		},
		{
			NewRequest("GET", "emptea"),
			NewResponse(404, statusText(404)),
		},
	})
}

func TestIncrement(t *testing.T) {
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("POST", "", "", rest.CounterPrefix),
			NewResponse(400, "empty key not allowed\n"),
		},
		{
			NewRequest("POST", "some_key", "", rest.CounterPrefix),
			NewResponse(400, "Could not parse int64 for increment\n"),
		},

		{
			NewRequest("PUT", "some_key", "1", rest.CounterPrefix),
			NewResponse(405, "Method Not Allowed\n"),
		},
		{
			NewRequest("GET", "some_key", "", rest.CounterPrefix),
			NewResponse(200, "0"),
		},
		{
			NewRequest("POST", "some_key", "2", rest.CounterPrefix),
			NewResponse(200, "2"),
		},
		{
			NewRequest("GET", "some_key", "", rest.CounterPrefix),
			NewResponse(200, "2"),
		},
		{
			NewRequest("POST", "some_key", "-3", rest.CounterPrefix),
			NewResponse(200, "-1"),
		},
		{
			NewRequest("POST", "some_key", "0", rest.CounterPrefix),
			NewResponse(200, "-1"),
		},
	})
}

func TestCounterAndEntryInteraction(t *testing.T) {
	// TODO(zbrock + matthew) Counters and Entries aren't currently distinguished in the storage layer. When they are, implement checking for types.
	t.Skip("Counter operations shouldn't work on Entry values and vice versa")
	runHTTPTestFixture(t, []RequestResponse{
		// Set our key as an Entry.
		{
			NewRequest("POST", "some_entry", "some value"),
			NewResponse(200),
		},
		// Counter operations should fail.
		//		{ TODO(zbrock+matthew) Implement HEAD for Counter API
		//			NewRequest("HEAD", "some_entry", "", rest.CounterPrefix),
		//			NewResponse(400, statusText(400)),
		//		},
		{
			NewRequest("GET", "some_entry", "", rest.CounterPrefix),
			NewResponse(400, statusText(400)),
		},
		{
			NewRequest("POST", "some_entry", "2", rest.CounterPrefix),
			NewResponse(400, statusText(400)),
		},
		// Set our key as a Counter.
		{
			NewRequest("POST", "some_counter", "2", rest.CounterPrefix),
			NewResponse(200, "2"),
		},
		// Entry operations should fail.
		{
			NewRequest("HEAD", "some_counter"),
			NewResponse(400, statusText(400)),
		},
		{
			NewRequest("GET", "some_counter"),
			NewResponse(400, statusText(400)),
		},
		{
			NewRequest("POST", "some_counter", "some value"),
			NewResponse(400, statusText(400)),
		},
	})
}

// TestNullPrefixedKeys makes sure that the internal system keys are not accessible through the HTTP API.
func TestNullPrefixedKeys(t *testing.T) {
	// TODO(zbrock + matthew) fix this once sqlite key encoding is finished so we can namespace user keys
	t.Skip("Internal Meta1 Keys should not be accessible from the HTTP REST API. But they are right now.")

	metaKey := engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax)
	s := startNewServer()

	// Precondition: we want to make sure the meta1 key exists.
	initialVal, err := s.rawGet(metaKey)
	if err != nil {
		t.Fatalf("Precondition Failed! Unable to fetch %+v from local db", metaKey)
	}
	if initialVal == nil {
		t.Fatalf("Precondition Failed! Expected meta1 key to exist in the underlying store, but no value found")
	}

	// Try to manipulate the meta1 key.
	encMeta1Key := url.QueryEscape(string(metaKey))
	runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("GET", encMeta1Key),
			NewResponse(404),
		},
		{
			NewRequest("POST", encMeta1Key, "cool"),
			NewResponse(200),
		},
		{
			NewRequest("GET", encMeta1Key),
			NewResponse(200, "cool", "application/octet-stream"),
		},
	}, s)

	// Postcondition: the meta1 key is untouched.
	afterVal, err := s.rawGet(metaKey)
	if err != nil {
		t.Errorf("Unable to fetch %+v from local db", metaKey)
	}
	if !bytes.Equal(afterVal, initialVal) {
		t.Errorf("Expected meta1 to be unchanged, but it differed: %+v", afterVal)
	}
}

func TestKeysAndBodyArePreserved(t *testing.T) {
	encKey := "%00some%2Fkey%20that%20encodes%E4%B8%96%E7%95%8C"
	encBody := "%00some%2FBODY%20that%20encodes"
	s := runHTTPTestFixture(t, []RequestResponse{
		{
			NewRequest("POST", encKey, encBody),
			NewResponse(200),
		},
		{
			NewRequest("GET", encKey),
			NewResponse(200, encBody, "application/octet-stream"),
		},
	})
	gr := <-s.db.Get(&storage.GetRequest{
		RequestHeader: storage.RequestHeader{
			Key:  engine.Key("\x00some/key that encodes世界"),
			User: storage.UserRoot,
		},
	})
	if gr.Error != nil {
		t.Errorf("Unable to fetch values from local db")
	}
	if !bytes.Equal(gr.Value.Bytes, []byte(encBody)) {
		t.Errorf("Expected value (%s) but got (%s)", encBody, gr.Value.Bytes)
	}
}

func runHTTPTestFixture(t *testing.T, testcases []RequestResponse, args ...*kvTestServer) *kvTestServer {
	var s *kvTestServer

	// Initialize the test server if one isn't provided.
	if len(args) == 1 {
		s = args[0]
	} else if len(args) == 0 {
		s = startNewServer()
	} else {
		panic("Internal error: Too many args to this helper function.")
	}

	// Run all the given test cases through the server.
	for i, tc := range testcases {
		tReq, tResp := tc.request, tc.response
		req, err := http.NewRequest(tReq.method, s.httpServer.URL+tReq.url+tReq.key, strings.NewReader(tReq.body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			glog.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if resp.StatusCode != tResp.status {
			t.Errorf("%d: %s %s: expected response status of %d, got %d", i, req.Method, req.URL.Path, tResp.status, resp.StatusCode)
		}
		if string(b) != tResp.body {
			t.Errorf("%d: %s %s: expected response %q, got %q", i, req.Method, req.URL.Path, tResp.body, string(b))
		}
		if resp.Header.Get("Content-Type") != tResp.ct {
			t.Errorf("%d: %s %s: expected Content-Type to be %q, got %q", i, req.Method, req.URL.Path, tResp.ct, resp.Header.Get("Content-Type"))
		}
	}
	return s
}

// statusText appends a new line because go's default http error writer adds a new line.
func statusText(status int) string {
	return http.StatusText(status) + "\n"
}

type kvTestServer struct {
	db         kv.DB
	rest       *rest.RESTServer
	httpServer *httptest.Server
	firstRange *storage.Range
}

func startNewServer() *kvTestServer {
	s := &kvTestServer{}

	// Initialize engine, store, and localDB.
	e := engine.NewInMem(engine.Attributes{}, 1<<20)
	localDB, err := server.BootstrapCluster("test-cluster", e)
	if err != nil {
		panic(err)
	}
	s.db = localDB

	// Rip through the stores (should be just one) and grab the first range (there should also just be one).
	localDB.VisitStores(func(store *storage.Store) error {
		rs := store.GetRanges()
		if len(rs) > 0 {
			s.firstRange = rs[0]
		}
		return nil
	})
	if s.firstRange == nil {
		panic("Internal Error: Expected to find a range while initializing test server!")
	}

	// Initialize the REST server.
	s.rest = rest.NewRESTServer(s.db)
	s.httpServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.rest.HandleAction(w, r)
	}))

	return s
}

// rawGet goes directly to the Range for the Get. The reason is that we don't want any intermediate user land
// encoding to get in the way. We want the actual stored byte value without any chance of URL-encoding or SQLite-encoding.
func (s *kvTestServer) rawGet(key engine.Key) ([]byte, error) {
	req := &storage.GetRequest{storage.RequestHeader{Key: key, User: storage.UserRoot}}
	resp := &storage.GetResponse{}
	s.firstRange.Get(req, resp)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Value.Bytes, nil
}
