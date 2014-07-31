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
// Author: Andrew Bonventre (andybons@gmail.com)

package kv

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

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
	req := TestRequest{"GET", "", "", EntryPrefix}
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
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
	verifySingleEntryTestCases(t, []RequestResponse{
		{
			NewRequest("POST", "", "", CounterPrefix),
			NewResponse(400, "empty key not allowed\n"),
		},
		{
			NewRequest("POST", "some_key", "", CounterPrefix),
			NewResponse(400, "Could not parse int64 for increment\n"),
		},

		{
			NewRequest("PUT", "some_key", "1", CounterPrefix),
			NewResponse(405, "Method Not Allowed\n"),
		},
		{
			NewRequest("GET", "some_key", "", CounterPrefix),
			NewResponse(200, "0"),
		},
		{
			NewRequest("POST", "some_key", "2", CounterPrefix),
			NewResponse(200, "2"),
		},
		{
			NewRequest("GET", "some_key", "", CounterPrefix),
			NewResponse(200, "2"),
		},
		{
			NewRequest("POST", "some_key", "-3", CounterPrefix),
			NewResponse(200, "-1"),
		},
		{
			NewRequest("POST", "some_key", "0", CounterPrefix),
			NewResponse(200, "-1"),
		},
	})
}

// TestKeyUnescape ensures that keys specified via URL paths are properly decoded.
func TestKeyUnescape(t *testing.T) {
	testCases := map[string]string{
		"my_key":                      "my_key",
		"Hello%2C+%E4%B8%96%E7%95%8C": "Hello, 世界",
	}
	for escaped, expected := range testCases {
		key, err := dbKey(EntryPrefix+escaped, EntryPrefix)
		if err != nil {
			t.Errorf("error getting db key from path %s: %s", EntryPrefix+escaped, err)
			continue
		}
		if string(key) != expected {
			t.Errorf("expected key value %q, got %q", expected, string(key))
		}
	}
}

func verifySingleEntryTestCases(t *testing.T, testcases []RequestResponse) {
	s := startNewServer()
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
}

// go's default http error writer adds a new line
func statusText(status int) string {
	return http.StatusText(status) + "\n"
}
