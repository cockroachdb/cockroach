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
	"log"
	"net/http"
	"strings"
	"testing"
)

// TestKVRESTEndpoints tests that the REST endpoints for modifying KV
// map are working properly.
func TestRESTEndpoints(t *testing.T) {
	s := startServer()
	testCases := []struct {
		method, key, payload, response string
		status                         int
	}{
		{"GET", "my_key", "", "key not found\n", 404},
		{"PUT", "my_key", "is cool", "", 200},
		{"GET", "my_key", "", "is cool", 200},
		{"DELETE", "my_key", "", "", 200},
		{"GET", "my_key", "", "key not found\n", 404},
		{"POST", "my_key", "is cool", "", 200},
		{"GET", "my_key", "", "is cool", 200},
		{"DELETE", "my_key", "", "", 200},
		{"GET", "my_key", "", "key not found\n", 404},
		{"PATCH", "my_key", "", "Bad Request\n", 401},
		{"GET", "Hello, 世界", "", "key not found\n", 404},
		{"PUT", "Hello, 世界", "is cool", "", 200},
		{"GET", "Hello, 世界", "", "is cool", 200},
		{"DELETE", "Hello, 世界", "", "", 200},
		{"GET", "Hello, 世界", "", "key not found\n", 404},
	}

	for i, c := range testCases {
		req, err := http.NewRequest(c.method, s.httpServer.URL+KVKeyPrefix+c.key, strings.NewReader(c.payload))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if string(b) != c.response {
			t.Errorf("%d: %s %s: expected response %q, got %q", i, req.Method, req.URL.Path, c.response, string(b))
		}
	}
}

// TestKeyUnescape ensures that keys specified via URL paths are properly decoded.
func TestKeyUnescape(t *testing.T) {
	testCases := map[string]string{
		"my_key":                      "my_key",
		"Hello%2C+%E4%B8%96%E7%95%8C": "Hello, 世界",
	}
	for escaped, expected := range testCases {
		key, err := dbKey(KVKeyPrefix + escaped)
		if err != nil {
			t.Errorf("error getting db key from path %s: %s", KVKeyPrefix+escaped, err)
			continue
		}
		if string(key) != expected {
			t.Errorf("expected key value %q, got %q", expected, string(key))
		}
	}
}
