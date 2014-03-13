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

package server

import (
	"compress/gzip"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

var (
	serverAddr string
	once       sync.Once
)

func startServer() {
	server := httptest.NewServer(new())
	serverAddr = server.Listener.Addr().String()
	log.Println("Test server listening on", serverAddr)
}

// TestHealthz verifies that /healthz does, in fact, return "ok"
// as expected.
func TestHealthz(t *testing.T) {
	once.Do(startServer)
	addr := "http://" + serverAddr + "/healthz"
	resp, err := http.Get(addr)
	if err != nil {
		t.Fatalf("error requesting healthz at %s: %s", addr, err)
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

// TestGzip hits the /healthz endpoint while explicitly disabling
// decompression on a custom client’s Transport and setting it
// conditionally via the request’s Accept-Encoding headers.
func TestGzip(t *testing.T) {
	once.Do(startServer)
	client := http.Client{
		Transport: &http.Transport{
			Proxy:              http.ProxyFromEnvironment,
			DisableCompression: true,
		},
	}
	req, err := http.NewRequest("GET", "http://"+serverAddr+"/healthz", nil)
	if err != nil {
		t.Fatalf("could not create request: %s", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("could not make request to %s: %s", req.URL, err)
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
	// Test for gzip explicitly.
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("could not make request to %s: %s", req.URL, err)
	}
	defer resp.Body.Close()
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("could not create new gzip reader: %s", err)
	}
	b, err = ioutil.ReadAll(gz)
	if err != nil {
		t.Fatalf("could not read gzipped response body: %s", err)
	}
	if !strings.Contains(string(b), expected) {
		t.Errorf("expected body to contain %q, got %q", expected, string(b))
	}
}

// TestRESTEndpoints tests that the exposed endpoints for modifying keyspace
// are working properly.
func TestRESTEndpoints(t *testing.T) {
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

	for _, c := range testCases {
		req, err := http.NewRequest(c.method, "http://"+serverAddr+dbKeyPrefix+c.key, strings.NewReader(c.payload))
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
			t.Errorf("%s %s: expected response %q, got %q", req.Method, req.URL.Path, c.response, string(b))
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
		key, err := dbKey(dbKeyPrefix + escaped)
		if err != nil {
			t.Errorf("error getting db key from path %s: %s", dbKeyPrefix+escaped, err)
			continue
		}
		if key != expected {
			t.Errorf("expected key value %q, got %q", expected, key)
		}
	}
}
