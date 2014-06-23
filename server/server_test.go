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
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/storage"
	"github.com/golang/glog"
)

var (
	s              *server
	serverTestOnce sync.Once
)

func init() {
	// We update these with the actual port once the servers
	// have been launched for the purpose of this test.
	*httpAddr = "localhost:0"
	*rpcAddr = "localhost:0"
}

func startServer() *server {
	serverTestOnce.Do(func() {
		resetTestData()
		s, err := newServer()
		if err != nil {
			glog.Fatal(err)
		}
		engines := []storage.Engine{storage.NewInMem(1 << 20)}
		if _, err := BootstrapCluster("cluster-1", engines[0]); err != nil {
			glog.Fatal(err)
		}
		err = s.start(engines, true) // TODO(spencer): should shutdown server.
		if err != nil {
			glog.Fatalf("Could not start server: %s", err)
		}

		// Update the configuration variables to reflect the actual
		// sockets bound during this test.
		*httpAddr = (*s.httpListener).Addr().String()
		*rpcAddr = s.rpc.Addr().String()
		glog.Infof("Test server listening on http: %s, rpc: %s", *httpAddr, *rpcAddr)
	})
	return s
}

// TestInitEngine tests whether the data directory string is parsed correctly.
func TestInitEngine(t *testing.T) {
	testCases := []struct {
		key          string           // data directory
		expectedType storage.DiskType // type for created engine
		wantError    bool             // do we expect an error from this key?
	}{
		{"mem=1000", storage.MEM, false},
		{"ssd=/tmp/.foobar", storage.SSD, false},
		{"hdd=/tmp/.foobar2", storage.HDD, false},
		{"", storage.HDD, true},
		{"  ", storage.HDD, true},
		{"arbitrarystring", storage.HDD, true},
		{"mem=notaninteger", storage.HDD, true},
		{"mem=", storage.HDD, true},
		{"ssd=", storage.HDD, true},
		{"hdd=", storage.HDD, true},
		{"abc=/dev/null", storage.HDD, true},
	}
	for _, spec := range testCases {
		engine, err := initEngine(spec.key)
		if err == nil {
			if spec.wantError {
				t.Fatalf("invalid engine spec '%v' erroneously accepted", spec.key)
			}
			if engine.Type() != spec.expectedType {
				t.Errorf("wrong engine type created, expected %v but got %v", spec.expectedType, engine.Type())
			}
		} else if !spec.wantError {
			t.Error(err)
		}
	}
}

func resetTestData() {
	// TODO(spencer): remove all data files once rocksdb is hooked up.
}

// TestHealthz verifies that /_admin/healthz does, in fact, return "ok"
// as expected.
func TestHealthz(t *testing.T) {
	startServer()
	defer resetTestData()
	url := "http://" + *httpAddr + "/_admin/healthz"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("error requesting healthz at %s: %s", url, err)
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

// TestGzip hits the /_admin/healthz endpoint while explicitly disabling
// decompression on a custom client's Transport and setting it
// conditionally via the request's Accept-Encoding headers.
func TestGzip(t *testing.T) {
	startServer()
	defer resetTestData()
	client := http.Client{
		Transport: &http.Transport{
			Proxy:              http.ProxyFromEnvironment,
			DisableCompression: true,
		},
	}
	req, err := http.NewRequest("GET", "http://"+*httpAddr+"/_admin/healthz", nil)
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
