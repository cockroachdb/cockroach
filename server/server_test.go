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
	*httpAddr = "127.0.0.1:0"
	*rpcAddr = "127.0.0.1:0"
}

func startServer() *server {
	serverTestOnce.Do(func() {
		resetTestData()
		s, err := newServer()
		if err != nil {
			glog.Fatal(err)
		}
		engines := []storage.Engine{storage.NewInMem(storage.Attributes{}, 1<<20)}
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
		key       string             // data directory
		expAttrs  storage.Attributes // attributes for engine
		wantError bool               // do we expect an error from this key?
		isMem     bool               // is the engine in-memory?
	}{
		{"mem=1000", storage.Attributes([]string{"mem"}), false, true},
		{"ssd=1000", storage.Attributes([]string{"ssd"}), false, true},
		{"ssd=/tmp/.foobar", storage.Attributes([]string{"ssd"}), false, false},
		{"hdd=/tmp/.foobar2", storage.Attributes([]string{"hdd"}), false, false},
		{"mem=/tmp/.foobar3", storage.Attributes([]string{"mem"}), false, false},
		{"abc=/tmp/.foobar4", storage.Attributes([]string{"abc"}), false, false},
		{"hdd,7200rpm=/tmp/.foobar5", storage.Attributes([]string{"hdd", "7200rpm"}), false, false},
		{"hdd=/dev/null", storage.Attributes{}, true, false},
		{"", storage.Attributes{}, true, false},
		{"  ", storage.Attributes{}, true, false},
		{"arbitrarystring", storage.Attributes{}, true, false},
		{"mem=", storage.Attributes{}, true, false},
		{"ssd=", storage.Attributes{}, true, false},
		{"hdd=", storage.Attributes{}, true, false},
	}
	for _, spec := range testCases {
		engine, err := initEngine(spec.key)
		if err == nil {
			if spec.wantError {
				t.Fatalf("invalid engine spec '%v' erroneously accepted: %+v", spec.key, spec)
			}
			if engine.Attrs().SortedString() != spec.expAttrs.SortedString() {
				t.Errorf("wrong engine attributes, expected %v but got %v: %+v", spec.expAttrs, engine.Attrs(), spec)
			}
			_, ok := engine.(*storage.InMem)
			if spec.isMem != ok {
				t.Errorf("expected in memory? %b, got %b: %+v", spec.isMem, ok, spec)
			}
		} else if !spec.wantError {
			t.Errorf("expected no error, got %v: %+v", err, spec)
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
