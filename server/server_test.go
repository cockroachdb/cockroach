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
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	s              *TestServer
	serverTestOnce sync.Once
)

// Start a test server. The server will be initialized with an
// in-memory engine and will execute a split at key "m" so that
// it will end up having two logical ranges.
func startServer(t *testing.T) *TestServer {
	serverTestOnce.Do(func() {
		s = StartTestServer(t)
		log.Infof("Test server listening on http: %s, rpc: %s", s.HTTPAddr, s.RPCAddr)
	})
	return s
}

// createTestConfigFile creates a temporary file and writes the
// testConfig yaml data to it. The caller is responsible for
// removing it. Returns the filename for a subsequent call to
// os.Remove().
func createTestConfigFile(body string) string {
	f, err := ioutil.TempFile("", "test-config")
	if err != nil {
		log.Fatalf("failed to open temporary file: %v", err)
	}
	defer f.Close()
	f.Write([]byte(body))
	return f.Name()
}

// createTempDirs creates "count" temporary directories and returns
// the paths to each as a slice.
func createTempDirs(count int, t *testing.T) []string {
	tmp := make([]string, count)
	for i := 0; i < count; i++ {
		var err error
		if tmp[i], err = ioutil.TempDir("", "_server_test"); err != nil {
			t.Fatal(err)
		}
	}
	return tmp
}

// resetTestData recursively removes all files written to the
// directories specified as parameters.
func resetTestData(dirs []string) {
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
}

// TestInitEngine tests whether the data directory string is parsed correctly.
func TestInitEngine(t *testing.T) {
	tmp := createTempDirs(5, t)
	defer resetTestData(tmp)

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
		engines, err := initEngines(spec.key)
		if err == nil {
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
			_, ok := e.(*engine.InMem)
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
	tmp := createTempDirs(2, t)
	defer resetTestData(tmp)

	stores := fmt.Sprintf("mem=1000,mem:ddr3=1000,ssd=%s,hdd:7200rpm=%s", tmp[0], tmp[1])
	expEngines := []struct {
		attrs proto.Attributes
		isMem bool
	}{
		{proto.Attributes{Attrs: []string{"mem"}}, true},
		{proto.Attributes{Attrs: []string{"mem", "ddr3"}}, true},
		{proto.Attributes{Attrs: []string{"ssd"}}, false},
		{proto.Attributes{Attrs: []string{"hdd", "7200rpm"}}, false},
	}

	engines, err := initEngines(stores)
	if err != nil {
		t.Fatal(err)
	}
	if len(engines) != len(expEngines) {
		t.Errorf("number of engines parsed %d != expected %d", len(engines), len(expEngines))
	}
	for i, e := range engines {
		if e.Attrs().SortedString() != expEngines[i].attrs.SortedString() {
			t.Errorf("wrong engine attributes, expected %v but got %v: %+v", expEngines[i].attrs, e.Attrs(), expEngines[i])
		}
		_, ok := e.(*engine.InMem)
		if expEngines[i].isMem != ok {
			t.Errorf("expected in memory? %t, got %t: %+v", expEngines[i].isMem, ok, expEngines[i])
		}
	}
}

// TestHealthz verifies that /_admin/healthz does, in fact, return "ok"
// as expected.
func TestHealthz(t *testing.T) {
	startServer(t)
	url := "http://" + s.HTTPAddr + "/_admin/healthz"
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
	startServer(t)
	client := http.Client{
		Transport: &http.Transport{
			Proxy:              http.ProxyFromEnvironment,
			DisableCompression: true,
		},
	}
	req, err := http.NewRequest("GET", "http://"+s.HTTPAddr+"/_admin/healthz", nil)
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

func TestMultiRangeScanDeleteRange(t *testing.T) {
	ts := StartTestServer(t)
	ds := kv.NewDistSender(ts.Gossip())

	if err := ts.node.db.Call(proto.AdminSplit,
		&proto.AdminSplitRequest{
			RequestHeader: proto.RequestHeader{
				Key: proto.Key("m"),
			},
			SplitKey: proto.Key("m"),
		}, &proto.AdminSplitResponse{}); err != nil {
		log.Fatal(err)
	}
	// TODO(Tobias): Bogus GET with a key range, want error.
	writes := []proto.Key{proto.Key("a"), proto.Key("z")}
	var call *client.Call
	for i, k := range writes {
		call = &client.Call{
			Method: proto.Put,
			Args:   proto.PutArgs(k, k),
			Reply:  &proto.PutResponse{},
		}
		call.Args.Header().User = storage.UserRoot
		ds.Send(call)
		if err := call.Reply.Header().GoError(); err != nil {
			t.Fatal(err)
		}
		scan := &client.Call{
			Method: proto.Scan,
			Args:   proto.ScanArgs(writes[0], writes[len(writes)-1].Next(), 0),
			Reply:  &proto.ScanResponse{},
		}
		scan.Args.Header().User = storage.UserRoot
		// TODO(Tobias): Why is this necessary? If I skip this,
		// then the Scan() will end up reading with a timestamp
		// that's slightly behind the one of the Puts above,
		// and not see the inserts.
		scan.Args.Header().Timestamp = call.Reply.Header().Timestamp
		ds.Send(scan)
		if err := scan.Reply.Header().GoError(); err != nil {
			t.Fatal(err)
		}
		if rows := scan.Reply.(*proto.ScanResponse).Rows; len(rows) != i+1 {
			t.Fatalf("expected %d rows, but got %d", i+1, len(rows))
		}
	}
	del := &client.Call{
		Method: proto.DeleteRange,
		Args: &proto.DeleteRangeRequest{
			RequestHeader: proto.RequestHeader{
				User:   storage.UserRoot,
				Key:    writes[0],
				EndKey: proto.Key(writes[len(writes)-1]).Next(),
			},
		},
		Reply: &proto.DeleteRangeResponse{},
	}
	ds.Send(del)
	if err := del.Reply.Header().GoError(); err != nil {
		t.Fatal(err)
	}
	scan := &client.Call{
		Method: proto.Scan,
		Args:   proto.ScanArgs(writes[0], writes[len(writes)-1].Next(), 0),
		Reply:  &proto.ScanResponse{},
	}
	scan.Args.Header().User = storage.UserRoot
	// TODO(Tobias): ditto.
	scan.Args.Header().Timestamp = del.Reply.Header().Timestamp
	ds.Send(scan)
	if err := scan.Reply.Header().GoError(); err != nil {
		t.Fatal(err)
	}
	if rows := scan.Reply.(*proto.ScanResponse).Rows; len(rows) > 0 {
		t.Fatalf("scan after delete returned rows: %v", rows)
	}
}
