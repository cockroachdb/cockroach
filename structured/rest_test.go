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

package structured

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
)

var (
	serverAddr string
	once       sync.Once
)

type testDB struct {
	sync.RWMutex
	kv map[string]interface{}
}

func (db *testDB) PutSchema(s *Schema) error {
	db.Lock()
	defer db.Unlock()
	db.kv["/"+s.Key] = s
	return nil
}

func (db *testDB) DeleteSchema(s *Schema) error {
	db.Lock()
	defer db.Unlock()
	delete(db.kv, "/"+s.Key)
	return nil
}

func (db *testDB) GetSchema(key string) (*Schema, error) {
	db.RLock()
	defer db.RUnlock()
	v, ok := db.kv["/"+key]
	if ok {
		return v.(*Schema), nil
	}
	return nil, nil
}

func newTestDB() *testDB {
	return &testDB{kv: map[string]interface{}{}}
}

func startServer(t *testing.T) {
	server := httptest.NewTLSServer(NewRESTServer(newTestDB()))
	serverAddr = server.Listener.Addr().String()
}

func TestGetPutDeleteSchema(t *testing.T) {
	once.Do(func() { startServer(t) })
	sch := Schema{
		Name: "Carl's Blankets",
		Key:  "foo",
	}
	marshalledBytes, err := json.Marshal(sch)
	if err != nil {
		t.Fatalf("could not marshal Schema: %v", err)
	}
	payload := bytes.NewBuffer(marshalledBytes)
	testCases := []struct {
		method     string
		path       string
		body       io.Reader
		statusCode int
		resp       []Schema
	}{
		{methodGet, "/schema/foo", nil, http.StatusNotFound, nil},
		{methodGet, "/schema/foo/bar", nil, http.StatusNotFound, nil},
		{methodGet, "/schema/foo/bar/baz", nil, http.StatusNotFound, nil},
		{methodGet, "/schema/foo/bar/baz/", nil, http.StatusBadRequest, nil},
		{methodPut, "/schema/foo", payload, http.StatusOK, nil},
		{methodGet, "/schema/foo", nil, http.StatusOK, []Schema{sch}},
		{methodDelete, "/schema/foo", nil, http.StatusOK, nil},
		{methodGet, "/schema/foo", nil, http.StatusNotFound, nil},
	}
	testContext := testutils.NewTestBaseContext()
	httpClient, err := testContext.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range testCases {
		addr := testContext.RequestScheme() + "://" + serverAddr + tc.path
		req, err := http.NewRequest(tc.method, addr, tc.body)
		if err != nil {
			t.Fatalf("[%s] %s: error creating request: %v", tc.method, tc.path, err)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("[%s] %s: error requesting %s: %s", tc.method, tc.path, addr, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != tc.statusCode {
			bytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("[%s] %s: could not read response body: %v", tc.method, tc.path, string(bytes))
				continue
			}
			t.Errorf("[%s] %s: unexpected response status code: expected %d, got %d. Body: %s", tc.method, tc.path, tc.statusCode, resp.StatusCode, string(bytes))
			continue
		}
		var resResp struct {
			Data []Schema
		}
		if err := json.NewDecoder(resp.Body).Decode(&resResp); err != nil {
			t.Errorf("[%s] %s: could not decode body into resourceResponse: %v", tc.method, tc.path, err)
			continue
		}
		if !reflect.DeepEqual(resResp.Data, tc.resp) {
			t.Errorf("[%s] %s: response data is not equal: expected %+v, got %+v", tc.method, tc.path, tc.resp, resResp.Data)
			continue
		}
	}
}

func TestNewResourceRequest(t *testing.T) {
	testCases := []struct {
		path        string
		resReq      *resourceRequest
		errExpected bool
	}{
		{"/schema/pdb", &resourceRequest{schemaKey: "pdb"}, false},
		{"/schema/pdb/us", &resourceRequest{schemaKey: "pdb", tableKey: "us"}, false},
		{"/schema/pdb/us/531", &resourceRequest{schemaKey: "pdb", tableKey: "us", primaryKey: "531"}, false},
		{"", nil, true},
		{"/schema", &resourceRequest{}, false},
		{"/schema/", &resourceRequest{}, false},
		{"/schema/pdb/us/431/fooooooo", nil, true},
		{"/schema/pdb?limit=100", &resourceRequest{schemaKey: "pdb", limit: 100}, false},
		{"/schema/pdb?offset=101", &resourceRequest{schemaKey: "pdb", offset: 101}, false},
		{"/schema/pdb?limit=hi", nil, true},
		{"/schema/pdb?offset=carl", nil, true},
		{"/schema/pdb?owner=spencer&name=carl&name=carlos&limit=100&offset=50", &resourceRequest{
			schemaKey: "pdb",
			limit:     100,
			offset:    50,
			params: map[string][]string{
				"owner": {"spencer"},
				"name":  {"carl", "carlos"},
			},
		}, false},
	}
	for _, tc := range testCases {
		req, err := http.NewRequest("GET", "http://foo.com"+tc.path, nil)
		if err != nil {
			t.Fatalf("error creating request: %v", err)
		}
		resReq, err := newResourceRequest(req)
		if tc.errExpected && err == nil {
			t.Errorf("expected error from path %q", tc.path)
		}
		if err != nil {
			if !tc.errExpected {
				t.Fatalf("error creating resource request: %v", err)
			}
		}
		if !reflect.DeepEqual(resReq, tc.resReq) {
			t.Errorf("expected resourceRequest to be %+v, got %+v", tc.resReq, resReq)
		}
	}
}
