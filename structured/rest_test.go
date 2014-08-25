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
	"net/http"
	"reflect"
	"testing"
)

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
		{"/schema", nil, true},
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
				"owner": []string{"spencer"},
				"name":  []string{"carl", "carlos"},
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
