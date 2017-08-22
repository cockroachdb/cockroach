// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestHandleVModule(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ClusterSettings = cluster.MakeTestingClusterSettings()
	for _, tc := range []struct {
		vmodule bool
		path    string
		code    int // http response code
	}{
		{true, "storage=2", http.StatusOK},
		{false, "storage=2", http.StatusForbidden},
		{true, "storage=something", http.StatusInternalServerError},
		{false, "storage=something", http.StatusForbidden},
	} {
		t.Run("", func(t *testing.T) {
			ClusterSettings.DebugVModule.Override(tc.vmodule)
			req, err := http.NewRequest("GET", vmodulePrefix+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			rr := httptest.NewRecorder()
			http.HandlerFunc(handleVModule).ServeHTTP(rr, req)
			if tc.code != rr.Code {
				t.Errorf("handler returned wrong status code: expected %d got %d with body %s",
					tc.code, rr.Code, rr.Body)
			}
		})
	}
}
