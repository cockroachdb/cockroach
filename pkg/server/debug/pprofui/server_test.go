// Copyright 2018 The Cockroach Authors.
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

package pprofui

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServer(t *testing.T) {
	storage := NewMemStorage(1, 0)
	s := NewServer(storage)

	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r := httptest.NewRequest("GET", "/heap/", nil)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)

			if a, e := w.Code, http.StatusTemporaryRedirect; a != e {
				t.Fatalf("expected status code %d, got %d", e, a)
			}

			loc := w.Result().Header.Get("Location")

			if a, e := loc, fmt.Sprintf("/heap/%d/flamegraph", i+1); a != e {
				t.Fatalf("expected location header %s, but got %s", e, a)
			}

			r = httptest.NewRequest("GET", loc, nil)
			w = httptest.NewRecorder()

			s.ServeHTTP(w, r)

			if a, e := w.Code, http.StatusOK; a != e {
				t.Fatalf("expected status code %d, got %d", e, a)
			}

			if a, e := w.Body.String(), "pprof</a></h1>"; !strings.Contains(a, e) {
				t.Fatalf("body does not contain %q: %v", e, a)
			}
		})
		if a, e := len(storage.mu.records), 1; a != e {
			t.Fatalf("storage did not expunge records; have %d instead of %d", a, e)
		}
	}
}
