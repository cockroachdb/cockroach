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

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestServeMuxConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const duration = 20 * time.Millisecond
	start := timeutil.Now()

	// TODO(peter): This test reliably fails using http.ServeMux with a
	// "concurrent map read and write error" on go1.10. The bug in http.ServeMux
	// is fixed in go1.11.
	var mux safeServeMux
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
		for i := 1; timeutil.Since(start) < duration; i++ {
			mux.Handle(fmt.Sprintf("/%d", i), f)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 1; timeutil.Since(start) < duration; i++ {
			r := &http.Request{
				Method: "GET",
				URL: &url.URL{
					Path: "/",
				},
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
		}
	}()

	wg.Wait()
}
