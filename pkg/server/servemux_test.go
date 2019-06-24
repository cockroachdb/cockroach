// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
