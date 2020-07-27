// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// These helpers live in test_utils.go, rather than a more idiomatic
// utils_test.go so that they can be referenced outside of this package (e.g. by
// roachtests).

// NewMockServer creates an HTTP server which can be used when to perform
// BACKUP/RESTOREs against. It also accepts an `interception` function which is
// run on every request that the server handles.
func NewMockServer(interception func(r *http.Request)) *httptest.Server {
	bulkFiles := make(map[string][]byte)
	lock := syncutil.Mutex{}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()

		if interception != nil {
			interception(r)
		}

		localFile := r.URL.Path
		switch r.Method {
		case "PUT":
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			bulkFiles[localFile] = b
			w.WriteHeader(201)
		case "GET", "HEAD":
			b, ok := bulkFiles[localFile]
			if !ok {
				http.Error(w, fmt.Sprintf("not found: %s", localFile), 404)
				return
			}
			_, _ = w.Write(b)
		case "DELETE":
			delete(bulkFiles, localFile)
			w.WriteHeader(204)
		default:
			http.Error(w, "unsupported method", 400)
		}
	}))
}
