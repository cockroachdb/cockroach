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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/golang/glog"
)

// startAdminServer launches a new admin server using minimal engine
// and local database setup. Returns the new http test server, which
// should be cleaned up by caller via httptest.Server.Close(). The
// Cockroach KV client address is set to the address of the test server.
func startAdminServer() *httptest.Server {
	db, err := BootstrapCluster("cluster-1", engine.NewInMem(engine.Attributes{}, 1<<20))
	if err != nil {
		glog.Fatal(err)
	}
	admin := newAdminServer(db)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admin.handleZoneAction(w, r)
	}))
	if strings.HasPrefix(httpServer.URL, "http://") {
		*kv.Addr = strings.TrimPrefix(httpServer.URL, "http://")
	} else if strings.HasPrefix(httpServer.URL, "https://") {
		*kv.Addr = strings.TrimPrefix(httpServer.URL, "https://")
	}
	return httpServer
}
