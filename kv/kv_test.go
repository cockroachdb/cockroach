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

package kv

import (
	"net/http"
	"net/http/httptest"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/storage"
)

var (
	server *kvTestServer
)

type kvTestServer struct {
	db         DB
	rest       *RESTServer
	httpServer *httptest.Server
}

func startNewServer() *kvTestServer {
	server = &kvTestServer{}
	g := gossip.New()
	localDB := NewLocalDB()
	engine := storage.NewInMem(storage.Attributes{}, 1<<20)
	store := storage.NewStore(engine, g)
	_, err := store.CreateRange(storage.KeyMin, storage.KeyMax, []storage.Replica{storage.Replica{RangeID: 1}})
	if err != nil {
		panic(err)
	}
	localDB.AddStore(store)
	BootstrapConfigs(localDB)
	server.db = localDB
	server.rest = NewRESTServer(server.db)
	server.httpServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		server.rest.HandleAction(w, r)
	}))
	return server
}
