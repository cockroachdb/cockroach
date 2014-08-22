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
// Author: Shawn Morel (shawn@strangemond.com)

package server

import (
	"encoding/json"
	"net/http"

	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/storage"
)

const (
	// statusKeyPrefix is the root of the RESTful cluster statistics and metrics API.
	statusKeyPrefix = "/_status/"

	// statusNodesKeyPrefix exposes status for each of the nodes the cluster.
	// GETing statusNodesKeyPrefix will list all nodes.
	// Individual node status can be queried at statusNodesKeyPrefix/NodeID.
	statusNodesKeyPrefix = statusKeyPrefix + "nodes/"

	// statusGossipKeyPrefix exposes a view of the gossip network.
	statusGossipKeyPrefix = statusKeyPrefix + "gossip"

	// statusStoresKeyPrefix exposes status for each store.
	statusStoresKeyPrefix = statusKeyPrefix + "stores/"

	// statusTransactionsKeyPrefix exposes transaction statistics.
	statusTransactionsKeyPrefix = statusKeyPrefix + "txns/"

	// statusLocalKeyPrefix exposes the status of the node serving the request.
	// This is equivalent to GETing statusNodesKeyPrefix/<current-node-id>.
	// Useful for debuging nodes that aren't communicating with the cluster properly.
	statusLocalKeyPrefix = statusKeyPrefix + "local"
)

// A statusServer provides a RESTful status API.
type statusServer struct {
	db storage.DB
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(db storage.DB) *statusServer {
	return &statusServer{
		db: db,
	}
}

// TODO(shawn) lots of implementing - setting up a skeleton for hack week.

// handleStatus handles GET requests for cluster status.
func (s *statusServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cluster := &status.Cluster{}

	b, err := json.Marshal(cluster)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

// handleNodeStatus handles GET requests for node status.
func (s *statusServer) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// TODO(shawn) parse node-id in path

	nodes := &status.NodeList{}

	b, err := json.Marshal(nodes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

// handleGossipStatus handles GET requests for gossip network status.
func (s *statusServer) handleGossipStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	w.Write([]byte(`{}`))
}

// handleStoresStatus handles GET requests for store status.
func (s *statusServer) handleStoresStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	w.Write([]byte(`{"stores": []}`))
}

// handleTransactionStatus handles GET requests for transaction status.
func (s *statusServer) handleTransactionStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	w.Write([]byte(`{"transactions": []}`))
}

// handleLocalStatus handles GET requests for local-node status.
func (s *statusServer) handleLocalStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	w.Write([]byte(`{}`))
}
