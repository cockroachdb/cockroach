// Copyright 2021 The Cockroach Authors.
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
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// nodeRouterMiddleware wraps a handler with middleware that inspects
// a cookie named `node_id` before handling the request using `next`.
// If the cookie is present and matches a NodeID that's not the current
// node, the request is proxied via HTTP to the node identified by the
// NodeID.
func (s *Server) nodeRouterMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		cookie, err := r.Cookie("node_id")
		if err == nil && cookie.Value != "" && cookie.Value != "local" {
			nodeIDStr := cookie.Value
			nodeIDInt, err := strconv.ParseInt(nodeIDStr, 10, 32)
			if err != nil {
				log.Errorf(ctx, "server: unable to parse nodeID from cookie node_id: %v", err)
				http.Error(w, err.Error(), 400)
				return
			}
			nodeID := roachpb.NodeID(nodeIDInt)
			if nodeID != s.gossip.NodeID.Get() {
				s.routeToNode(w, r, nodeID)
				return
			}
		}
		next.ServeHTTP(w, r)
	}
}

func (s *Server) routeToNode(w http.ResponseWriter, r *http.Request, nodeID roachpb.NodeID) {
	ctx := r.Context()

	addr, err := s.gossip.GetNodeIDHTTPAddress(nodeID)
	if err != nil {
		log.Errorf(ctx, "server: node router failed to get node address: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	log.Infof(ctx, "parsing URL from %s", addr)
	// TODO(davidh): https for secure servers
	u, err := url.Parse("http://" + addr.String())
	if err != nil {
		log.Errorf(ctx, "server: node router failed to parse node address: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	log.Infof(ctx, "proxying request to %s", u)
	cookie, err := r.Cookie("node_id")
	if err != nil {
		log.Errorf(ctx, "server: expected cookie missing in request", err)
		http.Error(w, err.Error(), 500)
		return
	}
	// Clear node_id cookie so the request doesn't proxy forever
	cookie.Value = "local"
	httputil.NewSingleHostReverseProxy(u).ServeHTTP(w, r)
}
