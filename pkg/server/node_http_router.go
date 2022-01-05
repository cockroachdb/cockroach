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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// NodeRoutingIDKey is used to lookup remote nodeID values to route
// requests to from a request's query params or a Cookie.
const NodeRoutingIDKey = "crdb_node_id"

var proxyCache reverseProxyCache

// nodeRouterMiddleware wraps a handler with middleware that inspects
// a query param and a cookie named with the `NodeRoutingIDKey` name
// before handling the request using `next`. If the param is present
// and matches a NodeID that's not the current node, the request is
// proxied via HTTP to the node identified by the NodeID. The query
// param takes precedence over the cookie.
func (s *Server) nodeRouterMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		queryParam := r.URL.Query().Get(NodeRoutingIDKey)
		if queryParam != "" {
			s.routeToNodeIDString(w, r, queryParam, next)
			return
		}
		cookie, err := r.Cookie(NodeRoutingIDKey)
		if err != nil {
			if errors.Is(err, http.ErrNoCookie) {
				// Missing cookie means we serve the request as local
				next.ServeHTTP(w, r)
				return
			}
			// This should never trigger based on the implementation of
			// `Cookie` in stdlib
			log.Errorf(ctx, "server: error decoding node routing cookie: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		s.routeToNodeIDString(w, r, cookie.Value, next)
	}
}

func (s *Server) routeToNodeIDString(
	w http.ResponseWriter, r *http.Request, nodeIDString string, next http.HandlerFunc,
) {
	ctx := r.Context()
	nodeID, local, err := s.status.parseNodeID(nodeIDString)
	if err != nil {
		log.Errorf(ctx, "server: unable to parse nodeID from cookie %s: %v", NodeRoutingIDKey, err)
		http.Error(w, err.Error(), 400)
		return
	}
	if local {
		next.ServeHTTP(w, r)
		return
	}
	s.routeToNode(w, r, nodeID)
}

// reverseProxyCache implements a shared cache of `ReverseProxy`
// instances keyed by their addresses so that we can effectively
// re-use proxies when routing multiple requests to the same node.
type reverseProxyCache struct {
	mu              sync.Mutex
	proxiesByNodeID map[util.UnresolvedAddr]*httputil.ReverseProxy
}

func (c *reverseProxyCache) get(addr util.UnresolvedAddr) (*httputil.ReverseProxy, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.proxiesByNodeID[addr]
	return p, ok
}

func (c *reverseProxyCache) put(addr util.UnresolvedAddr, proxy *httputil.ReverseProxy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.proxiesByNodeID == nil {
		c.proxiesByNodeID = make(map[util.UnresolvedAddr]*httputil.ReverseProxy)
	}
	c.proxiesByNodeID[addr] = proxy
}

func (s *Server) routeToNode(w http.ResponseWriter, r *http.Request, nodeID roachpb.NodeID) {
	ctx := r.Context()

	addr, err := s.gossip.GetNodeIDHTTPAddress(nodeID)
	if err != nil {
		log.Errorf(ctx, "server: node router failed to get node address: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	proxy, ok := proxyCache.get(*addr)
	if ok {
		proxy.ServeHTTP(w, r)
		return
	}

	prefix := "https://"
	if s.Insecure() {
		prefix = "http://"
	}
	u, err := url.Parse(prefix + addr.String())
	if err != nil {
		log.Errorf(ctx, "server: node router failed to parse node address: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	proxy = httputil.NewSingleHostReverseProxy(u)

	// This client is initialized with the TLS config we need to make
	// requests to the HTTP server so we copy over its Transport
	httpClient, err := s.rpcContext.GetHTTPClient()
	if err != nil {
		log.Errorf(ctx, "server: failed to get http client: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	proxy.Transport = httpClient.Transport

	proxyCache.put(*addr, proxy)
	proxy.ServeHTTP(w, r)
}
