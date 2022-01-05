// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Dat specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RemoteNodeID is used to look up remote nodeID values to route
// requests to from a request's query params or a Cookie.
const RemoteNodeID = "remote_node_id"

// nodeProxy is an HTTP handler that inspects the request's cookie
// header and query params. If the `RemoteNodeID` key is found in
// either location, it will proxy the request to nodeID set in the
// value associated with the key. Otherwise, the requested is handled
// by this node as usual.
type nodeProxy struct {
	gossip            *gossip.Gossip
	rpcContext        *rpc.Context
	insecure          bool
	proxyCache        reverseProxyCache
	serverHandler     http.HandlerFunc
	disableTLSForHTTP bool
}

var _ http.Handler = &nodeProxy{}

// ServeHTTP is expected to return responses that will be inspected
// by customers directly (for instance, by third party software that
// scrapes our metrics endpoint) and thus well-formed error responses
// are necessary.
func (np *nodeProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodeIDString, err := np.getNodeIDFromRequest(r)
	if err != nil {
		if errors.Is(err, ErrNoNodeID) {
			np.serverHandler.ServeHTTP(w, r)
			return
		}
		resetCookie(w)
		panic(errors.Wrap(err, "server: unexpected error reading nodeID from request"))
	}
	nodeID, local, err := parseNodeID(np.gossip, nodeIDString)
	if err != nil {
		httpErr := errors.Wrapf(err, "server: error parsing nodeID from request: %s", nodeIDString)
		log.Error(ctx, httpErr.Error())
		resetCookie(w)
		// Users are expected to recover from this by formatting their
		// nodeID properly, hence the 4xx code.
		http.Error(w, httpErr.Error(), http.StatusBadRequest)
	}
	if local {
		np.serverHandler.ServeHTTP(w, r)
		return
	}
	np.routeToNode(w, r, nodeID)
}

// ErrNoNodeID is returned by getNodeIDFromRequest if the request
// contains no nodeID to proxy to.
var ErrNoNodeID = errors.New("http: nodeID not present in request")

func (np *nodeProxy) getNodeIDFromRequest(r *http.Request) (string, error) {
	queryParam := r.URL.Query().Get(RemoteNodeID)
	if queryParam != "" {
		return queryParam, nil
	}
	cookie, err := r.Cookie(RemoteNodeID)
	if err != nil {
		if errors.Is(err, http.ErrNoCookie) {
			return "", ErrNoNodeID
		}
		// This should never trigger based on the implementation of
		// `Cookie` in stdlib.
		return "", errors.Wrapf(err, "server: error decoding node routing cookie")
	}
	return cookie.Value, nil
}

// routeToNode will proxy the given request to the node identified by
// nodeID.
func (np *nodeProxy) routeToNode(w http.ResponseWriter, r *http.Request, nodeID roachpb.NodeID) {
	addr, err := np.gossip.GetNodeIDHTTPAddress(nodeID)
	if err != nil {
		httpErr := errors.Wrapf(err, "unable to get address for nodeID: %d", nodeID)
		log.Error(r.Context(), httpErr.Error())

		// Reset the cookie to `local` so the user session isn't stuck on a bad node
		resetCookie(w)
		// Users could recover from this by asking for a nodeID the cluster
		// knows about, hence the 4xx code.
		http.Error(w, httpErr.Error(), http.StatusBadRequest)
		return
	}
	proxy, ok := np.proxyCache.get(*addr)
	if !ok {
		prefix := "https://"
		if np.insecure || np.disableTLSForHTTP {
			prefix = "http://"
		}
		u, err := url.Parse(prefix + addr.String())
		if err != nil {
			resetCookie(w)
			panic(errors.Wrapf(err, "server: node router failed to parse node address %s", addr.String()))
		}
		proxy = httputil.NewSingleHostReverseProxy(u)

		// This client is initialized with the TLS config we require in
		// order to make requests to the HTTP server.
		httpClient, err := np.rpcContext.GetHTTPClient()
		if err != nil {
			resetCookie(w)
			panic(errors.Wrapf(err, "server: failed to get httpClient"))
		}
		proxy.Transport = httpClient.Transport
		np.proxyCache.put(*addr, proxy)
	}
	proxy.ServeHTTP(w, r)
}

func resetCookie(w http.ResponseWriter) {
	w.Header().Set("set-cookie", fmt.Sprintf("%s=", RemoteNodeID))
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
