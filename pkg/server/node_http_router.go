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
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// RemoteNodeID is used to look up remote nodeID values to route
// requests to from a request's query params or a Cookie.
const RemoteNodeID = "remote_node_id"

// nodeProxy is an HTTP handler that inspects the request's cookie
// header and query params. If the `RemoteNodeID` key is found in
// either location, it will proxy the request to nodeID set in the
// value associated with the key. Otherwise, the request is handled
// by this node as usual.
type nodeProxy struct {
	scheme     string
	gossip     *gossip.Gossip
	rpcContext *rpc.Context
	proxyCache reverseProxyCache
}

// nodeProxyHandler is expected to return responses that will be inspected
// by customers directly (for instance, by third party software that
// scrapes our metrics endpoint) and thus well-formed error responses
// are necessary.
func (np *nodeProxy) nodeProxyHandler(
	w http.ResponseWriter, r *http.Request, next http.HandlerFunc,
) {
	ctx := r.Context()
	nodeIDString, err := np.getNodeIDFromRequest(r)
	if err != nil {
		if errors.Is(err, ErrNoNodeID) {
			next(w, r)
			return
		}
		resetCookie(w)
		panic(errors.Wrap(err, "server: unexpected error reading nodeID from request"))
	}
	nodeID, local, err := parseNodeID(np.gossip, nodeIDString)
	if err != nil {
		httpErr := errors.Wrapf(err, "server: error parsing nodeID from request: %s", nodeIDString)
		log.Errorf(ctx, "%v", httpErr)
		resetCookie(w)
		// Users are expected to recover from this by formatting their
		// nodeID properly, hence the 4xx code.
		http.Error(w, httpErr.Error(), http.StatusBadRequest)
	}
	if local {
		next(w, r)
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
		log.Errorf(r.Context(), "%v", httpErr.Error())

		// Reset the cookie to `local` so the user session isn't stuck on a bad node
		resetCookie(w)
		// Users could recover from this by asking for a nodeID the cluster
		// knows about, hence the 4xx code.
		http.Error(w, httpErr.Error(), http.StatusBadRequest)
		return
	}
	proxy, ok := np.proxyCache.get(addr)
	if !ok {
		u := url.URL{
			Scheme: np.scheme,
			Path:   addr,
		}
		proxy = httputil.NewSingleHostReverseProxy(&u)

		// This client is initialized with the TLS config we require in
		// order to make requests to the HTTP server.
		httpClient, err := np.rpcContext.GetHTTPClient()
		if err != nil {
			resetCookie(w)
			panic(errors.Wrapf(err, "server: failed to get httpClient"))
		}
		proxy.Transport = httpClient.Transport
		np.proxyCache.put(addr, proxy)
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
	mu              syncutil.RWMutex
	proxiesByNodeID map[string]*httputil.ReverseProxy
}

func (c *reverseProxyCache) get(addr string) (*httputil.ReverseProxy, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.proxiesByNodeID[addr]
	return p, ok
}

func (c *reverseProxyCache) put(addr string, proxy *httputil.ReverseProxy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.proxiesByNodeID == nil {
		c.proxiesByNodeID = make(map[string]*httputil.ReverseProxy)
	}
	c.proxiesByNodeID[addr] = proxy
}
