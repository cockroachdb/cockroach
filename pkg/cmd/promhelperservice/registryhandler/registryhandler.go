// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Handler for registering all URLs with their functions

package registryhandler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/logging"
)

type HTTPMethod string

const (
	// HTTP methods
	POST   HTTPMethod = http.MethodPost
	GET    HTTPMethod = http.MethodGet
	PUT    HTTPMethod = http.MethodPut
	PATCH  HTTPMethod = http.MethodPatch
	DELETE HTTPMethod = http.MethodDelete
)

// URLRegistry gives a generic way of registering the urls with http methods to a handler function
type URLRegistry struct {
	Url        string
	Method     HTTPMethod
	HandleFunc http.HandlerFunc
	Skip       bool // to skip a specific registration
}

// Registry is the interface to be implemented for registering handlers
type Registry interface {
	// GetRegistrations returns the list of URLRegistry for URL registration
	GetRegistrations(ctx context.Context) []*URLRegistry
}

// proxyHandler is responsible for handling the request and redirect to the right http handler based on the http method
type proxyHandler struct {
	url string
	// methodToHandlerMap maintains a map of the http method to the request handler function
	methodToHandlerMap map[HTTPMethod]http.HandlerFunc
}

// register registers the http method to the redirect http handler
func (h *proxyHandler) register(m HTTPMethod, handler http.HandlerFunc) error {
	if _, ok := h.methodToHandlerMap[m]; !ok {
		h.methodToHandlerMap[m] = handler
		return nil
	}
	return fmt.Errorf("http method %v is already registered for the url %s", m, h.url)
}

// handleProxy is registered as a proxy to the http.HandleFunc.
// This redirects to the right handler based on the http method
func (h *proxyHandler) handleProxy(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	if handler, ok := h.methodToHandlerMap[HTTPMethod(method)]; ok {
		handler(w, r)
		return
	}
	// if the http method is not registered for the handler
	w.WriteHeader(http.StatusNotFound)
}

// newProxyHandler returns a new proxyHandler
func newProxyHandler(url string) *proxyHandler {
	return &proxyHandler{url: url, methodToHandlerMap: make(map[HTTPMethod]http.HandlerFunc)}
}

// RegisterHandlers reads all the URLRegistry and registers the proxy handler
// the purpose of proxy handler is to abstract out the HTTP Method check. With this
// the same URL can be configured for different HTTP Methods will different handlers.
func RegisterHandlers(ctx context.Context, r Registry) error {
	// maintain a map of the url to the proxy handler.
	// this is needed to ensure that the same proxyHandler is used for all http methods for the same url
	handlerMap := make(map[string]*proxyHandler)
	log := logging.MakeLogger(ctx, "registry handler")
	for _, registry := range r.GetRegistrations(ctx) {
		if registry.Skip {
			// purpose is to skip a specific URL registration - useful for unregistering a URL without removing a lot od code
			log.Infof("skipping %s with http method %s\n", registry.Url, registry.Method)
			continue
		}
		var ph *proxyHandler
		var ok bool
		if ph, ok = handlerMap[registry.Url]; !ok {
			// url was never registered
			ph = newProxyHandler(registry.Url)
			handlerMap[registry.Url] = ph
			// register the url
			http.HandleFunc(registry.Url, ph.handleProxy)
		}
		log.Infof("registering %s with http method %s\n", registry.Url, registry.Method)
		// register the function against the http method
		if err := ph.register(registry.Method, registry.HandleFunc); err != nil {
			return err
		}
	}
	return nil
}
