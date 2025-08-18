// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gorilla/mux"
)

// restStatusServer provides REST endpoints that directly call Status RPC methods
type restStatusServer struct {
	status serverpb.StatusServer
	mux    *mux.Router
}

// writeProtoJSONResponse writes a protobuf response as JSON using the same
// marshaler as gRPC Gateway (protoutil.JSONPb) to ensure identical formatting
func writeProtoJSONResponse(
	ctx context.Context, w http.ResponseWriter, code int, payload interface{},
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	// Use the same JSONPb marshaler that gRPC Gateway uses
	marshaler := &protoutil.JSONPb{
		EnumsAsInts:  true,
		EmitDefaults: true,
		Indent:       "  ",
	}

	res, err := marshaler.Marshal(payload)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	_, _ = w.Write(res)
}

// newRESTStatusServer creates a new REST API server for status endpoints
func newRESTStatusServer(status serverpb.StatusServer) *restStatusServer {
	r := &restStatusServer{
		status: status,
		mux:    mux.NewRouter(),
	}
	r.registerRoutes()
	return r
}

// registerRoutes sets up all the REST endpoints
func (r *restStatusServer) registerRoutes() {
	// Define all the status endpoints we want to replace from gRPC Gateway
	routes := []struct {
		path    string
		handler http.HandlerFunc
		methods []string
	}{
		// Node endpoints
		{"/drpc/_status/nodes", r.nodes, []string{"GET"}},

		// Range endpoints
		{"/drpc/_status/ranges", r.ranges, []string{"GET"}},
	}

	// Register each route
	for _, route := range routes {
		r.mux.HandleFunc(route.path, route.handler).Methods(route.methods...)
	}
}

// ServeHTTP implements http.Handler interface
func (r *restStatusServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

// nodes handles GET /_status/nodes
func (r *restStatusServer) nodes(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, req)

	resp, err := r.status.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	writeProtoJSONResponse(ctx, w, http.StatusOK, resp)
}

// ranges handles GET /_status/ranges
func (r *restStatusServer) ranges(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, req)

	nodeID := req.URL.Query().Get("node_id")
	if nodeID == "" {
		nodeID = "local"
	}

	resp, err := r.status.Ranges(ctx, &serverpb.RangesRequest{NodeId: nodeID})
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	writeProtoJSONResponse(ctx, w, http.StatusOK, resp)
}
