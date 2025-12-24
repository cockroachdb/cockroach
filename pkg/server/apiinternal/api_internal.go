// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

import (
	"context"
	"net/http"
	"net/url"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// httpMethod represents HTTP methods supported by the API.
type httpMethod string

// Supported HTTP methods for the internal API.
const (
	GET  httpMethod = http.MethodGet
	POST httpMethod = http.MethodPost
)

var decoder = func() *schema.Decoder {
	d := schema.NewDecoder()
	d.SetAliasTag("json")
	d.IgnoreUnknownKeys(true)
	return d
}()

// route defines a REST endpoint with its handler and HTTP method.
type route struct {
	method  httpMethod
	path    string
	handler http.HandlerFunc
}

// apiInternalServer provides REST endpoints that proxy to RPC services. It
// serves as a bridge between HTTP REST clients and internal RPC services.
type apiInternalServer struct {
	mux        *mux.Router
	status     serverpb.RPCStatusClient
	admin      serverpb.RPCAdminClient
	timeseries tspb.RPCTimeSeriesClient
}

// NewAPIInternalServer creates a new REST API server that proxies to internal
// RPC services. It establishes connections to the RPC services and registers
// all REST endpoints.
func NewAPIInternalServer(
	ctx context.Context, nd rpcbase.NodeDialer, localNodeID roachpb.NodeID, cs *cluster.Settings,
) (*apiInternalServer, error) {
	status, err := serverpb.DialStatusClient(nd, ctx, localNodeID, cs)
	if err != nil {
		return nil, err
	}

	admin, err := serverpb.DialAdminClient(nd, ctx, localNodeID, cs)
	if err != nil {
		return nil, err
	}

	timeseries, err := rpcbase.DialRPCClient(
		nd,
		ctx,
		localNodeID,
		rpcbase.DefaultClass,
		tspb.NewGRPCTimeSeriesClientAdapter,
		tspb.NewDRPCTimeSeriesClientAdapter,
		cs,
	)
	if err != nil {
		return nil, err
	}
	r := &apiInternalServer{
		status:     status,
		admin:      admin,
		timeseries: timeseries,
		mux:        mux.NewRouter(),
	}

	r.registerStatusRoutes()
	r.registerAdminRoutes()
	r.registerTimeSeriesRoutes()

	return r, nil
}

// ServeHTTP implements http.Handler interface
func (r *apiInternalServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

// createHandler creates an HTTP handler function that proxies requests to the
// given RPC method.
func createHandler[TReq, TResp protoutil.Message](
	rpcMethod func(context.Context, TReq) (TResp, error),
) http.HandlerFunc {
	var zero TReq
	msgName := proto.MessageName(zero)
	msgType := proto.MessageType(msgName)
	if msgType == nil {
		panic(errors.AssertionFailedf("failed to determine request protobuf type: %s", msgName))
	}
	return func(w http.ResponseWriter, req *http.Request) {
		newReq := reflect.New(msgType.Elem()).Interface().(TReq)
		if err := executeRPC(w, req, rpcMethod, newReq); err != nil {
			ctx := req.Context()
			apiutil.WriteHTTPError(ctx, w, req, err)
		}
	}
}

// executeRPC is a generic function that handles the common pattern of:
// 1. Decoding HTTP request parameters (query string for GET, body for POST)
// 2. Forwarding HTTP auth information to the RPC context
// 3. Calling the RPC method
// 4. Writing the response back to the HTTP client
//
// This eliminates boilerplate code across all endpoint handlers.
func executeRPC[TReq, TResp protoutil.Message](
	w http.ResponseWriter,
	req *http.Request,
	rpcMethod func(context.Context, TReq) (TResp, error),
	rpcReq TReq,
) error {
	ctx := req.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, req)
	ctx = rpc.MarkDRPCGatewayRequest(ctx)

	if err := decoder.Decode(rpcReq, req.URL.Query()); err != nil {
		return err
	}
	if err := decodePathVars(rpcReq, mux.Vars(req)); err != nil {
		return err
	}
	// For POST requests, decode the request body (JSON or protobuf)
	if req.Method == http.MethodPost {
		if err := apiutil.DecodeRequest(req, rpcReq); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to decode request body: %v", err)
		}
	}

	resp, err := rpcMethod(ctx, rpcReq)
	if err != nil {
		return err
	}
	return apiutil.WriteResponse(ctx, w, req, http.StatusOK, resp)
}

func decodePathVars[TReq protoutil.Message](rpcReq TReq, vars map[string]string) error {
	pathParams := make(url.Values)
	for k, v := range vars {
		pathParams[k] = []string{v}
	}
	return decoder.Decode(rpcReq, pathParams)
}
