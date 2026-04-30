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
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"storj.io/drpc"
)

var decoder = func() *schema.Decoder {
	d := schema.NewDecoder()
	d.SetAliasTag("json")
	d.IgnoreUnknownKeys(true)
	return d
}()

// apiInternalServer provides REST endpoints that proxy to RPC services. It
// serves as a bridge between HTTP REST clients and internal RPC services.
type apiInternalServer struct {
	mux *mux.Router
}

// NewAPIInternalServer creates a new REST API server that proxies to internal
// RPC services. It establishes connections to the RPC services and registers
// all REST endpoints. Routes are auto-generated from google.api.http proto
// annotations via protoc-gen-go-drpc.
func NewAPIInternalServer(
	ctx context.Context, nd rpcbase.NodeDialer, localNodeID roachpb.NodeID, useDRPC bool,
) (*apiInternalServer, error) {
	statusClient, err := serverpb.DialStatusClient(nd, ctx, localNodeID, useDRPC)
	if err != nil {
		return nil, err
	}

	adminClient, err := serverpb.DialAdminClient(nd, ctx, localNodeID, useDRPC)
	if err != nil {
		return nil, err
	}

	tsClient, err := rpcbase.DialRPCClient(
		nd,
		ctx,
		localNodeID,
		rpcbase.DefaultClass,
		tspb.NewGRPCTimeSeriesClientAdapter,
		tspb.NewDRPCTimeSeriesClientAdapter,
		useDRPC,
	)
	if err != nil {
		return nil, err
	}

	r := &apiInternalServer{mux: mux.NewRouter()}
	r.registerRoutes(serverpb.DRPCStatusGatewayRoutes(statusClient))
	r.registerRoutes(serverpb.DRPCAdminGatewayRoutes(adminClient))
	r.registerRoutes(tspb.DRPCTimeSeriesGatewayRoutes(tsClient))

	return r, nil
}

// ServeHTTP implements http.Handler interface.
func (r *apiInternalServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

func (r *apiInternalServer) registerRoutes(routes []drpc.HTTPRoute) {
	for _, route := range routes {
		r.mux.HandleFunc(route.Path, createHandlerFromRoute(route.Handler)).
			Methods(route.Method)
	}
}

// createHandlerFromRoute creates an HTTP handler from a drpc.HTTPRoute.Handler,
// which is typed as any but holds a func(context.Context, *TReq) (*TResp, error).
// Type safety is guaranteed by the generated DRPC*GatewayRoutes functions.
func createHandlerFromRoute(rpcMethod any) http.HandlerFunc {
	fn := reflect.ValueOf(rpcMethod)
	fnType := fn.Type()
	if fnType.Kind() != reflect.Func || fnType.NumIn() != 2 || fnType.NumOut() != 2 {
		panic(errors.AssertionFailedf(
			"expected func(context.Context, *TReq) (*TResp, error), got %T", rpcMethod))
	}
	reqType := fnType.In(1).Elem()
	return func(w http.ResponseWriter, req *http.Request) {
		newReq := reflect.New(reqType)
		rpcReq := newReq.Interface().(protoutil.Message)
		ctx := req.Context()
		ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, req)
		ctx = rpc.MarkDRPCGatewayRequest(ctx)

		if err := decoder.Decode(rpcReq, req.URL.Query()); err != nil {
			apiutil.WriteHTTPError(ctx, w, req, err)
			return
		}
		if vars := mux.Vars(req); len(vars) > 0 {
			if err := decodePathVars(rpcReq, vars); err != nil {
				apiutil.WriteHTTPError(ctx, w, req, err)
				return
			}
		}
		// For POST requests, decode the request body (JSON or protobuf).
		if req.Method == http.MethodPost {
			if err := apiutil.DecodeRequest(req, rpcReq); err != nil {
				apiutil.WriteHTTPError(ctx, w, req,
					status.Errorf(codes.InvalidArgument, "failed to decode request body: %v", err))
				return
			}
		}

		results := fn.Call([]reflect.Value{reflect.ValueOf(ctx), newReq})
		if !results[1].IsNil() {
			apiutil.WriteHTTPError(ctx, w, req, results[1].Interface().(error))
			return
		}
		resp := results[0].Interface().(protoutil.Message)
		if err := apiutil.WriteResponse(ctx, w, req, http.StatusOK, resp); err != nil {
			apiutil.WriteHTTPError(ctx, w, req, err)
		}
	}
}

func decodePathVars(rpcReq any, vars map[string]string) error {
	pathParams := make(url.Values)
	for k, v := range vars {
		pathParams[k] = []string{v}
	}
	return decoder.Decode(rpcReq, pathParams)
}
