// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/gorilla/schema"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
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

var decoder = schema.NewDecoder()

// route defines a REST endpoint with its handler and HTTP method.
type route struct {
	method  httpMethod
	path    string
	handler http.HandlerFunc
}

// apiInternalServer provides REST endpoints that proxy to RPC services. It
// serves as a bridge between HTTP REST clients and internal RPC services.
type apiInternalServer struct {
	mux    *mux.Router
	status serverpb.RPCStatusClient
}

// NewAPIInternalServer creates a new REST API server that proxies to internal
// RPC services. It establishes connections to the RPC services and registers
// all REST endpoints.
func NewAPIInternalServer(
	ctx context.Context, nd rpcbase.NodeDialer, localNodeID roachpb.NodeID,
) (*apiInternalServer, error) {
	status, err := serverpb.DialStatusClient(nd, ctx, localNodeID)
	if err != nil {
		return nil, err
	}

	r := &apiInternalServer{
		status: status,
		mux:    mux.NewRouter(),
	}

	r.registerStatusRoutes()

	decoder.SetAliasTag("json")
	decoder.IgnoreUnknownKeys(true)

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
			writeHTTPError(ctx, w, req, err)
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

	if err := decoder.Decode(rpcReq, req.URL.Query()); err != nil {
		return err
	}
	if err := decodePathVars(rpcReq, mux.Vars(req)); err != nil {
		return err
	}
	// For POST requests, decode the request body (JSON or protobuf)
	if req.Method == http.MethodPost {
		if err := decodeRequest(req, rpcReq); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to decode request body: %v", err)
		}
	}

	resp, err := rpcMethod(ctx, rpcReq)
	if err != nil {
		return err
	}
	return writeResponse(ctx, w, req, http.StatusOK, resp)
}

func decodePathVars[TReq protoutil.Message](rpcReq TReq, vars map[string]string) error {
	queryParams := make(url.Values)
	for k, v := range vars {
		queryParams[k] = []string{v}
	}
	return decoder.Decode(rpcReq, queryParams)
}

// writeHTTPError converts an error to an HTTP error response. It handles gRPC
// status codes and converts them to appropriate HTTP status codes. Internal
// errors are masked to avoid leaking implementation details.
func writeHTTPError(ctx context.Context, w http.ResponseWriter, req *http.Request, err error) {
	s, ok := status.FromError(err)
	if !ok {
		s = status.New(codes.Unknown, err.Error())
	}

	message := s.Message()
	if s.Code() == codes.Internal {
		message = srverrors.ErrAPIInternalErrorString
		log.Dev.Errorf(ctx, "failed internal API [%s] %s - %v", req.Method, req.URL.Path, err)
	} else {
		log.Ops.Errorf(ctx, "failed internal API [%s] %s - %v", req.Method, req.URL.Path, err)
	}

	data := &serverpb.ResponseError{
		Error:   message,
		Message: message,
		Code:    int32(s.Code()),
		// Details field is intentionally not populated as it's unused
	}

	// Convert gRPC status code to HTTP status code
	// TODO(server): eliminate this dependency on grpc-gateway when
	// migrating away from it
	httpCode := runtime.HTTPStatusFromCode(s.Code())

	if err := writeResponse(ctx, w, req, httpCode, data); err != nil {
		log.Dev.Errorf(ctx, "failed to respond with error: %v", err)
		const fallback = `{"code": 13, "message": "failed to marshal error message"}`
		if _, err := io.WriteString(w, fallback); err != nil {
			log.Dev.Errorf(ctx, "failed to write fallback error: %v", err)
		}
	}
}

// writeResponse writes a protobuf message as an HTTP response. It supports both
// JSON and protobuf content types based on the request headers.
func writeResponse(
	ctx context.Context,
	w http.ResponseWriter,
	req *http.Request,
	code int,
	payload protoutil.Message,
) error {
	// Determine the response content type by checking Accept header first, then
	// falling back to Content-Type header. Default to JSON if neither specifies
	// a supported type.
	resContentType := selectContentType(append(
		req.Header[httputil.AcceptEncodingHeader],
		req.Header[httputil.ContentTypeHeader]...))

	var buf []byte
	switch resContentType {
	case httputil.ProtoContentType:
		b, err := protoutil.Marshal(payload)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal the protobuf response: %v", err)
		}
		buf = b
	case httputil.JSONContentType, httputil.MIMEWildcard:
		jsonpb := &protoutil.JSONPb{
			EnumsAsInts:  true,
			EmitDefaults: true,
			Indent:       "  ",
		}
		b, err := jsonpb.Marshal(payload)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal the JSON response: %v", err)
		}
		buf = b
	}

	w.Header().Set("Content-Type", resContentType)
	w.WriteHeader(code)
	if _, err := w.Write(buf); err != nil {
		return status.Errorf(codes.Internal, "failed to write HTTP response: %v", err)
	}
	return nil
}

// decodeRequest decodes the request body into a protobuf message. It supports
// both JSON and protobuf content types, defaulting to JSON.
func decodeRequest(req *http.Request, target protoutil.Message) error {
	if req.Body == nil {
		return nil
	}
	reqContentType := selectContentType(req.Header[httputil.ContentTypeHeader])
	switch reqContentType {
	case httputil.JSONContentType, httputil.MIMEWildcard:
		return jsonpb.Unmarshal(req.Body, target)
	case httputil.ProtoContentType:
		bytes, err := io.ReadAll(req.Body)
		if err != nil {
			return err
		}
		return protoutil.Unmarshal(bytes, target)
	default:
		return jsonpb.Unmarshal(req.Body, target)
	}
}

// selectContentType chooses the appropriate content type from a list of
// options. It prefers protobuf or JSON if available, defaulting to JSON if none
// match.
func selectContentType(contentTypes []string) string {
	for _, c := range contentTypes {
		switch c {
		case httputil.ProtoContentType:
			return httputil.ProtoContentType
		case httputil.JSONContentType, httputil.MIMEWildcard:
			return httputil.JSONContentType
		}
	}
	return httputil.JSONContentType
}
