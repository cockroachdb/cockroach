// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WriteJSONResponse returns a payload as JSON to the HTTP client.
func WriteJSONResponse(ctx context.Context, w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	res, err := json.Marshal(payload)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	_, _ = w.Write(res)
}

// GetIntQueryStringVal gets the int value of a Query String parameter, param, from the provided url query string
// values.
//
// Returns an int if the param exists and can be cast into an int. Otherwise, returns 0.
func GetIntQueryStringVal(queryStringVals url.Values, param string) (int, error) {
	if queryStringVals.Has(param) {
		queryArgStr := queryStringVals.Get(param)
		queryArgInt, err := strconv.Atoi(queryArgStr)
		if err != nil {
			return 0, err
		}
		return queryArgInt, nil
	}
	return 0, nil
}

// GetIntQueryStringVals gets all int values of a Query String parameter, param, from the provided url query string
// values.
//
// Returns an int slice if the param exists and ALL the values can be cast to ints. Otherwise, returns an empty slice.
func GetIntQueryStringVals(queryStringVals url.Values, param string) ([]int, error) {
	if queryStringVals.Has(param) {
		queryArgStrs := queryStringVals[param]
		queryArgInts := make([]int, 0, len(queryArgStrs))
		for _, a := range queryArgStrs {

			i, err := strconv.Atoi(a)
			if err != nil {
				return []int{}, err
			}
			queryArgInts = append(queryArgInts, i)
		}

		return queryArgInts, nil
	}
	return []int{}, nil
}

// WriteHTTPError converts an error to an HTTP error response. It handles gRPC
// status codes and converts them to appropriate HTTP status codes. Internal
// errors are masked to avoid leaking implementation details.
func WriteHTTPError(ctx context.Context, w http.ResponseWriter, req *http.Request, err error) {
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

	if err := WriteResponse(ctx, w, req, httpCode, data); err != nil {
		log.Dev.Errorf(ctx, "failed to respond with error: %v", err)
		const fallback = `{"code": 13, "message": "failed to marshal error message"}`
		if _, err := io.WriteString(w, fallback); err != nil {
			log.Dev.Errorf(ctx, "failed to write fallback error: %v", err)
		}
	}
}

// WriteResponse writes a protobuf message as an HTTP response. It supports both
// JSON and protobuf content types based on the request headers.
func WriteResponse(
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
	case httputil.JSONContentType:
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

// DecodeRequest decodes the request body into a protobuf message. It supports
// both JSON and protobuf content types, defaulting to JSON.
func DecodeRequest(req *http.Request, target protoutil.Message) error {
	if req.Body == nil {
		return nil
	}
	reqContentType := selectContentType(req.Header[httputil.ContentTypeHeader])
	switch reqContentType {
	case httputil.JSONContentType:
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
		case httputil.ProtoContentType, httputil.AltProtoContentType:
			return httputil.ProtoContentType
		case httputil.JSONContentType, httputil.AltJSONContentType, httputil.MIMEWildcard:
			return httputil.JSONContentType
		}
	}
	return httputil.JSONContentType
}
