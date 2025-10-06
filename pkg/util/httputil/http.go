// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package httputil

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

const (
	// AcceptHeader is the canonical header name for accept.
	AcceptHeader = "Accept"
	// AcceptEncodingHeader is the canonical header name for accept encoding.
	AcceptEncodingHeader = "Accept-Encoding"
	// ContentDispositionHeader is the canonical header name for content disposition.
	ContentDispositionHeader = "Content-Disposition"
	// ContentEncodingHeader is the canonical header name for content type.
	ContentEncodingHeader = "Content-Encoding"
	// ContentTypeHeader is the canonical header name for content type.
	ContentTypeHeader = "Content-Type"
	// JSONContentType is the JSON content type.
	JSONContentType = "application/json"
	// AltJSONContentType is the alternate JSON content type.
	AltJSONContentType = "application/x-json"
	// ProtoContentType is the protobuf content type.
	ProtoContentType = "application/x-protobuf"
	// AltProtoContentType is the alternate protobuf content type.
	AltProtoContentType = "application/x-google-protobuf"
	// PlaintextContentType is the plaintext content type.
	PlaintextContentType = "text/plain"
	// GzipEncoding is the gzip encoding.
	GzipEncoding = "gzip"
)

type JSONOptions struct {
	ignoreUnknownFields bool
}

type JSONOption func(result *JSONOptions)

func IgnoreUnknownFields() JSONOption {
	return func(result *JSONOptions) {
		result.ignoreUnknownFields = true
	}
}

// GetJSON uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response. This will fail if the response
// contains unknown fields.
// TODO(someone): make this context-aware, see client.go.
func GetJSON(httpClient http.Client, path string, response protoutil.Message) error {
	return GetJSONWithOptions(httpClient, path, response)
}

func GetJSONWithOptions(
	httpClient http.Client, path string, response protoutil.Message, opts ...JSONOption,
) error {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return err
	}
	_, err = doJSONRequest(httpClient, req, response, opts...)
	return err
}

// PostJSON uses the supplied client to POST request to the URL specified by
// the parameters and unmarshals the result into response.
// TODO(someone): make this context-aware, see client.go.
func PostJSON(httpClient http.Client, path string, request, response protoutil.Message) error {
	// Hack to avoid upsetting TestProtoMarshal().
	marshalFn := (&jsonpb.Marshaler{}).Marshal

	var buf bytes.Buffer
	if err := marshalFn(&buf, request); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", path, &buf)
	if err != nil {
		return err
	}
	_, err = doJSONRequest(httpClient, req, response)
	return err
}

// PostJSONRaw uses the supplied client to POST request to the URL specified by
// the parameters and returns the response.
func PostJSONRaw(httpClient http.Client, path string, request []byte) (*http.Response, error) {
	buf := bytes.NewBuffer(request)
	req, err := http.NewRequest("POST", path, buf)
	if err != nil {
		return nil, err
	}
	return doJSONRawRequest(httpClient, req)
}

// PostJSONWithRequest uses the supplied client to POST request to the URL
// specified by the parameters and unmarshals the result into response.
//
// The response is returned to the caller, though its body will have been
// closed.
// TODO(someone): make this context-aware, see client.go.
func PostJSONWithRequest(
	httpClient http.Client, path string, request, response protoutil.Message,
) (*http.Response, error) {
	// Hack to avoid upsetting TestProtoMarshal().
	marshalFn := (&jsonpb.Marshaler{}).Marshal

	var buf bytes.Buffer
	if err := marshalFn(&buf, request); err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", path, &buf)
	if err != nil {
		return nil, err
	}

	return doJSONRequest(httpClient, req, response)
}

// PostProtobuf uses the supplied client to POST request to the URL specified by
// the parameters and unmarshal the result into response, using a
// protobuf-encoded request body.
func PostProtobuf(
	ctx context.Context, httpClient http.Client, path string, request, response protoutil.Message,
) error {
	buf, err := protoutil.Marshal(request)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(buf)
	req, err := http.NewRequestWithContext(ctx, "POST", path, reader)
	if err != nil {
		return err
	}
	if timeout := httpClient.Timeout; timeout > 0 {
		req.Header.Set("Grpc-Timeout", strconv.FormatInt(timeout.Nanoseconds(), 10)+"n")
	}
	req.Header.Set(AcceptHeader, ProtoContentType)
	req.Header.Set(ContentTypeHeader, ProtoContentType)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if contentType := resp.Header.Get(ContentTypeHeader); !(resp.StatusCode == http.StatusOK && contentType == ProtoContentType) {
		// NB: errors.Wrapf(nil, ...) returns nil.
		// nolint:errwrap
		return errors.Errorf(
			"status: %s, content-type: %s, body: %s, error: %v", resp.Status, contentType, b, err,
		)
	}
	return protoutil.Unmarshal(b, response)
}

func doJSONRequest(
	httpClient http.Client, req *http.Request, response protoutil.Message, opts ...JSONOption,
) (*http.Response, error) {
	options := &JSONOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if timeout := httpClient.Timeout; timeout > 0 {
		req.Header.Set("Grpc-Timeout", strconv.FormatInt(timeout.Nanoseconds(), 10)+"n")
	}
	req.Header.Set(AcceptHeader, JSONContentType)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if contentType := resp.Header.Get(ContentTypeHeader); !(resp.StatusCode == http.StatusOK && contentType == JSONContentType) {
		b, err := io.ReadAll(resp.Body)
		// NB: errors.Wrapf(nil, ...) returns nil.
		// nolint:errwrap
		return resp, errors.Errorf(
			"status: %s, content-type: %s, body: %s, error: %v", resp.Status, contentType, b, err,
		)
	}
	if options.ignoreUnknownFields {
		json := jsonpb.Unmarshaler{AllowUnknownFields: true}
		return resp, json.Unmarshal(resp.Body, response)
	}
	return resp, jsonpb.Unmarshal(resp.Body, response)
}

func doJSONRawRequest(httpClient http.Client, req *http.Request) (*http.Response, error) {
	if timeout := httpClient.Timeout; timeout > 0 {
		req.Header.Set("Grpc-Timeout", strconv.FormatInt(timeout.Nanoseconds(), 10)+"n")
	}
	req.Header.Set(AcceptHeader, JSONContentType)
	return httpClient.Do(req)
}
