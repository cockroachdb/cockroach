// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package httputil

import (
	"bytes"
	"io/ioutil"
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

// GetJSON uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
// TODO(someone): make this context-aware, see client.go.
func GetJSON(httpClient http.Client, path string, response protoutil.Message) error {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return err
	}
	_, err = doJSONRequest(httpClient, req, response)
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

func doJSONRequest(
	httpClient http.Client, req *http.Request, response protoutil.Message,
) (*http.Response, error) {
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
		b, err := ioutil.ReadAll(resp.Body)
		return resp, errors.Errorf(
			"status: %s, content-type: %s, body: %s, error: %v", resp.Status, contentType, b, err,
		)
	}
	return resp, jsonpb.Unmarshal(resp.Body, response)
}
