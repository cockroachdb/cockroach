// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package httputil

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
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

type httpHeaderFn func(header http.Header)

// GetJSON uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSON(httpClient http.Client, path string, response proto.Message) error {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return err
	}
	_, err = doJSONRequest(httpClient, nil, req, response)
	return err
}

// PostJSON uses the supplied client to POST request to the URL specified by
// the parameters and unmarshals the result into response.
func PostJSON(httpClient http.Client, path string, request, response proto.Message) error {
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
	_, err = doJSONRequest(httpClient, nil, req, response)
	return err
}

// PostJSONWithHeaders uses the supplied client to POST request to the URL
// specified by the parameters and unmarshals the result into response.
//
// The caller can provide an optional callback function that can modify outgoing
// HTTP headers before the request is sent. Headers returned with the response
// are returned to the caller.
func PostJSONWithHeaders(
	httpClient http.Client, path string, headerFn httpHeaderFn, request, response proto.Message,
) (http.Header, error) {
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

	return doJSONRequest(httpClient, headerFn, req, response)
}

func doJSONRequest(
	httpClient http.Client, headerFn httpHeaderFn, req *http.Request, response proto.Message,
) (http.Header, error) {
	if timeout := httpClient.Timeout; timeout > 0 {
		req.Header.Set("Grpc-Timeout", strconv.FormatInt(timeout.Nanoseconds(), 10)+"n")
	}
	req.Header.Set(AcceptHeader, JSONContentType)
	if headerFn != nil {
		headerFn(req.Header)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if contentType := resp.Header.Get(ContentTypeHeader); !(resp.StatusCode == http.StatusOK && contentType == JSONContentType) {
		b, err := ioutil.ReadAll(resp.Body)
		return resp.Header, errors.Errorf(
			"status: %s, content-type: %s, body: %s, error: %v", resp.Status, contentType, b, err,
		)
	}
	return resp.Header, jsonpb.Unmarshal(resp.Body, response)
}
