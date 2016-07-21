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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
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

// GetJSON uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSON(httpClient http.Client, path string, response proto.Message) error {
	resp, err := httpClient.Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf("status: %s, body: %s", resp.Status, b)
	}
	return jsonpb.Unmarshal(resp.Body, response)
}

// PostJSON uses the supplied client to POST request to the URL specified by
// the parameters and unmarshals the result into response .
func PostJSON(httpClient http.Client, path string, request, response proto.Message) error {
	str, err := (&jsonpb.Marshaler{}).MarshalToString(request)
	if err != nil {
		return err
	}
	resp, err := httpClient.Post(path, JSONContentType, strings.NewReader(str))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		return errors.Errorf("status: %s, body: %s, error: %s", resp.Status, b, err)
	}
	return jsonpb.Unmarshal(resp.Body, response)
}

// StreamJSON uses the supplied client to POST the given proto request as JSON
// to the supplied streaming grpc-gw endpoint; the response type serves only to
// create the values passed to the callback (which is invoked for every message
// in the stream with a value of the supplied response type masqueraded as an
// interface).
func StreamJSON(
	httpClient http.Client,
	path string,
	request proto.Message,
	dest proto.Message,
	callback func(proto.Message),
) error {
	str, err := (&jsonpb.Marshaler{}).MarshalToString(request)
	if err != nil {
		return err
	}

	resp, err := httpClient.Post(path, JSONContentType, strings.NewReader(str))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		return errors.Errorf("status: %s, body: %s, error: %s", resp.Status, b, err)
	}

	// grpc-gw/runtime's JSONpb {en,de}coder is pretty half-baked. Essentially
	// we must pass a *map[string]*concreteType or it won't work (in
	// particular, using a struct or replacing `*concreteType` with either
	// `concreteType` or `proto.Message` leads to errors). This method should do
	// a decent enough job at encapsulating this away; should this change, we
	// should consider cribbing and fixing up the marshaling code.
	m := reflect.New(reflect.MapOf(reflect.TypeOf(""), reflect.TypeOf(dest)))
	// TODO(tschottdorf,tamird): We have cribbed parts of this object to deal
	// with varying proto imports, and should technically use them here. We can
	// get away with not cribbing more here for now though.
	marshaler := runtime.JSONPb{}
	decoder := marshaler.NewDecoder(resp.Body)
	for {
		if err := decoder.Decode(m.Interface()); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		v := m.Elem().MapIndex(reflect.ValueOf("result"))
		if !v.IsValid() {
			// TODO(tschottdorf): recover actual JSON.
			return errors.Errorf("unexpected JSON response: %+v", m)
		}
		callback(v.Interface().(proto.Message))
	}
	return nil
}
