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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/gogo/protobuf/proto"
	"gopkg.in/yaml.v1"

	"github.com/cockroachdb/cockroach/util/protoutil"
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
	// YAMLContentType is the YAML content type.
	YAMLContentType = "text/yaml"
	// AltYAMLContentType is the alternate YAML content type.
	AltYAMLContentType = "application/x-yaml"
	// PlaintextContentType is the plaintext content type.
	PlaintextContentType = "text/plain"
	// SnappyEncoding is the snappy encoding.
	SnappyEncoding = "snappy"
	// GzipEncoding is the gzip encoding.
	GzipEncoding = "gzip"
)

// EncodingType is an enum describing available encodings.
type EncodingType int

const (
	// JSONEncoding includes application/json and application/x-json.
	JSONEncoding EncodingType = iota
	// ProtoEncoding includes application/x-protobuf and application/x-google-protobuf.
	ProtoEncoding
	// YAMLEncoding includes text/yaml and application/x-yaml.
	YAMLEncoding
)

// AllEncodings includes all supported encodings.
var AllEncodings = []EncodingType{JSONEncoding, ProtoEncoding, YAMLEncoding}

func isAllowed(encType EncodingType, allowed []EncodingType) bool {
	for _, et := range allowed {
		if encType == et {
			return true
		}
	}
	return false
}

var yamlXXXUnrecognizedRE = regexp.MustCompile(` *xxx_unrecognized: \[\]\n?`)

// sanitizeYAML filters lines in the input which match xxx_unrecognized, a
// truly-annoying public member of proto Message structs, which we
// cannot specify yaml output tags for.
// TODO(spencer): there's got to be a better way to do this.
func sanitizeYAML(b []byte) []byte {
	return yamlXXXUnrecognizedRE.ReplaceAll(b, []byte{})
}

// getEncodingIndex returns the index at which enc appears in accept.
// If enc does not appear in accept, returns MaxInt32
func getEncodingIndex(enc, accept string) int32 {
	if idx := strings.Index(accept, enc); idx != -1 {
		return int32(idx)
	}
	return math.MaxInt32
}

// GetContentType pulls out the content type from a request header
// it ignores every value after the first semicolon
func GetContentType(request *http.Request) string {
	contentType := request.Header.Get(ContentTypeHeader)
	semicolonIndex := strings.Index(contentType, ";")
	if semicolonIndex > -1 {
		contentType = contentType[:semicolonIndex]
	}
	return contentType
}

// UnmarshalRequest examines the request Content-Type header in order
// to determine the encoding of the supplied body. Supported content
// types include:
//
//   JSON     - {"application/json", "application/x-json"}
//   Protobuf - {"application/x-protobuf", "application/x-google-protobuf"}
//   YAML     - {"text/yaml", "application/x-yaml"}
//
// The body is unmarshalled into the supplied value parameter. An
// error is returned on an unmarshalling error or on an unsupported
// content type.
func UnmarshalRequest(r *http.Request, body []byte, value interface{}, allowed []EncodingType) error {
	contentType := GetContentType(r)
	switch contentType {
	case JSONContentType, AltJSONContentType:
		if isAllowed(JSONEncoding, allowed) {
			return json.Unmarshal(body, value)
		}
	case ProtoContentType, AltProtoContentType:
		if isAllowed(ProtoEncoding, allowed) {
			msg, ok := value.(proto.Message)
			if !ok {
				return Errorf("unable to convert %+v to protobuf", value)
			}
			return proto.Unmarshal(body, msg)
		}
	case YAMLContentType, AltYAMLContentType:
		if isAllowed(YAMLEncoding, allowed) {
			return yaml.Unmarshal(body, value)
		}
	}
	return Errorf("unsupported content type: %q", contentType)
}

// jsonWrapper provides a wrapper on any slice data type being
// marshaled to JSON. This prevents a security vulnerability
// where a phishing attack can trick a user's browser into
// requesting a document from Cockroach as an executable script,
// allowing the contents of the fetched document to be treated
// as executable javascript. More details here:
// http://haacked.com/archive/2009/06/25/json-hijacking.aspx/
type jsonWrapper struct {
	Data interface{} `json:"d"`
}

// MarshalResponse examines the request Accept header to determine the
// client's preferred response encoding. Supported content types
// include JSON, protobuf, and YAML. If the Accept header is not
// available, the Content-Type header specifying the request encoding
// is used. The value parameter is marshalled using the response
// encoding and the resulting body and content type are returned. If
// the encoding could not be determined by either header, the response
// is marshalled using JSON. Falls back to JSON when the protobuf format
// cannot be used for the given value.
func MarshalResponse(r *http.Request, value interface{}, allowed []EncodingType) (
	body []byte, contentType string, err error) {
	// TODO(spencer): until there's a nice (free) way to parse the
	//   Accept header and properly use the request's preference for a
	//   content type, we simply find out which of "json", "protobuf" or
	//   "yaml" appears first in the Accept header. If neither do, we
	//   default to JSON.
	jsonIdx := int32(math.MaxInt32)
	protoIdx := int32(math.MaxInt32)
	yamlIdx := int32(math.MaxInt32)

	accept := r.Header.Get(AcceptHeader)
	if isAllowed(JSONEncoding, allowed) {
		jsonIdx = getEncodingIndex("json", accept)
	}
	if isAllowed(ProtoEncoding, allowed) {
		protoIdx = getEncodingIndex("protobuf", accept)
	}
	if isAllowed(YAMLEncoding, allowed) {
		yamlIdx = getEncodingIndex("yaml", accept)
	}

	if jsonIdx == math.MaxInt32 && yamlIdx == math.MaxInt32 && protoIdx == math.MaxInt32 {
		switch GetContentType(r) {
		case JSONContentType, AltJSONContentType:
			if isAllowed(JSONEncoding, allowed) {
				jsonIdx = 0
			}
		case ProtoContentType, AltProtoContentType:
			if isAllowed(ProtoEncoding, allowed) {
				protoIdx = 0
			}
		case YAMLContentType, AltYAMLContentType:
			if isAllowed(YAMLEncoding, allowed) {
				yamlIdx = 0
			}
		}
	}

	// Reset protoIdx if value cannot be converted to a protocol message
	if protoIdx < math.MaxInt32 {
		if _, ok := value.(proto.Message); !ok {
			protoIdx = int32(math.MaxInt32)
		}
	}

	if protoIdx < jsonIdx && protoIdx < yamlIdx {
		// Protobuf-encode the config.
		contentType = ProtoContentType
		if body, err = protoutil.Marshal(value.(proto.Message)); err != nil {
			err = Errorf("unable to marshal %+v to protobuf: %s", value, err)
		}
	} else if yamlIdx < jsonIdx && yamlIdx < protoIdx {
		// YAML-encode the config.
		contentType = YAMLContentType
		if body, err = yaml.Marshal(value); err != nil {
			err = Errorf("unable to marshal %+v to yaml: %s", value, err)
		} else {
			body = sanitizeYAML(body)
		}
	} else {
		// Always fall back to JSON-encode the config.
		contentType = JSONContentType
		switch reflect.ValueOf(value).Kind() {
		case reflect.Array, reflect.Slice:
			value = jsonWrapper{Data: value}
		}
		if body, err = json.MarshalIndent(value, "", "  "); err != nil {
			err = Errorf("unable to marshal %+v to json: %s", value, err)
		}
	}
	return
}

// GetJSON uses the supplied client to retrieve the URL specified by the parameters and
// unmarshals the result into the supplied interface.
//
// TODO(cdo): Refactor the *JSON methods to handle more encodings.
func GetJSON(httpClient *http.Client, scheme, hostport, path string, v interface{}) error {
	url := fmt.Sprintf("%s://%s%s", scheme, hostport, path)
	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return Errorf("status: %s, error: %s", resp.Status, b)
	}
	return json.Unmarshal(b, v)
}

// PostJSON uses the supplied client to perform a POST to the URL specified
// by the parameters and unmarshals the result into the supplied interface.
// This function assumes that the body is also JSON.
func PostJSON(httpClient *http.Client, scheme, hostport, path, body string, v interface{}) error {
	url := fmt.Sprintf("%s://%s%s", scheme, hostport, path)
	resp, err := httpClient.Post(url, JSONContentType, strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return Errorf("status: %s, error: %s", resp.Status, b)
	}
	return json.Unmarshal(b, v)
}
