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

package util_test

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

var testConfig = config.ZoneConfig{
	ReplicaAttrs: []roachpb.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"a", "hdd"}},
		{Attrs: []string{"b", "ssd"}},
		{Attrs: []string{"b", "hdd"}},
	},
	RangeMinBytes: 1 << 20,
	RangeMaxBytes: 64 << 20,
}

var yamlConfig = []byte(`replicas:
- attrs: [a, ssd]
- attrs: [a, hdd]
- attrs: [b, ssd]
- attrs: [b, hdd]
range_min_bytes: 1048576
range_max_bytes: 67108864
gc:
  ttlseconds: 0
`)

var jsonConfig = []byte(`{
  "replica_attrs": [
    {
      "attrs": [
        "a",
        "ssd"
      ]
    },
    {
      "attrs": [
        "a",
        "hdd"
      ]
    },
    {
      "attrs": [
        "b",
        "ssd"
      ]
    },
    {
      "attrs": [
        "b",
        "hdd"
      ]
    }
  ],
  "range_min_bytes": 1048576,
  "range_max_bytes": 67108864,
  "gc": {
    "ttl_seconds": 0
  }
}`)

var protobufConfig []byte

func init() {
	var err error
	if protobufConfig, err = protoutil.Marshal(&testConfig); err != nil {
		log.Fatalf("unable to marshal test config %+v: %s", testConfig, err)
	}
}

func TestGetContentType(t *testing.T) {
	testCases := []struct {
		header, expType string
	}{
		{util.JSONContentType, util.JSONContentType},
		{"text/html; charset=ISO-8859-4", "text/html"},
	}
	for i, test := range testCases {
		req, err := http.NewRequest("GET", "http://foo.com", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, test.header)
		if typ := util.GetContentType(req); typ != test.expType {
			t.Errorf("%d: expected content type %s; got %s", i, test.expType, typ)
		}
	}
}

func TestUnmarshalRequest(t *testing.T) {
	testCases := []struct {
		cType    string
		body     []byte
		expError bool
	}{
		{util.JSONContentType, jsonConfig, false},
		{util.AltJSONContentType, jsonConfig, false},
		{util.ProtoContentType, protobufConfig, false},
		{util.AltProtoContentType, protobufConfig, false},
		{util.YAMLContentType, yamlConfig, false},
		{util.AltYAMLContentType, yamlConfig, false},
		{"foo", jsonConfig, true},
		{"baz", protobufConfig, true},
		{"bar", yamlConfig, true},
	}

	for i, test := range testCases {
		req, err := http.NewRequest("GET", "http://foo.com", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, test.cType)
		config := &config.ZoneConfig{}
		err = util.UnmarshalRequest(req, test.body, config, util.AllEncodings)
		if test.expError {
			if err == nil {
				t.Errorf("%d: unexpected success", i)
			}
			continue
		} else if err != nil {
			t.Errorf("%d: unexpected failure: %s", i, err)
			continue
		}
		if !reflect.DeepEqual(config, &testConfig) {
			t.Errorf("%d: unmarshalling yielded config %+v; expected %+v", i, config, testConfig)
		}
	}
}

func TestMarshalResponse(t *testing.T) {
	testCases := []struct {
		cType, accept string
		expCType      string
		expBody       []byte
	}{
		{"", util.JSONContentType, util.JSONContentType, jsonConfig},
		{"", util.AltJSONContentType, util.JSONContentType, jsonConfig},
		{util.JSONContentType, "", util.JSONContentType, jsonConfig},
		{util.JSONContentType, "foo", util.JSONContentType, jsonConfig},
		{"", util.AltProtoContentType, util.ProtoContentType, protobufConfig},
		{"", util.ProtoContentType, util.ProtoContentType, protobufConfig},
		{util.ProtoContentType, "", util.ProtoContentType, protobufConfig},
		{util.ProtoContentType, "foo", util.ProtoContentType, protobufConfig},
		{"", util.YAMLContentType, util.YAMLContentType, yamlConfig},
		{"", util.AltYAMLContentType, util.YAMLContentType, yamlConfig},
		{util.YAMLContentType, "", util.YAMLContentType, yamlConfig},
		{util.YAMLContentType, "foo", util.YAMLContentType, yamlConfig},
		// Test mixed accept headers; but we ignore quality.
		{"", "application/json, application/x-protobuf; q=0.8", util.JSONContentType, jsonConfig},
		{"", "application/json, application/x-protobuf; q=0.8, text/yaml; q=0.5", util.JSONContentType, jsonConfig},
		{"", "application/x-protobuf; q=0.8, text/yaml; q=0.5, application/json", util.ProtoContentType, protobufConfig},
		{"", "text/yaml; q=0.5, application/x-protobuf; q=0.8, application/json", util.YAMLContentType, yamlConfig},
		// Test default encoding is JSON.
		{"foo", "foo", util.JSONContentType, jsonConfig},
	}
	for i, test := range testCases {
		req, err := http.NewRequest("GET", "http://foo.com", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, test.cType)
		req.Header.Add(util.AcceptHeader, test.accept)
		body, cType, err := util.MarshalResponse(req, &testConfig, util.AllEncodings)
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		if !bytes.Equal(body, test.expBody) {
			t.Errorf("%d: expected:\n%q\ngot\n%q", i, test.expBody, body)
		}
		if cType != test.expCType {
			t.Errorf("%d: expected %s content type; got %s", i, test.expCType, cType)
		}
	}
}

// Verify that marshal response protects against returning
// unguarded slice or array types.
func TestMarshalResponseSlice(t *testing.T) {
	// We expect the array to be wrapped in a struct with data key "d".
	expBody := []byte(`{
  "d": [
    1,
    2,
    3
  ]
}`)
	// Respond with JSON versions of a slice and an array of integers from 1,2,3.
	for i, value := range []interface{}{[]int{1, 2, 3}, [3]int{1, 2, 3}} {
		req, err := http.NewRequest("GET", "http://foo.com", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Add(util.ContentTypeHeader, util.JSONContentType)
		req.Header.Add(util.AcceptHeader, util.JSONContentType)
		body, _, err := util.MarshalResponse(req, value, util.AllEncodings)
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		if !bytes.Equal(body, expBody) {
			t.Errorf("%d: expected %q; got %q", i, expBody, body)
		}
	}
}

// TestProtoEncodingError verifies that MarshalResponse and
// UnmarshalRequest gracefully handles a protocol message type error.
func TestProtoEncodingError(t *testing.T) {
	req, err := http.NewRequest("GET", "http://foo.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add(util.ContentTypeHeader, util.ProtoContentType)
	reqBody := []byte("foo")
	var value string
	err = util.UnmarshalRequest(req, reqBody, value, []util.EncodingType{util.ProtoEncoding})
	if err == nil {
		t.Errorf("unexpected success")
	}

	req.Header.Add(util.AcceptHeader, util.ProtoContentType)
	body, cType, err := util.MarshalResponse(req, value, []util.EncodingType{util.ProtoEncoding})
	if err != nil {
		t.Fatal(err)
	}
	if cType != util.JSONContentType {
		t.Errorf("unexpected content type; got %s", cType)
	}
	if !bytes.Equal(body, body) {
		t.Errorf("unexpected boy; got %s", body)
	}
}
