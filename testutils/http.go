// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package testutils

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TestHTTPSession is a helper for tests which want to exercise repeated simple
// HTTP requests against the same server, especially where no errors are
// expected in the HTTP layer.
type TestHTTPSession struct {
	t       util.Tester
	client  *http.Client
	baseURL string
}

// NewTestHTTPSession constructs a new TestHTTPSession. The session will
// instantiate a client using the based base context. All HTTP requests from the
// session will be sent to the given baseUrl.
//
// baseUrl should be specified *without* a request scheme (i.e. "http://"); the
// request scheme will be used from the context.
//
// If an error occurs in HTTP layer during any session operation, a Fatal method
// will be called on the supplied t.Tester.
func NewTestHTTPSession(t util.Tester, ctx *base.Context, baseURL string) *TestHTTPSession {
	client, err := ctx.GetHTTPClient()
	if err != nil {
		t.Fatalf("error creating context: %s", err)
	}
	return &TestHTTPSession{
		t:       t,
		client:  client,
		baseURL: ctx.RequestScheme() + "://" + baseURL,
	}
}

// Post performs an http POST of the given bytes to the given relative URL. Any
// response returned from the request will be returned from this method as a
// byte slice.
func (ths *TestHTTPSession) Post(relative string, body []byte) []byte {
	req, err := http.NewRequest("POST", ths.baseURL+relative, bytes.NewBuffer(body))
	if err != nil {
		ths.t.Fatal(err)
	}

	// All requests currently accept JSON.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := ths.client.Do(req)
	if err != nil {
		ths.t.Fatal(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		ths.t.Fatalf("unexpected status code: %v, %s", resp.StatusCode, body)
	}
	returnedContentType := resp.Header.Get(util.ContentTypeHeader)
	if returnedContentType != "application/json" {
		ths.t.Fatalf("unexpected content type: %v", returnedContentType)
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ths.t.Fatal(err)
	}
	return respBody
}

// PostProto handles the common case where both the data posted and the expected
// response are encoded protobuffers. This method handles the marshalling of
// those buffers.
func (ths *TestHTTPSession) PostProto(relative string, msg gogoproto.Message, out interface{}) {
	requestBytes, err := json.Marshal(msg)
	if err != nil {
		ths.t.Fatal(err)
	}
	resp := ths.Post(relative, requestBytes)
	if err := json.Unmarshal(resp, out); err != nil {
		ths.t.Fatal(err)
	}
}

// Get performs an http GET request to the given relative URL. Any response
// returned from the request will be returned from this method as a byte slice.
func (ths *TestHTTPSession) Get(relative string) []byte {
	req, err := http.NewRequest("GET", ths.baseURL+relative, nil)
	if err != nil {
		ths.t.Fatal(err)
	}

	// All requests currently accept JSON.
	req.Header.Set("Accept", "application/json")
	resp, err := ths.client.Do(req)
	if err != nil {
		ths.t.Fatal(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		ths.t.Fatalf("unexpected status code: %v, %s", resp.StatusCode, body)
	}
	returnedContentType := resp.Header.Get(util.ContentTypeHeader)
	if returnedContentType != "application/json" {
		ths.t.Fatalf("unexpected content type: %v", returnedContentType)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ths.t.Fatal(err)
	}
	return body
}
