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
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
)

// testHTTPSession is a helper for tests which want to exercise repeated simple
// HTTP requests against the same server, especially where no errors are
// expected in the HTTP layer.
type testHTTPSession struct {
	t       *testing.T
	client  *http.Client
	baseURL string
}

// newTestHTTPSession constructs a new testHTTPSession. The session will
// instantiate a client using the based base context. All HTTP requests from the
// session will be sent to the given baseUrl.
//
// baseUrl should be specified *without* a request scheme (i.e. "http://"); the
// request scheme will be used from the context.
//
// If an error occurs in HTTP layer during any session operation, a Fatal method
// will be called on the supplied t.Tester.
func newTestHTTPSession(t *testing.T, ctx *base.Context, baseURL string) *testHTTPSession {
	client, err := ctx.GetHTTPClient()
	if err != nil {
		t.Fatalf("error creating client: %s", err)
	}
	return &testHTTPSession{
		t:       t,
		client:  client,
		baseURL: ctx.HTTPRequestScheme() + "://" + baseURL,
	}
}

// Post performs an http POST of the given bytes to the given relative URL. Any
// response returned from the request will be returned from this method as a
// byte slice.
func (ths *testHTTPSession) Post(relative string, body []byte) []byte {
	req, err := http.NewRequest("POST", ths.baseURL+relative, bytes.NewBuffer(body))
	if err != nil {
		ths.t.Fatal(err)
	}

	// All requests currently accept JSON.
	req.Header.Set(util.ContentTypeHeader, util.JSONContentType)
	req.Header.Set(util.AcceptHeader, util.JSONContentType)
	resp, err := ths.client.Do(req)
	if err != nil {
		ths.t.Fatal(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := ioutil.ReadAll(resp.Body)
		ths.t.Fatalf("unexpected status code: %v, %s", resp.StatusCode, respBody)
	}
	returnedContentType := resp.Header.Get(util.ContentTypeHeader)
	if returnedContentType != util.JSONContentType {
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
func (ths *testHTTPSession) PostProto(relative string, msg proto.Message, out interface{}) {
	requestBytes, err := json.Marshal(msg)
	if err != nil {
		ths.t.Fatal(err)
	}
	resp := ths.Post(relative, requestBytes)
	if err := json.Unmarshal(resp, out); err != nil {
		ths.t.Fatal(err)
	}
}
