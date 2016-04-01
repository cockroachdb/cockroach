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
	client  *http.Client
	baseURL string
}

// makeTestHTTPSession constructs a new testHTTPSession. The session will
// instantiate a client using the based base context. All HTTP requests from the
// session will be sent to the given baseUrl.
//
// baseUrl should be specified *without* a request scheme (i.e. "http://"); the
// request scheme will be used from the context.
//
// If an error occurs in HTTP layer during any session operation, a Fatal method
// will be called on the supplied t.Tester.
func makeTestHTTPSession(t *testing.T, ctx *base.Context, baseURL string) testHTTPSession {
	client, err := ctx.GetHTTPClient()
	if err != nil {
		t.Fatalf("error creating client: %s", err)
	}
	return testHTTPSession{
		client:  client,
		baseURL: ctx.HTTPRequestScheme() + "://" + baseURL,
	}
}

// Post performs an http POST of the given bytes to the given relative URL. Any
// response returned from the request will be returned from this method as a
// byte slice.
func (ths testHTTPSession) Post(relative string, body []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", ths.baseURL+relative, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	// All requests currently accept JSON.
	req.Header.Set(util.ContentTypeHeader, util.JSONContentType)
	req.Header.Set(util.AcceptHeader, util.JSONContentType)
	resp, err := ths.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := ioutil.ReadAll(resp.Body)
		return nil, util.Errorf("unexpected status code: %v, %s", resp.StatusCode, respBody)
	}
	returnedContentType := resp.Header.Get(util.ContentTypeHeader)
	if returnedContentType != util.JSONContentType {
		return nil, util.Errorf("unexpected content type: %v", returnedContentType)
	}
	return ioutil.ReadAll(resp.Body)
}

// PostProto handles the common case where both the data posted and the expected
// response are encoded protobuffers. This method handles the marshalling of
// those buffers.
func (ths testHTTPSession) PostProto(relative string, msg proto.Message, out interface{}) error {
	requestBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	resp, err := ths.Post(relative, requestBytes)
	if err != nil {
		return err
	}
	return json.Unmarshal(resp, out)
}
