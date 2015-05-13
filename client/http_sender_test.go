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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package client

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
)

var (
	testKey     = proto.Key("a")
	testTS      = proto.Timestamp{WallTime: 1, Logical: 1}
	testPutReq  = &proto.PutRequest{RequestHeader: proto.RequestHeader{Timestamp: testTS, Key: testKey}}
	testPutResp = &proto.PutResponse{ResponseHeader: proto.ResponseHeader{Timestamp: testTS}}
)

func startTestHTTPServer(handler http.Handler) (*httptest.Server, string) {
	server := httptest.NewTLSServer(handler)
	addr := server.Listener.Addr().String()
	return server, addr
}

// TestHTTPSenderSend verifies sending posts.
func TestHTTPSenderSend(t *testing.T) {
	server, addr := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected method POST; got %s", r.Method)
		}
		if r.URL.Path != KVDBEndpoint+"Put" {
			t.Errorf("expected url %s; got %s", KVDBEndpoint+"Put", r.URL.Path)
		}
		// Unmarshal the request.
		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected error reading body: %s", err)
		}
		args := &proto.PutRequest{}
		if err := util.UnmarshalRequest(r, reqBody, args, util.AllEncodings); err != nil {
			t.Errorf("unexpected error unmarshalling request: %s", err)
		}
		if !args.Key.Equal(testPutReq.Key) || !args.Timestamp.Equal(testPutReq.Timestamp) {
			t.Errorf("expected parsed %+v to equal %+v", args, testPutReq)
		}
		body, contentType, err := util.MarshalResponse(r, testPutResp, util.AllEncodings)
		if err != nil {
			t.Errorf("failed to marshal response: %s", err)
		}
		w.Header().Set("Content-Type", contentType)
		w.Write(body)
	}))
	defer server.Close()

	sender, err := NewHTTPSender(addr, testutils.NewTestBaseContext())
	if err != nil {
		t.Fatal(err)
	}
	reply := &proto.PutResponse{}
	sender.Send(context.TODO(), Call{Args: testPutReq, Reply: reply})
	if reply.GoError() != nil {
		t.Errorf("expected success; got %s", reply.GoError())
	}
	if !reply.Timestamp.Equal(testPutResp.Timestamp) {
		t.Errorf("expected received %+v to equal %+v", reply, testPutResp)
	}
}

// TestHTTPSenderRetryResponseCodes verifies that send is retried
// on some HTTP response codes but not on others.
func TestHTTPSenderRetryResponseCodes(t *testing.T) {
	HTTPRetryOptions.Backoff = 1 * time.Millisecond

	testCases := []struct {
		code  int
		retry bool
	}{
		{http.StatusServiceUnavailable, true},
		{http.StatusGatewayTimeout, true},
		{StatusTooManyRequests, true},
		{http.StatusRequestTimeout, false},
		{http.StatusBadRequest, false},
		{http.StatusNotFound, false},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusMethodNotAllowed, false},
		{http.StatusNotAcceptable, false},
		{http.StatusInternalServerError, false},
		{http.StatusNotImplemented, false},
	}
	for i, test := range testCases {
		count := 0
		server, addr := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count++
			if count == 1 {
				http.Error(w, "manufactured error", test.code)
				return
			}
			if !test.retry {
				t.Errorf("%d: didn't expect retry on code %d", i, test.code)
			}
			body, contentType, err := util.MarshalResponse(r, testPutResp, util.AllEncodings)
			if err != nil {
				t.Errorf("%d: failed to marshal response: %s", i, err)
			}
			w.Header().Set("Content-Type", contentType)
			w.Write(body)
		}))

		sender, err := NewHTTPSender(addr, testutils.NewTestBaseContext())
		if err != nil {
			t.Fatal(err)
		}
		reply := &proto.PutResponse{}
		sender.Send(context.TODO(), Call{Args: testPutReq, Reply: reply})
		if test.retry {
			if count != 2 {
				t.Errorf("%d: expected retry", i)
			}
			if reply.GoError() != nil {
				t.Errorf("%d: expected success after retry; got %s", i, reply.GoError())
			}
		} else {
			if count != 1 {
				t.Errorf("%d; expected no retry; got %d", i, count)
			}
			if reply.GoError() == nil {
				t.Errorf("%d: expected error", i)
			}
		}
		server.Close()
	}
}

// TestHTTPSenderRetryHTTPSendError verifies that send is retried
// on all errors sending HTTP requests.
func TestHTTPSenderRetryHTTPSendError(t *testing.T) {
	HTTPRetryOptions.Backoff = 1 * time.Millisecond

	testCases := []func(*httptest.Server, http.ResponseWriter){
		// Send back an unparseable response but a success code on first try.
		func(s *httptest.Server, w http.ResponseWriter) {
			fmt.Fprintf(w, "\xff\xfe\x23\x44")
		},
		// Close the client connection.
		func(s *httptest.Server, w http.ResponseWriter) {
			s.CloseClientConnections()
		},
	}

	for i, testFunc := range testCases {
		count := 0
		var s *httptest.Server
		server, addr := startTestHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count++
			if count == 1 {
				// On first retry, invoke the error function.
				testFunc(s, w)
				return
			}
			// Success on second try.
			body, contentType, err := util.MarshalResponse(r, testPutResp, util.AllEncodings)
			if err != nil {
				t.Errorf("%d: failed to marshal response: %s", i, err)
			}
			w.Header().Set("Content-Type", contentType)
			w.Write(body)
		}))

		s = server
		sender, err := NewHTTPSender(addr, testutils.NewTestBaseContext())
		if err != nil {
			t.Fatal(err)
		}
		reply := &proto.PutResponse{}
		sender.Send(context.TODO(), Call{Args: testPutReq, Reply: reply})
		if reply.GoError() != nil {
			t.Errorf("%d: expected success; got %s", i, reply.GoError())
		}
		if count != 2 {
			t.Errorf("%d: expected retry", i)
		}
		server.Close()
	}
}
