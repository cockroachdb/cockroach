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
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// KVDBEndpoint is the URL path prefix which accepts incoming
	// HTTP requests for the KV API.
	KVDBEndpoint = "/kv/db/"
	// KVDBScheme is the scheme for connecting to the kvdb endpoint.
	// TODO(spencer): change this to CONSTANT https. We shouldn't be
	// supporting http here at all.
	KVDBScheme = "http"
	// defaultMaxIdleConns controls the maximum idle (keep-alive)
	// connections to the Cockroach gateway node (or redirects).  A
	// higher value here means lower latency when activity spikes,
	// especially in light of the latency required to establish a new
	// SSL connection.
	defaultMaxIdleConns = 5
)

// HTTPRetryOptions sets the retry options for handling retryable
// HTTP errors and connection I/O errors.
var HTTPRetryOptions = util.RetryOptions{
	Backoff:     50 * time.Millisecond,
	MaxBackoff:  5 * time.Second,
	Constant:    2,
	MaxAttempts: 0, // retry indefinitely
}

// httpSendError wraps any error returned when sending an HTTP request
// in order to signal the retry loop that it should backoff and retry.
type httpSendError struct {
	error
}

// httpSender is an implementation of KVSender which exposes the
// Key-Value database provided by a Cockroach cluster by connecting
// via HTTP to a Cockroach node. Overly-busy nodes will redirect
// this client to other nodes.
type httpSender struct {
	server string       // The host:port address of the Cockroach gateway node
	client *http.Client // The HTTP client
}

// newHTTPSender returns a new instance of httpSender.
func newHTTPSender(server string, tlsConfig *tls.Config) *httpSender {
	return &httpSender{
		server: server,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     tlsConfig,
				MaxIdleConnsPerHost: defaultMaxIdleConns,
			},
		},
	}
}

// Send sends call to Cockroach via an HTTP post. HTTP response codes
// which are retryable are retried with backoff in a loop using the
// default retry options. Other errors sending HTTP request are
// retried indefinitely using the same client command ID to avoid
// reporting failure when in fact the command may have gone through
// and been executed successfully. We retry here to eventually get
// through with the same client command ID and be given the cached
// response.
func (s *httpSender) Send(call *Call) {
	retryOpts := HTTPRetryOptions
	retryOpts.Tag = fmt.Sprintf("http %s", call.Method)
	if err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		resp, err := s.post(call)
		if err != nil {
			if resp != nil {
				// See if we can retry based on HTTP response code.
				switch resp.StatusCode {
				case http.StatusServiceUnavailable, http.StatusRequestTimeout,
					http.StatusMovedPermanently, http.StatusTemporaryRedirect:
					// Retry on service unavailable and request timeout. We also
					// try on the redirection statuses because of the redirect
					// limit in the default http.Client CheckRedirect
					// implementation. We want to backoff in case the cluster is
					// giving us the run around with endless redirects.
					return util.RetryContinue, nil
				default:
					// Can't recover from all other errors.
					return util.RetryBreak, err
				}
			}
			switch t := err.(type) {
			case *httpSendError:
				// Assume all errors sending request are retryable. The actual
				// number of things that could go wrong is vast, but we don't
				// want to miss any which should in theory be retried with
				// the same client command ID. We log the error here as a
				// warning so there's visiblity that this is happening. Some of
				// the errors we'll sweep up in this net shouldn't be retried,
				// but we can't really know for sure which.
				log.Warningf("failed to send HTTP request or read its response: %s", t)
				return util.RetryContinue, nil
			default:
				// Can't retry in order to recover from this error. Propagate.
				return util.RetryBreak, err
			}
		}
		// On successful post, we're done with retry loop.
		return util.RetryBreak, nil
	}); err != nil {
		call.Reply.Header().SetGoError(err)
	}
}

// post posts the call using the HTTP client. The call's method is
// appended to KVDBEndpoint and set as the URL path. The call's arguments
// are protobuf-serialized and written as the POST body. The content
// type is set to application/x-protobuf.
//
// On success, the response body is unmarshalled into call.Reply.
func (s *httpSender) post(call *Call) (*http.Response, error) {
	// Marshal the args into a request body.
	body, err := gogoproto.Marshal(call.Args)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s://%s%s%s", KVDBScheme, s.server, KVDBEndpoint, call.Method)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, util.Errorf("unable to create request: %s", err)
	}
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("Accept", "application/x-protobuf")
	resp, err := s.client.Do(req)
	if resp == nil {
		fmt.Println("connection closed")
		return nil, &httpSendError{util.Errorf("http client was closed")}
	}
	defer resp.Body.Close()
	if err != nil {
		fmt.Println("response error")
		return nil, &httpSendError{err}
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("response read error")
		return nil, &httpSendError{err}
	}
	if resp.StatusCode != 200 {
		return resp, errors.New(resp.Status)
	}
	if err := gogoproto.Unmarshal(b, call.Reply); err != nil {
		fmt.Println("unmarshal error")
		log.Errorf("request completed, but unable to unmarshal response from server: %s; body=%q", err, b)
		return nil, &httpSendError{err}
	}
	return resp, nil
}

// Close sets the client to nil.
func (s *httpSender) Close() {
	s.client = nil
}
