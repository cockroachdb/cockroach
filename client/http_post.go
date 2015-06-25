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
// Author: Spencer Kimball (spencer@cockroachlabs.com)
//         Vivek Menezes (vivek@cockroachlabs.com)

package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/retry"
	gogoproto "github.com/gogo/protobuf/proto"
)

// PostContext is the context passed into the Post function
type PostContext struct {
	Server    string // The host:port address of the Cockroach gateway node
	Endpoint  string
	Client    *http.Client  // The HTTP client
	Context   *base.Context // The base context: needed for client setup.
	RetryOpts retry.Options
}

// HTTPPost posts the req using the HTTP client. The call's method is
// appended to the endpoint and set as the URL path. The call's arguments
// are protobuf-serialized and written as the POST body. The content
// type is set to application/x-protobuf.
//
// On success, the response body is unmarshalled into call.Reply.
//
// HTTP response codes which are retryable are retried with backoff in a loop
// using the default retry options. Other errors sending HTTP request are
// retried indefinitely using the same client command ID to avoid reporting
// failure when in fact the command may go through and execute successfully. We
// retry here to eventually get through with the same client command ID and be
// given the cached response.
func HTTPPost(c PostContext, request, response gogoproto.Message, method fmt.Stringer) error {
	retryOpts := c.RetryOpts
	retryOpts.Tag = fmt.Sprintf("%s %s", c.Context.RequestScheme(), method)

	// Marshal the args into a request body.
	body, err := gogoproto.Marshal(request)
	if err != nil {
		return err
	}

	url := c.Context.RequestScheme() + "://" + c.Server + c.Endpoint + method.String()

	return retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return retry.Break, err
		}
		req.Header.Add(util.ContentTypeHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptEncodingHeader, util.SnappyEncoding)

		resp, err := c.Client.Do(req)
		if err != nil {
			return retry.Continue, err
		}

		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			// We're cool.
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout, StatusTooManyRequests:
			// Retry on service unavailable and request timeout.
			// TODO(spencer): consider respecting the Retry-After header for
			// backoff / retry duration.
			return retry.Continue, errors.New(resp.Status)
		default:
			// Can't recover from all other errors.
			return retry.Break, errors.New(resp.Status)
		}

		if resp.Header.Get(util.ContentEncodingHeader) == util.SnappyEncoding {
			resp.Body = &snappyReader{body: resp.Body}
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return retry.Continue, err
		}

		if err := gogoproto.Unmarshal(b, response); err != nil {
			return retry.Continue, err
		}

		return retry.Break, nil
	})
}

// snappyReader wraps a response body so it can lazily
// call snappy.NewReader on the first call to Read
type snappyReader struct {
	body io.ReadCloser // underlying Response.Body
	sr   io.Reader     // lazily-initialized snappy reader
}

func (s *snappyReader) Read(p []byte) (n int, err error) {
	if s.sr == nil {
		s.sr = snappy.NewReader(s.body)
	}
	return s.sr.Read(p)
}

func (s *snappyReader) Close() error {
	return s.body.Close()
}
