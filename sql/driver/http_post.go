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
// Author: Spencer Kimball (spencer@cockroachlabs.com)
//         Vivek Menezes (vivek@cockroachlabs.com)

package driver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/cockroachdb/c-snappy"
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/retry"
)

const (
	// StatusTooManyRequests indicates client should retry due to
	// server having too many requests.
	StatusTooManyRequests = 429
)

// postContext is the context passed into the Post function
type postContext struct {
	Server    string // The host:port address of the Cockroach gateway node
	Endpoint  string
	Context   *base.Context // The base context: needed for client setup.
	RetryOpts retry.Options
}

// httpPost posts the req using the HTTP client. The call's method is
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
func httpPost(c postContext, request, response proto.Message, method fmt.Stringer) error {
	// Marshal the args into a request body.
	body, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	sharedClient, err := c.Context.GetHTTPClient()
	if err != nil {
		return err
	}

	// TODO(pmattis): Figure out the right thing to do here. The HTTP client we
	// get back from base.Context has a 3 second timeout. If a client operation
	// takes longer than that it will be retried. If we're not in an explicit
	// transaction the auto-transaction created on the server for the second
	// operation will be different than the still running first operation giving
	// them the potential to stomp on each other.
	client := *sharedClient
	client.Timeout = 60 * time.Second

	url := c.Context.HTTPRequestScheme() + "://" + c.Server + c.Endpoint + method.String()

	var (
		req  *http.Request
		resp *http.Response
		b    []byte
	)

	for r := retry.Start(c.RetryOpts); r.Next(); {
		req, err = http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Add(util.ContentTypeHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptEncodingHeader, util.SnappyEncoding)

		resp, err = client.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			// We're cool.
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout, StatusTooManyRequests:
			// Retry on service unavailable and request timeout.
			// TODO(spencer): consider respecting the Retry-After header for
			// backoff / retry duration.
			continue
		default:
			// Can't recover from all other errors.
			return errors.New(resp.Status)
		}

		if resp.Header.Get(util.ContentEncodingHeader) == util.SnappyEncoding {
			resp.Body = &snappyReader{body: resp.Body}
		}

		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			continue
		}
		if err = proto.Unmarshal(b, response); err != nil {
			log.Println(err)
			continue
		}

		break
	}
	return err
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
