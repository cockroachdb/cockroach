// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpchttp

import (
	"net/http"
	"strings"

	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/rpc/rpcheader"
)

// NewServerInterceptor returns a new server interceptor for http.
//
// This should be the last interceptor installed, except for twirp.
func NewServerInterceptor() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if len(request.Header) > 0 {
				request = request.WithContext(
					rpc.WithIncomingHeaders(
						request.Context(),
						fromHTTPHeader(
							request.Header,
						),
					),
				)
			}
			next.ServeHTTP(writer, request)
		})
	}
}

// NewClientInterceptor returns a new client interceptor for http.
//
// This should be the last interceptor installed, except for twirp.
func NewClientInterceptor(next http.RoundTripper) http.RoundTripper {
	return newHTTPRoundTripper(next)
}

type httpRoundTripper struct {
	next http.RoundTripper
}

func newHTTPRoundTripper(next http.RoundTripper) *httpRoundTripper {
	return &httpRoundTripper{
		next: next,
	}
}

func (h *httpRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if headers := rpc.GetOutgoingHeaders(request.Context()); len(headers) > 0 {
		for key, value := range headers {
			request.Header.Add(rpcheader.KeyPrefix+key, value)
		}
	}
	return h.next.RoundTrip(request)
}

func fromHTTPHeader(httpHeader http.Header) map[string]string {
	headers := make(map[string]string)
	for key, values := range httpHeader {
		key = strings.ToLower(key)
		// prefix so that we strip out other headers
		// rpc clients and servers should only be aware of headers set with the rpc package
		if strings.HasPrefix(key, rpcheader.KeyPrefix) {
			if key := strings.TrimPrefix(key, rpcheader.KeyPrefix); key != "" {
				headers[key] = values[0]
			}
		}
	}
	return headers
}
