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

package httpclient

import (
	"crypto/tls"
	"net/http"
	"strings"

	"github.com/bufbuild/buf/private/pkg/observability"
	"github.com/bufbuild/buf/private/pkg/rpc/rpchttp"
)

type client struct {
	httpClient *http.Client

	tlsConfig     *tls.Config
	observability bool
}

func newClient(options ...ClientOption) *client {
	client := &client{}
	for _, option := range options {
		option(client)
	}
	roundTripper := rpchttp.NewClientInterceptor(
		&http.Transport{
			TLSClientConfig: client.tlsConfig,
		},
	)
	if client.observability {
		roundTripper = observability.NewHTTPTransport(roundTripper)
	}
	client.httpClient = &http.Client{
		Transport: roundTripper,
	}
	return client
}

func newClientWithTransport(transport http.RoundTripper) *client {
	return &client{
		httpClient: &http.Client{
			Transport: transport,
		},
	}
}

func (c *client) Do(request *http.Request) (*http.Response, error) {
	return c.httpClient.Do(request)
}

func (c *client) ParseAddress(address string) string {
	if len(strings.SplitN(address, "://", 2)) == 1 {
		if c.tlsConfig != nil {
			return "https://" + address
		}
		return "http://" + address
	}
	return address
}

func (c *client) Transport() http.RoundTripper {
	return c.httpClient.Transport
}
