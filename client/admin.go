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
// Author: Marc Berhault (marc@cockroachlabs.com)

package client

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// Types of admin config to manipulate.

	// Quit only handles Get requests.
	Quit = "quit"
)

// AdminClient issues http requests to admin endpoints.
// TODO(marc): unify the way we handle addresses in clients.
type AdminClient struct {
	context    *base.Context
	address    string
	configType string
}

// NewAdminClient returns a new 'configType' admin client, talking to 'address'.
func NewAdminClient(ctx *base.Context, address, configType string) (AdminClient, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return AdminClient{}, err
	}
	if len(host) == 0 {
		host = "localhost"
	}
	address = net.JoinHostPort(host, port)

	return AdminClient{ctx, address, configType}, nil
}

// adminURI builds a base URI for the embedded 'configType'
func (a *AdminClient) adminURI() string {
	return fmt.Sprintf("%s://%s/_admin/v1/%s", a.context.HTTPRequestScheme(),
		a.address, a.configType)
}

// Get issues a GET and returns the plain-text body. It cannot take a key.
func (a *AdminClient) Get() (string, error) {
	body, err := a.do("GET", a.adminURI(), "", util.PlaintextContentType, nil)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// do issues the http request to 'url' using 'method'.
// If 'contentType' is not empty, set the request header ContentType.
// If 'acceptContentType is not empty, set the request header Accept.
// Returns the response body.
// Errors out on transport problems and non-200 response codes.
func (a *AdminClient) do(method, url, contentType, acceptContentType string,
	body io.Reader) ([]byte, error) {
	client, err := a.context.GetHTTPClient()
	if err != nil {
		return nil, util.Errorf("failed to initialize %s client: %s",
			a.context.HTTPRequestScheme(), err)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if len(contentType) != 0 {
		req.Header.Add(util.ContentTypeHeader, contentType)
	}
	if len(acceptContentType) != 0 {
		req.Header.Add(util.AcceptHeader, acceptContentType)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, util.Errorf("%s request failed: %s, err: %s", req.Method, req.URL, err)
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, util.Errorf("failed to read response for %s %s: %s", req.Method, req.URL, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, util.Errorf("%s %s: got %s: %s", req.Method, req.URL, resp.Status, string(b))
	}
	return b, nil
}
