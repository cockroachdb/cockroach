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
// Author: Marc Berhault (marc@cockroachlabs.com)

package client

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// Types of admin config to manipulate.

	// Accounting expects proto.AcctConfig
	Accounting = "acct"
	// Permission expects proto.PermConfig
	Permission = "perms"
	// Quit only handles Get requests.
	Quit = "quit"
	// User expects proto.UserConfig
	User = "users"
	// Zone expects proto.ZoneConfig.
	Zone = "zones"
)

// AdminClient issues http requests to admin endpoints.
// TODO(marc): unify the way we handle addresses in clients.
type AdminClient struct {
	context    *base.Context
	address    string
	configType string
}

// NewAdminClient returns a new 'configType' admin client, talking to 'address'.
func NewAdminClient(ctx *base.Context, address, configType string) AdminClient {
	return AdminClient{ctx, address, configType}
}

// adminURI builds a base URI for the embedded 'configType'
func (a *AdminClient) adminURI() string {
	return fmt.Sprintf("%s://%s/_admin/%s", a.context.RequestScheme(),
		a.address, a.configType)
}

// adminURIWithKey builds a URI for the 'configType' and key.
func (a *AdminClient) adminURIWithKey(key string) string {
	return fmt.Sprintf("%s/%s", a.adminURI(), url.QueryEscape(key))
}

// Get issues a GET and returns the plain-text body. It cannot take a key.
func (a *AdminClient) Get() (string, error) {
	body, err := a.do("GET", a.adminURI(), "", util.PlaintextContentType, nil)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// GetJSON issues a GET request and returns a json-encoded response.
func (a *AdminClient) GetJSON(key string) (string, error) {
	body, err := a.do("GET", a.adminURIWithKey(key), "", util.JSONContentType, nil)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// GetYAML issues a GET request and returns a yaml-encoded response.
func (a *AdminClient) GetYAML(key string) (string, error) {
	body, err := a.do("GET", a.adminURIWithKey(key), "", util.YAMLContentType, nil)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// SetJSON issues a POST request for the given key using the json-encoded body.
func (a *AdminClient) SetJSON(key, body string) error {
	_, err := a.do("POST", a.adminURIWithKey(key), util.JSONContentType,
		"", strings.NewReader(body))
	return err
}

// SetYAML issues a POST request for the given key using the yaml-encoded body.
func (a *AdminClient) SetYAML(key, body string) error {
	_, err := a.do("POST", a.adminURIWithKey(key), util.YAMLContentType,
		"", strings.NewReader(body))
	return err
}

// List issues a GET request to list all configs. Returns a list of keys.
func (a *AdminClient) List() ([]string, error) {
	body, err := a.do("GET", a.adminURI(), "", util.JSONContentType, nil)
	if err != nil {
		return nil, err
	}
	type wrapper struct {
		Data []string `json:"d"`
	}
	var w wrapper
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, util.Errorf("unable to parse response %q: %s", body, err)
	}

	// Make sure we unescape all keys.
	for i, val := range w.Data {
		w.Data[i], err = url.QueryUnescape(val)
		if err != nil {
			return nil, util.Errorf("unable to unescape %q: %s", val, err)
		}
	}
	return w.Data, nil
}

// Delete issues a DELETE request for the given key.
func (a *AdminClient) Delete(key string) error {
	_, err := a.do("DELETE", a.adminURIWithKey(key), "", "", nil)
	return err
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
			a.context.RequestScheme(), err)
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
