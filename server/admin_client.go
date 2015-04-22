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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cockroachdb/cockroach/util"
)

// sendAdminRequest send an HTTP request and processes the response for
// its body or error message if a non-200 response code.
func sendAdminRequest(ctx *Context, req *http.Request) ([]byte, error) {
	client, err := ctx.GetHTTPClient()
	if err != nil {
		return nil, util.Errorf("failed to initialized http client: %s", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, util.Errorf("admin REST request failed: %s", err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, util.Errorf("unable to read admin REST response: %s", err)
	}
	if resp.StatusCode != 200 {
		return nil, util.Errorf("%s: %s", resp.Status, string(b))
	}
	return b, nil
}

// SendQuit requests the admin quit path to drain and shutdown the server.
func SendQuit(ctx *Context) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s%s", adminScheme, ctx.Addr, quitPath), nil)
	if err != nil {
		return util.Errorf("unable to create request to admin REST endpoint: %s", err)
	}
	b, err := sendAdminRequest(ctx, req)
	if err != nil {
		return util.Errorf("admin REST request failed: %s", err)
	}

	fmt.Printf("node drained and shutdown: %s", string(b))

	return nil
}
