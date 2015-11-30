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
// Author: Peter Mattis (peter@cockroachlabs.com)

package cluster

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/log"
)

// HTTPClient is an http.Client configured for querying a cluster. We need to
// run with "InsecureSkipVerify" (at least on Docker) due to the fact that we
// cannot use a fixed hostname to reach the cluster. This in turn means that we
// do not have a verified server name in the certs.
var HTTPClient = http.Client{
	Timeout: base.NetworkTimeout,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}}

// getJSON retrieves the URL specified by the parameters and
// and unmarshals the result into the supplied interface.
func getJSON(tls bool, hostport, path string, v interface{}) error {
	protocol := "https"
	if !tls {
		protocol = "http"
	}
	resp, err := HTTPClient.Get(fmt.Sprintf("%s://%s%s", protocol, hostport, path))
	if err != nil {
		if log.V(1) {
			log.Info(err)
		}
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if log.V(1) {
			log.Info(err)
		}
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		if log.V(1) {
			log.Info(err)
		}
	}
	return nil
}
