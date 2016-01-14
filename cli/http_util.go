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
// Author: Cuong Do (cdo@cockroachlabs.com)

package cli

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/log"
)

var httpClient = &http.Client{
	Timeout: base.NetworkTimeout,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // TODO: find a better way of handling certs for tests
		},
	},
}

// Retrieves the URL specified by the parameters and and unmarshals the result into the supplied
// interface.
func getJSON(hostport, path string, v interface{}) error {

	url := fmt.Sprintf("%s://%s%s", context.HTTPRequestScheme(), hostport, path)
	if log.V(1) {
		log.Infof("Issuing request to %s", url)
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		if log.V(1) {
			log.Error(err)
		}
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if log.V(1) {
			log.Error(err)
		}
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		if log.V(1) {
			log.Errorf("%v\n%s", err, b)
		}
		return err
	}
	return nil
}
