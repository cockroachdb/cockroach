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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	yaml "gopkg.in/yaml.v1"
)

const testAcctConfig = `cluster_id: test`

// ExampleSetAndGetAccts sets acct configs for a variety of key
// prefixes and verifies they can be fetched directly.
func ExampleSetAndGetAccts() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testConfigFn := createTestConfigFile(testAcctConfig)
	defer os.Remove(testConfigFn)

	testData := []struct {
		prefix proto.Key
		yaml   string
	}{
		{engine.KeyMin, testAcctConfig},
		{proto.Key("db1"), testAcctConfig},
		{proto.Key("db 2"), testAcctConfig},
		{proto.Key("\xfe"), testAcctConfig},
	}

	for _, test := range testData {
		prefix := url.QueryEscape(string(test.prefix))
		RunSetAcct(testContext, prefix, testConfigFn)
		RunGetAcct(testContext, prefix)
	}
	// Output:
	// set accounting config for key prefix ""
	// accounting config for key prefix "":
	// cluster_id: test
	//
	// set accounting config for key prefix "db1"
	// accounting config for key prefix "db1":
	// cluster_id: test
	//
	// set accounting config for key prefix "db+2"
	// accounting config for key prefix "db+2":
	// cluster_id: test
	//
	// set accounting config for key prefix "%FE"
	// accounting config for key prefix "%FE":
	// cluster_id: test
}

// ExampleLsAccts creates a series of acct configs and verifies
// acct-ls works. First, no regexp lists all acct configs. Second,
// regexp properly matches results.
func ExampleLsAccts() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testConfigFn := createTestConfigFile(testAcctConfig)
	defer os.Remove(testConfigFn)

	keys := []proto.Key{
		engine.KeyMin,
		proto.Key("db1"),
		proto.Key("db2"),
		proto.Key("db3"),
		proto.Key("user"),
	}

	regexps := []string{
		"",
		"db*",
		"db[12]",
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetAcct(testContext, prefix, testConfigFn)
	}

	for i, regexp := range regexps {
		fmt.Fprintf(os.Stdout, "test case %d: %q\n", i, regexp)
		if regexp == "" {
			RunLsAcct(testContext, "")
		} else {
			RunLsAcct(testContext, regexp)
		}
	}
	// Output:
	// set accounting config for key prefix ""
	// set accounting config for key prefix "db1"
	// set accounting config for key prefix "db2"
	// set accounting config for key prefix "db3"
	// set accounting config for key prefix "user"
	// test case 0: ""
	// [default]
	// db1
	// db2
	// db3
	// user
	// test case 1: "db*"
	// db1
	// db2
	// db3
	// test case 2: "db[12]"
	// db1
	// db2
}

// ExampleRmAccts creates a series of acct configs and verifies
// acct-rm works by deleting some and then all and verifying entries
// have been removed via acct-ls. Also verify the default acct config
// cannot be removed.
func ExampleRmAccts() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testConfigFn := createTestConfigFile(testAcctConfig)
	defer os.Remove(testConfigFn)

	keys := []proto.Key{
		engine.KeyMin,
		proto.Key("db1"),
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetAcct(testContext, prefix, testConfigFn)
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunRmAcct(testContext, prefix)
		RunLsAcct(testContext, "")
	}
	// Output:
	// set accounting config for key prefix ""
	// set accounting config for key prefix "db1"
	// [default]
	// db1
	// removed accounting config for key prefix "db1"
	// [default]
}

// ExampleAcctContentTypes verifies that the Accept header can be used
// to control the format of the response and the Content-Type header
// can be used to specify the format of the request.
func ExampleAcctContentTypes() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	config := &proto.AcctConfig{}
	err := yaml.Unmarshal([]byte(testAcctConfig), config)
	if err != nil {
		fmt.Println(err)
	}
	testCases := []struct {
		contentType, accept string
	}{
		{"application/json", "application/json"},
		{"text/yaml", "application/json"},
		{"application/json", "text/yaml"},
		{"text/yaml", "text/yaml"},
	}
	for i, test := range testCases {
		key := fmt.Sprintf("/test%d", i)

		var body []byte
		if test.contentType == "application/json" {
			if body, err = json.MarshalIndent(config, "", "  "); err != nil {
				fmt.Println(err)
			}
		} else {
			if body, err = yaml.Marshal(config); err != nil {
				fmt.Println(err)
			}
		}
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s%s", adminScheme, testContext.Addr, acctPathPrefix, key), bytes.NewReader(body))
		req.Header.Add("Content-Type", test.contentType)
		if _, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}

		req, err = http.NewRequest("GET", fmt.Sprintf("%s://%s%s%s", adminScheme, testContext.Addr, acctPathPrefix, key), nil)
		req.Header.Add("Accept", test.accept)
		if body, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(body))
	}
	// Output:
	// {
	//   "cluster_id": "test"
	// }
	// {
	//   "cluster_id": "test"
	// }
	// cluster_id: test
	//
	// cluster_id: test
}
