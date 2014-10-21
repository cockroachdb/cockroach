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
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
)

const testAcctConfig = `cluster_id: test`

// ExampleSetAndGetAccts sets acct configs for a variety of key
// prefixes and verifies they can be fetched directly.
func ExampleSetAndGetAccts() {
	httpServer := startAdminServer()
	defer httpServer.Close()
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
		runSetAcct(CmdSetAcct, []string{prefix, testConfigFn})
		runGetAcct(CmdGetAcct, []string{prefix})
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
	httpServer := startAdminServer()
	defer httpServer.Close()
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
		runSetAcct(CmdSetAcct, []string{prefix, testConfigFn})
	}

	for i, regexp := range regexps {
		fmt.Fprintf(os.Stdout, "test case %d: %q\n", i, regexp)
		if regexp == "" {
			runLsAccts(CmdLsAccts, []string{})
		} else {
			runLsAccts(CmdLsAccts, []string{regexp})
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
	httpServer := startAdminServer()
	defer httpServer.Close()
	testConfigFn := createTestConfigFile(testAcctConfig)
	defer os.Remove(testConfigFn)

	keys := []proto.Key{
		engine.KeyMin,
		proto.Key("db1"),
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		runSetAcct(CmdSetAcct, []string{prefix, testConfigFn})
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		runRmAcct(CmdRmAcct, []string{prefix})
		runLsAccts(CmdLsAccts, []string{})
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
	httpServer := startAdminServer()
	defer httpServer.Close()

	config, err := proto.AcctConfigFromYAML([]byte(testAcctConfig))
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
			if body, err = config.ToJSON(); err != nil {
				fmt.Println(err)
			}
		} else {
			if body, err = config.ToYAML(); err != nil {
				fmt.Println(err)
			}
		}
		req, err := http.NewRequest("POST", kv.HTTPAddr()+acctKeyPrefix+key, bytes.NewReader(body))
		req.Header.Add("Content-Type", test.contentType)
		if _, err = sendAdminRequest(req); err != nil {
			fmt.Println(err)
		}

		req, err = http.NewRequest("GET", kv.HTTPAddr()+acctKeyPrefix+key, nil)
		req.Header.Add("Accept", test.accept)
		if body, err = sendAdminRequest(req); err != nil {
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
