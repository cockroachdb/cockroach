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
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	yaml "gopkg.in/yaml.v1"
)

const testPermConfig = `
read: [readonly, readwrite]
write: [readwrite, writeonly]
`

// ExampleSetAndGetPerm sets perm configs for a variety of key
// prefixes and verifies they can be fetched directly.
func ExampleSetAndGetPerms() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testConfigFn := createTestConfigFile(testPermConfig)
	defer os.Remove(testConfigFn)

	testData := []struct {
		prefix proto.Key
		yaml   string
	}{
		{engine.KeyMin, testPermConfig},
		{proto.Key("db1"), testPermConfig},
		{proto.Key("db 2"), testPermConfig},
		{proto.Key("\xfe"), testPermConfig},
	}

	for _, test := range testData {
		prefix := url.QueryEscape(string(test.prefix))
		RunSetPerm(testContext, prefix, testConfigFn)
		RunGetPerm(testContext, prefix)
	}
	// Output:
	// set permission config for key prefix ""
	// permission config for key prefix "":
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
	//
	// set permission config for key prefix "db1"
	// permission config for key prefix "db1":
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
	//
	// set permission config for key prefix "db+2"
	// permission config for key prefix "db+2":
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
	//
	// set permission config for key prefix "%FE"
	// permission config for key prefix "%FE":
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
}

// ExampleLsPerms creates a series of perm configs and verifies
// perm-ls works. First, no regexp lists all perm configs. Second,
// regexp properly matches results.
func ExampleLsPerms() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testConfigFn := createTestConfigFile(testPermConfig)
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
		RunSetPerm(testContext, prefix, testConfigFn)
	}

	for i, regexp := range regexps {
		fmt.Fprintf(os.Stdout, "test case %d: %q\n", i, regexp)
		if regexp == "" {
			RunLsPerm(testContext, "")
		} else {
			RunLsPerm(testContext, regexp)
		}
	}
	// Output:
	// set permission config for key prefix ""
	// set permission config for key prefix "db1"
	// set permission config for key prefix "db2"
	// set permission config for key prefix "db3"
	// set permission config for key prefix "user"
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

// ExampleRmPerms creates a series of perm configs and verifies
// perm-rm works by deleting some and then all and verifying entries
// have been removed via perm-ls. Also verify the default perm config
// cannot be removed.
func ExampleRmPerms() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testConfigFn := createTestConfigFile(testPermConfig)
	defer os.Remove(testConfigFn)

	keys := []proto.Key{
		engine.KeyMin,
		proto.Key("db1"),
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetPerm(testContext, prefix, testConfigFn)
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunRmPerm(testContext, prefix)
		RunLsPerm(testContext, "")
	}
	// Output:
	// set permission config for key prefix ""
	// set permission config for key prefix "db1"
	// [default]
	// db1
	// removed permission config for key prefix "db1"
	// [default]
}

// ExamplePermContentTypes verifies that the Accept header can be used
// to control the format of the response and the Content-Type header
// can be used to specify the format of the request.
func ExamplePermContentTypes() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	config := &proto.PermConfig{}
	err := yaml.Unmarshal([]byte(testPermConfig), config)
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
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s%s", testContext.RequestScheme(), testContext.Addr,
			permPathPrefix, key), bytes.NewReader(body))
		req.Header.Add("Content-Type", test.contentType)
		if _, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}

		req, err = http.NewRequest("GET", fmt.Sprintf("%s://%s%s%s", testContext.RequestScheme(), testContext.Addr,
			permPathPrefix, key), nil)
		req.Header.Add("Accept", test.accept)
		if body, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(body))
	}
	// Output:
	// {
	//   "read": [
	//     "readonly",
	//     "readwrite"
	//   ],
	//   "write": [
	//     "readwrite",
	//     "writeonly"
	//   ]
	// }
	// {
	//   "read": [
	//     "readonly",
	//     "readwrite"
	//   ],
	//   "write": [
	//     "readwrite",
	//     "writeonly"
	//   ]
	// }
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
	//
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
}

// TestPermEmptyKey verifies that the Accept header can be used
// to control the format of the response when a key is empty.
func TestPermEmptyKey(t *testing.T) {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	config := &proto.PermConfig{}
	err := yaml.Unmarshal([]byte(testPermConfig), config)
	if err != nil {
		t.Fatal(err)
	}

	body, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"key0", "key1"}
	for _, key := range keys {
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s/%s", testContext.RequestScheme(), testContext.Addr,
			permPathPrefix, key), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if _, err = sendAdminRequest(testContext, req); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		accept, expBody string
	}{
		{"application/json", `[
  "",
  "key0",
  "key1"
]`},
		{"text/yaml", `- ""
- key0
- key1
`},
		{"application/x-protobuf", `[
  "",
  "key0",
  "key1"
]`},
	}

	for i, test := range testCases {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s%s", testContext.RequestScheme(), testContext.Addr,
			permPathPrefix), nil)
		req.Header.Set("Accept", test.accept)
		body, err = sendAdminRequest(testContext, req)
		if err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		if string(body) != test.expBody {
			t.Errorf("%d: expected %q; got %q", i, test.expBody, body)
		}
	}
}
