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

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	yaml "gopkg.in/yaml.v1"
)

var testUserConfigBytes = fakeUserConfig()
var testUserConfig = string(testUserConfigBytes)

// We do not generate a real hashed password as that would make our
// example outputs very very long. Instead, just put two bytes.
func fakeUserConfig() []byte {
	pb := &proto.UserConfig{HashedPassword: []byte{10, 20}}
	contents, _ := yaml.Marshal(pb)
	return contents
}

// Example_setAndGetUsers sets user configs for a variety of key
// prefixes and verifies they can be fetched directly.
func Example_setAndGetUsers() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testData := []struct {
		prefix proto.Key
		yaml   string
	}{
		{proto.Key("user1"), testUserConfig},
		{proto.Key("user2"), testUserConfig},
		{proto.Key("user\xfe"), testUserConfig},
	}

	for _, test := range testData {
		prefix := url.QueryEscape(string(test.prefix))
		RunSetUser(testContext, prefix, testUserConfigBytes)
		RunGetUser(testContext, prefix)
	}
	// Output:
	// set user config for key prefix "user1"
	// user config for key prefix "user1":
	// hashed_password:
	// - 10
	// - 20
	//
	// set user config for key prefix "user2"
	// user config for key prefix "user2":
	// hashed_password:
	// - 10
	// - 20
	//
	// set user config for key prefix "user%FE"
	// user config for key prefix "user%FE":
	// hashed_password:
	// - 10
	// - 20
}

// Example_lsUsers creates a series of user configs and verifies
// user-ls works. First, no regexp lists all user configs. Second,
// regexp properly matches results.
func Example_lsUsers() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	keys := []proto.Key{
		proto.Key("user1"),
		proto.Key("user2"),
		proto.Key("user3"),
		proto.Key("foo"),
	}

	regexps := []string{
		"",
		"user*",
		"user[12]",
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetUser(testContext, prefix, testUserConfigBytes)
	}

	for i, regexp := range regexps {
		fmt.Fprintf(os.Stdout, "test case %d: %q\n", i, regexp)
		if regexp == "" {
			RunLsUser(testContext, "")
		} else {
			RunLsUser(testContext, regexp)
		}
	}
	// Output:
	// set user config for key prefix "user1"
	// set user config for key prefix "user2"
	// set user config for key prefix "user3"
	// set user config for key prefix "foo"
	// test case 0: ""
	// [default]
	// foo
	// user1
	// user2
	// user3
	// test case 1: "user*"
	// user1
	// user2
	// user3
	// test case 2: "user[12]"
	// user1
	// user2
}

// Example_rmUsers creates a series of user configs and verifies
// user-rm works by deleting some and then all and verifying entries
// have been removed via user-ls. Also verify the default user config
// cannot be removed.
func Example_rmUsers() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	keys := []proto.Key{
		proto.Key("user1"),
		proto.Key("user2"),
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetUser(testContext, prefix, testUserConfigBytes)
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunRmUser(testContext, prefix)
		RunLsUser(testContext, "")
	}
	// Output:
	// set user config for key prefix "user1"
	// set user config for key prefix "user2"
	// removed user config for key prefix "user1"
	// [default]
	// user2
	// removed user config for key prefix "user2"
	// [default]
}

// Example_userContentTypes verifies that the Accept header can be used
// to control the format of the response and the Content-Type header
// can be used to specify the format of the request.
func Example_userContentTypes() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	config := &proto.UserConfig{}
	err := yaml.Unmarshal(testUserConfigBytes, config)
	if err != nil {
		fmt.Println(err)
	}
	testCases := []struct {
		contentType, accept string
	}{
		{util.JSONContentType, util.JSONContentType},
		{util.YAMLContentType, util.JSONContentType},
		{util.JSONContentType, util.YAMLContentType},
		{util.YAMLContentType, util.YAMLContentType},
	}

	for i, test := range testCases {
		key := fmt.Sprintf("user%d", i)

		var body []byte
		if test.contentType == util.JSONContentType {
			if body, err = json.MarshalIndent(config, "", "  "); err != nil {
				fmt.Println(err)
			}
		} else {
			if body, err = yaml.Marshal(config); err != nil {
				fmt.Println(err)
			}
		}
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s/%s", testContext.RequestScheme(), testContext.Addr,
			userPathPrefix, key), bytes.NewReader(body))
		req.Header.Add(util.ContentTypeHeader, test.contentType)
		if _, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}

		req, err = http.NewRequest("GET", fmt.Sprintf("%s://%s%s/%s", testContext.RequestScheme(), testContext.Addr,
			userPathPrefix, key), nil)
		req.Header.Add(util.AcceptHeader, test.accept)
		if body, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(body))
	}
	// Output:
	// {
	//   "hashed_password": "ChQ="
	// }
	// {
	//   "hashed_password": "ChQ="
	// }
	// hashed_password:
	// - 10
	// - 20
	//
	// hashed_password:
	// - 10
	// - 20
}
