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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

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

const testZoneConfig = `
replicas:
  - attrs: [dc1, ssd]
  - attrs: [dc2, ssd]
  - attrs: [dc3, ssd]
range_min_bytes: 1048576
range_max_bytes: 67108864
`

// ExampleSetAndGetZone sets zone configs for a variety of key
// prefixes and verifies they can be fetched directly.
func ExampleSetAndGetZone() {
	httpServer := startAdminServer()
	defer httpServer.Close()
	testConfigFn := createTestConfigFile(testZoneConfig)
	defer os.Remove(testConfigFn)

	testData := []struct {
		prefix proto.Key
		yaml   string
	}{
		{engine.KeyMin, testZoneConfig},
		{proto.Key("db1"), testZoneConfig},
		{proto.Key("db 2"), testZoneConfig},
		{proto.Key("\xfe"), testZoneConfig},
	}

	for _, test := range testData {
		prefix := url.QueryEscape(string(test.prefix))
		runSetZone(CmdSetZone, []string{prefix, testConfigFn})
		runGetZone(CmdGetZone, []string{prefix})
	}
	// Output:
	// set zone config for key prefix ""
	// zone config for key prefix "":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "db1"
	// zone config for key prefix "db1":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "db+2"
	// zone config for key prefix "db+2":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "%FE"
	// zone config for key prefix "%FE":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
}

// ExampleLsZones creates a series of zone configs and verifies
// zone-ls works. First, no regexp lists all zone configs. Second,
// regexp properly matches results.
func ExampleLsZones() {
	httpServer := startAdminServer()
	defer httpServer.Close()
	testConfigFn := createTestConfigFile(testZoneConfig)
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
		runSetZone(CmdSetZone, []string{prefix, testConfigFn})
	}

	for i, regexp := range regexps {
		fmt.Fprintf(os.Stdout, "test case %d: %q\n", i, regexp)
		if regexp == "" {
			runLsZones(CmdLsZones, []string{})
		} else {
			runLsZones(CmdLsZones, []string{regexp})
		}
	}
	// Output:
	// set zone config for key prefix ""
	// set zone config for key prefix "db1"
	// set zone config for key prefix "db2"
	// set zone config for key prefix "db3"
	// set zone config for key prefix "user"
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

// ExampleRmZones creates a series of zone configs and verifies
// zone-rm works by deleting some and then all and verifying entries
// have been removed via zone-ls. Also verify the default zone cannot
// be removed.
func ExampleRmZones() {
	httpServer := startAdminServer()
	defer httpServer.Close()
	testConfigFn := createTestConfigFile(testZoneConfig)
	defer os.Remove(testConfigFn)

	keys := []proto.Key{
		engine.KeyMin,
		proto.Key("db1"),
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		runSetZone(CmdSetZone, []string{prefix, testConfigFn})
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		runRmZone(CmdRmZone, []string{prefix})
		runLsZones(CmdLsZones, []string{})
	}
	// Output:
	// set zone config for key prefix ""
	// set zone config for key prefix "db1"
	// [default]
	// db1
	// removed zone config for key prefix "db1"
	// [default]
}

// ExampleZoneContentTypes verifies that the Accept header can be used
// to control the format of the response and the Content-Type header
// can be used to specify the format of the request.
func ExampleZoneContentTypes() {
	httpServer := startAdminServer()
	defer httpServer.Close()

	config := &proto.ZoneConfig{}
	err := yaml.Unmarshal([]byte(testZoneConfig), config)
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
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s%s", adminScheme, *addr, zonePathPrefix, key), bytes.NewReader(body))
		req.Header.Add("Content-Type", test.contentType)
		if _, err = sendAdminRequest(req); err != nil {
			fmt.Println(err)
		}

		req, err = http.NewRequest("GET", fmt.Sprintf("%s://%s%s%s", adminScheme, *addr, zonePathPrefix, key), nil)
		req.Header.Add("Accept", test.accept)
		if body, err = sendAdminRequest(req); err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(body))
	}
	// Output:
	// {
	//   "replica_attrs": [
	//     {
	//       "attrs": [
	//         "dc1",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc2",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc3",
	//         "ssd"
	//       ]
	//     }
	//   ],
	//   "range_min_bytes": 1048576,
	//   "range_max_bytes": 67108864
	// }
	// {
	//   "replica_attrs": [
	//     {
	//       "attrs": [
	//         "dc1",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc2",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc3",
	//         "ssd"
	//       ]
	//     }
	//   ],
	//   "range_min_bytes": 1048576,
	//   "range_max_bytes": 67108864
	// }
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
}
