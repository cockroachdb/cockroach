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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/storage"
	"github.com/golang/glog"
)

const (
	testConfig = `
replicas:
  dc1:
  - SSD
range_min_bytes: 1048576
range_max_bytes: 67108864
`
)

// createTestConfigFile creates a temporary file and writes the
// testConfig yaml data to it. The caller is responsible for
// removing it. Returns the filename for a subsequent call to
// os.Remove().
func createTestConfigFile() string {
	f, err := ioutil.TempFile("", "test-config")
	if err != nil {
		glog.Fatalf("failed to open temporary file: %v", err)
	}
	defer f.Close()
	f.Write([]byte(testConfig))
	return f.Name()
}

// ExampleSetAndGetZone sets zone configs for a variety of key
// prefixes and verifies they can be fetched directly.
func ExampleSetAndGetZone() {
	httpServer := startAdminServer()
	defer httpServer.Close()
	testConfigFn := createTestConfigFile()
	defer os.Remove(testConfigFn)

	testData := []struct {
		prefix storage.Key
		yaml   string
	}{
		{storage.KeyMin, testConfig},
		{storage.Key("db1"), testConfig},
		{storage.Key("db 2"), testConfig},
		{storage.Key("\xfe"), testConfig},
	}

	for _, test := range testData {
		prefix := url.QueryEscape(string(test.prefix))
		runSetZone(CmdSetZone, []string{prefix, testConfigFn})
		runGetZones(CmdGetZone, []string{prefix})
	}
	// Output:
	// set zone config for key prefix ""
	// zone config for key prefix "":
	// replicas:
	//   dc1:
	//   - SSD
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "db1"
	// zone config for key prefix "db1":
	// replicas:
	//   dc1:
	//   - SSD
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "db+2"
	// zone config for key prefix "db+2":
	// replicas:
	//   dc1:
	//   - SSD
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "%FE"
	// zone config for key prefix "%FE":
	// replicas:
	//   dc1:
	//   - SSD
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
}

// ExampleLsZones creates a series of zone configs and verifies
// zone-ls works. First, no regexp lists all zone configs. Second,
// regexp properly matches results.
func ExampleLsZones() {
	httpServer := startAdminServer()
	defer httpServer.Close()
	testConfigFn := createTestConfigFile()
	defer os.Remove(testConfigFn)

	keys := []storage.Key{
		storage.KeyMin,
		storage.Key("db1"),
		storage.Key("db2"),
		storage.Key("db3"),
		storage.Key("user"),
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
	testConfigFn := createTestConfigFile()
	defer os.Remove(testConfigFn)

	keys := []storage.Key{
		storage.KeyMin,
		storage.Key("db1"),
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
