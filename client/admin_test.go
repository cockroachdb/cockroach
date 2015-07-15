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

package client_test

import (
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
)

// Example_accounting shows how to use the admin client to
// get/set/list/delete accounting configs.
//
// TODO(kkaneda,tschottdorf): This test is flaky. See #1684.
func Skip_Example_accounting() {
	s := server.StartTestServer(nil)
	defer s.Stop()

	context := testutils.NewRootTestBaseContext()
	client := client.NewAdminClient(context, s.ServingAddr(), client.Accounting)

	const yamlConfig = `cluster_id: test`
	const jsonConfig = `{
  "cluster_id": "test"
}`
	testData := []struct {
		prefix proto.Key
		cfg    string
		isJSON bool
	}{
		{proto.KeyMin, yamlConfig, false},
		{proto.Key("db1"), yamlConfig, false},
		{proto.Key("db 2"), jsonConfig, true},
		{proto.Key("\xfe"), jsonConfig, true},
	}

	// Write configs.
	for _, test := range testData {
		prefix := string(test.prefix)
		if test.isJSON {
			fmt.Printf("Set JSON accounting config for %q\n", prefix)
			if err := client.SetJSON(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		} else {
			fmt.Printf("Set YAML accounting config for %q\n", prefix)
			if err := client.SetYAML(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Get configs in various format.
	body, err := client.GetJSON("db1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("JSON config for \"db1\":\n%s\n", body)

	body, err = client.GetYAML("db 2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("YAML config for \"db 2\":\n%s\n", body)

	// List keys.
	keys, err := client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Accounting prefixes: %q\n", keys)

	// Remove keys: the default one cannot be removed.
	err = client.Delete("")
	if err == nil {
		log.Fatal("expected error")
	}
	err = client.Delete("db 2")
	if err != nil {
		log.Fatal(err)
	}

	// List keys again.
	keys, err = client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Accounting prefixes: %q\n", keys)

	// Output:
	// Set YAML accounting config for ""
	// Set YAML accounting config for "db1"
	// Set JSON accounting config for "db 2"
	// Set JSON accounting config for "\xfe"
	// JSON config for "db1":
	// {
	//   "cluster_id": "test"
	// }
	// YAML config for "db 2":
	// cluster_id: test
	//
	// Accounting prefixes: ["" "db 2" "db1" "\xfe"]
	// Accounting prefixes: ["" "db1" "\xfe"]
}

// Example_permission shows how to use the admin client to
// get/set/list/delete permission configs.
func Example_permission() {
	s := server.StartTestServer(nil)
	defer s.Stop()

	context := testutils.NewRootTestBaseContext()
	client := client.NewAdminClient(context, s.ServingAddr(), client.Permission)

	const yamlConfig = `
read: [readonly, readwrite]
write: [readwrite, writeonly]
`
	const jsonConfig = `{
	   "read": [
	     "readonly",
	     "readwrite"
	   ],
	   "write": [
	     "readwrite",
	     "writeonly"
	   ]
	 }`

	testData := []struct {
		prefix proto.Key
		cfg    string
		isJSON bool
	}{
		{proto.KeyMin, yamlConfig, false},
		{proto.Key("db1"), yamlConfig, false},
		{proto.Key("db 2"), jsonConfig, true},
		{proto.Key("\xfe"), jsonConfig, true},
	}

	// Write configs.
	for _, test := range testData {
		prefix := string(test.prefix)
		if test.isJSON {
			fmt.Printf("Set JSON permission config for %q\n", prefix)
			if err := client.SetJSON(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		} else {
			fmt.Printf("Set YAML permission config for %q\n", prefix)
			if err := client.SetYAML(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Get configs in various format.
	body, err := client.GetJSON("db1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("JSON config for \"db1\":\n%s\n", body)

	body, err = client.GetYAML("db 2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("YAML config for \"db 2\":\n%s\n", body)

	// List keys.
	keys, err := client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Permission prefixes: %q\n", keys)

	// Remove keys: the default one cannot be removed.
	err = client.Delete("")
	if err == nil {
		log.Fatal("expected error")
	}
	err = client.Delete("db 2")
	if err != nil {
		log.Fatal(err)
	}

	// List keys again.
	keys, err = client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Permission prefixes: %q\n", keys)

	// Output:
	// Set YAML permission config for ""
	// Set YAML permission config for "db1"
	// Set JSON permission config for "db 2"
	// Set JSON permission config for "\xfe"
	// JSON config for "db1":
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
	// YAML config for "db 2":
	// read:
	// - readonly
	// - readwrite
	// write:
	// - readwrite
	// - writeonly
	//
	// Permission prefixes: ["" "db 2" "db1" "\xfe"]
	// Permission prefixes: ["" "db1" "\xfe"]
}

// Example_user shows how to use the admin client to
// get/set/list/delete user configs.
func Example_user() {
	s := server.StartTestServer(nil)
	defer s.Stop()

	context := testutils.NewRootTestBaseContext()
	client := client.NewAdminClient(context, s.ServingAddr(), client.User)

	const yamlConfig = `hashed_password:
 - 10
 - 20`
	const jsonConfig = `{
	   "hashed_password": "ChQ="
	 }`
	testData := []struct {
		prefix proto.Key
		cfg    string
		isJSON bool
	}{
		{proto.Key("db1"), yamlConfig, false},
		{proto.Key("db 2"), jsonConfig, true},
		{proto.Key("\xfe"), jsonConfig, true},
	}

	// Overwriting the default entry fails.
	err := client.SetYAML("", yamlConfig)
	if err == nil {
		log.Fatal("expected error")
	}

	// Write configs.
	for _, test := range testData {
		prefix := string(test.prefix)
		if test.isJSON {
			fmt.Printf("Set JSON user config for %q\n", prefix)
			if err := client.SetJSON(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		} else {
			fmt.Printf("Set YAML user config for %q\n", prefix)
			if err := client.SetYAML(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Get configs in various format.
	body, err := client.GetJSON("db1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("JSON config for \"db1\":\n%s\n", body)

	body, err = client.GetYAML("db 2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("YAML config for \"db 2\":\n%s\n", body)

	// List keys.
	keys, err := client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Users: %q\n", keys)

	// Remove keys: the default one cannot be removed.
	err = client.Delete("")
	if err == nil {
		log.Fatal("expected error")
	}
	err = client.Delete("db 2")
	if err != nil {
		log.Fatal(err)
	}

	// List keys again.
	keys, err = client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Users: %q\n", keys)

	// Output:
	// Set YAML user config for "db1"
	// Set JSON user config for "db 2"
	// Set JSON user config for "\xfe"
	// JSON config for "db1":
	// {
	//   "hashed_password": "ChQ="
	// }
	// YAML config for "db 2":
	// hashed_password:
	// - 10
	// - 20
	//
	// Users: ["" "db 2" "db1" "\xfe"]
	// Users: ["" "db1" "\xfe"]
}

// Example_zone shows how to use the admin client to
// get/set/list/delete zone configs.
//
// TODO(kkaneda,tschottdorf): This test is flaky. See #1684.
func Skip_Example_zone() {
	s := server.StartTestServer(nil)
	defer s.Stop()

	context := testutils.NewRootTestBaseContext()
	client := client.NewAdminClient(context, s.ServingAddr(), client.Zone)

	const yamlConfig = `
replicas:
  - attrs: [dc1, ssd]
  - attrs: [dc2, ssd]
  - attrs: [dc3, ssd]
range_min_bytes: 1048576
range_max_bytes: 67108864
`
	const jsonConfig = `{
	   "replica_attrs": [
	     {
	       "attrs": [
	         "dc1",
	         "ssd"
	       ]
	     },
	     {
	       "attrs": [
	         "dc2",
	         "ssd"
	       ]
	     },
	     {
	       "attrs": [
	         "dc3",
	         "ssd"
	       ]
	     }
	   ],
	   "range_min_bytes": 1048576,
	   "range_max_bytes": 67108864
	 }`

	testData := []struct {
		prefix proto.Key
		cfg    string
		isJSON bool
	}{
		{proto.KeyMin, yamlConfig, false},
		{proto.Key("db1"), yamlConfig, false},
		{proto.Key("db 2"), jsonConfig, true},
		{proto.Key("\xfe"), jsonConfig, true},
	}

	// Write configs.
	for _, test := range testData {
		prefix := string(test.prefix)
		if test.isJSON {
			fmt.Printf("Set JSON zone config for %q\n", prefix)
			if err := client.SetJSON(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		} else {
			fmt.Printf("Set YAML zone config for %q\n", prefix)
			if err := client.SetYAML(prefix, test.cfg); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Get configs in various format.
	body, err := client.GetJSON("db1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("JSON config for \"db1\":\n%s\n", body)

	body, err = client.GetYAML("db 2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("YAML config for \"db 2\":\n%s\n", body)

	// List keys.
	keys, err := client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Zone prefixes: %q\n", keys)

	// Remove keys: the default one cannot be removed.
	err = client.Delete("")
	if err == nil {
		log.Fatal("expected error")
	}
	err = client.Delete("db 2")
	if err != nil {
		log.Fatal(err)
	}

	// List keys again.
	keys, err = client.List()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Zone prefixes: %q\n", keys)

	// Output:
	// Set YAML zone config for ""
	// Set YAML zone config for "db1"
	// Set JSON zone config for "db 2"
	// Set JSON zone config for "\xfe"
	// JSON config for "db1":
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
	// YAML config for "db 2":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// Zone prefixes: ["" "db 2" "db1" "\xfe"]
	// Zone prefixes: ["" "db1" "\xfe"]
}
