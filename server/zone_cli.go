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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

// sendAdminRequest send an HTTP request and processes the response for
// its body or error message if a non-200 response code.
func sendAdminRequest(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return nil, util.Errorf("admin REST request failed: %v\n", err)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, util.Errorf("unable to read admin REST response: %v\n", err)
	}
	if resp.StatusCode != 200 {
		return nil, util.Errorf("%s: %s\n", resp.Status, string(b))
	}
	return b, nil
}

// A CmdGetZone command displays the zone config for the specified
// prefix.
var CmdGetZone = &commander.Command{
	UsageLine: "get-zone [options] <key-prefix>",
	Short:     "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run: runGetZones}

// runGetZones invokes the REST API with GET action and key prefix as path.
func runGetZones(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	req, err := http.NewRequest("GET", kv.HTTPAddr()+zoneKeyPrefix+"/"+args[0], nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create request to admin REST endpoint: %v\n", err)
		return
	}
	// TODO(spencer): need to move to SSL.
	b, err := sendAdminRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin REST request failed: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stdout, "zone config for key prefix %q:\n%s\n", args[0], string(b))
}

// A CmdLsZones command displays a list of zone configs by prefix.
var CmdLsZones = &commander.Command{
	UsageLine: "ls-zones [options] [key-regexp]",
	Short:     "list all zone configs by key prefix",
	Long: `
List zone configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run: runLsZones}

// runLsZones invokes the REST API with GET action and no path, which
// fetches a list of all zone configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsZones(cmd *commander.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	req, err := http.NewRequest("GET", kv.HTTPAddr()+zoneKeyPrefix, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create request to admin REST endpoint: %v\n", err)
		return
	}
	b, err := sendAdminRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin REST request failed: %v\n", err)
		return
	}
	var prefixes []string
	if err = json.Unmarshal(b, &prefixes); err != nil {
		fmt.Printf("unable to parse admin REST response: %v\n", err)
	}
	var re *regexp.Regexp
	if len(args) == 1 {
		if re, err = regexp.Compile(args[0]); err != nil {
			glog.Warningf("invalid regular expression %q; skipping regexp match and listing all zone prefixes", args[0])
			re = nil
		}
	}
	for _, prefix := range prefixes {
		if re != nil {
			unescaped, err := url.QueryUnescape(prefix)
			if err != nil || !re.MatchString(unescaped) {
				continue
			}
		}
		if prefix == "" {
			prefix = "[default]"
		}
		fmt.Fprintf(os.Stdout, "%s\n", prefix)
	}
}

// A CmdRmZone command removes a zone config by prefix.
var CmdRmZone = &commander.Command{
	UsageLine: "rm-zone [options] <key-prefix>",
	Short:     "remove a zone config by key prefix",
	Long: `
Remove an existing zone config by key prefix. No action is taken if no
zone configuration exists for the specified key prefix. Note that this
command can affect only a single zone config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run: runRmZone}

// runRmZone invokes the REST API with DELETE action and key prefix as
// path.
func runRmZone(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	req, err := http.NewRequest("DELETE", kv.HTTPAddr()+zoneKeyPrefix+"/"+args[0], nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create request to admin REST endpoint: %v\n", err)
		return
	}
	// TODO(spencer): need to move to SSL.
	_, err = sendAdminRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin REST request failed: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stdout, "removed zone config for key prefix %q\n", args[0])
}

// A CmdSetZone command creates a new or updates an existing zone
// config.
var CmdSetZone = &commander.Command{
	UsageLine: "set-zone [options] <key-prefix> <zone-config-file>",
	Short:     "create or update zone config for key prefix",
	Long: `
Create or update a zone config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <zone-config-file>). The key prefix should be
escaped via URL query escaping if it contains non-ascii bytes or
spaces.

The zone config format has the following YAML schema:

  replicas:
    <datacenter>:
    - {SSD|HDD|MEM}
    - ...
    <datacenter>:
    - {SSD|HDD|MEM}
    - ...
    <...>:
    - {SSD|HDD|MEM}
    - ...
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>

Setting zone configs will guarantee that key ranges will be split
such that no key range straddles two zone config specifications.
This feature can be taken advantage of to pre-split ranges.
`,
	Run: runSetZone}

// runSetZone invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetZone(cmd *commander.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	// Read in the config file.
	body, err := ioutil.ReadFile(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to read zone config file %q: %v\n", args[1], err)
		return
	}
	req, err := http.NewRequest("POST", kv.HTTPAddr()+zoneKeyPrefix+"/"+args[0], bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create request to admin REST endpoint: %v\n", err)
		return
	}
	// TODO(spencer): need to move to SSL.
	_, err = sendAdminRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "admin REST request failed: %v\n", err)
		return
	}
	fmt.Fprintf(os.Stdout, "set zone config for key prefix %q\n", args[0])
}
