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
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// sendAdminRequest send an HTTP request and processes the response for
// its body or error message if a non-200 response code.
func sendAdminRequest(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		return nil, util.Errorf("admin REST request failed: %s", err)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, util.Errorf("unable to read admin REST response: %s", err)
	}
	if resp.StatusCode != 200 {
		return nil, util.Errorf("%s: %s", resp.Status, string(b))
	}
	return b, nil
}

// Gets a friendly name for output based on the passed in config prefix.
func getFriendlyNameFromPrefix(prefix string) string {
	switch prefix {
	case zoneKeyPrefix:
		return "zone"
	case permKeyPrefix:
		return "permission"
	default:
		return "unknown"
	}
}

// runGetConfig invokes the REST API with GET action and key prefix as path.
func runGetConfig(prefix string, cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	friendlyName := getFriendlyNameFromPrefix(prefix)
	req, err := http.NewRequest("GET", kv.HTTPAddr()+prefix+"/"+args[0], nil)
	req.Header.Add("Accept", "text/yaml")
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	// TODO(spencer): need to move to SSL.
	b, err := sendAdminRequest(req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "%s config for key prefix %q:\n%s\n", friendlyName, args[0], string(b))
}

// runLsConfigs invokes the REST API with GET action and no path, which
// fetches a list of all configuration prefixes.
// The type of config that is listed is based on the passed in prefix.
// The optional regexp is applied to the complete list and matching prefixes
// displayed.
func runLsConfigs(prefix string, cmd *commander.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	friendlyName := getFriendlyNameFromPrefix(prefix)
	req, err := http.NewRequest("GET", kv.HTTPAddr()+prefix, nil)
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	b, err := sendAdminRequest(req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	var prefixes []string
	if err = json.Unmarshal(b, &prefixes); err != nil {
		log.Errorf("unable to parse admin REST response: %s", err)
		return
	}
	var re *regexp.Regexp
	if len(args) == 1 {
		if re, err = regexp.Compile(args[0]); err != nil {
			log.Warningf("invalid regular expression %q; skipping regexp match and listing all %s prefixes", args[0], friendlyName)
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

// runRmConfig invokes the REST API with DELETE action and key prefix as path.
// The type of config that is removed is based on the passed in prefix.
func runRmConfig(prefix string, cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	friendlyName := getFriendlyNameFromPrefix(prefix)
	req, err := http.NewRequest("DELETE", kv.HTTPAddr()+prefix+"/"+args[0], nil)
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	// TODO(spencer): need to move to SSL.
	_, err = sendAdminRequest(req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "removed %s config for key prefix %q\n", friendlyName, args[0])
}

// runSetConfig invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
// The type of config that is set is based on the passed in prefix.
func runSetConfig(prefix string, cmd *commander.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	friendlyName := getFriendlyNameFromPrefix(prefix)
	// Read in the config file.
	body, err := ioutil.ReadFile(args[1])
	if err != nil {
		log.Errorf("unable to read %s config file %q: %s", friendlyName, args[1], err)
		return
	}
	// Send to admin REST API.
	req, err := http.NewRequest("POST", kv.HTTPAddr()+prefix+"/"+args[0], bytes.NewReader(body))
	req.Header.Add("Content-Type", "text/yaml")
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	// TODO(spencer): need to move to SSL.
	_, err = sendAdminRequest(req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "set %s config for key prefix %q\n", friendlyName, args[0])
}
