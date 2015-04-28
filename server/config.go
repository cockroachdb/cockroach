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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// Gets a friendly name for output based on the passed in config prefix.
func getFriendlyNameFromPrefix(prefix string) string {
	switch prefix {
	case acctPathPrefix:
		return "accounting"
	case permPathPrefix:
		return "permission"
	case zonePathPrefix:
		return "zone"
	default:
		return "unknown"
	}
}

// runGetConfig invokes the REST API with GET action and key prefix as path.
func runGetConfig(ctx *Context, prefix, keyPrefix string) {
	friendlyName := getFriendlyNameFromPrefix(prefix)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s%s/%s", ctx.RequestScheme(), ctx.Addr, prefix, keyPrefix), nil)
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	req.Header.Add("Accept", "text/yaml")
	b, err := sendAdminRequest(ctx, req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "%s config for key prefix %q:\n%s\n", friendlyName, keyPrefix, string(b))
}

// RunGetAcct gets the account from the given key.
func RunGetAcct(ctx *Context, keyPrefix string) {
	runGetConfig(ctx, acctPathPrefix, keyPrefix)
}

// RunGetPerm gets the permission from the given key.
func RunGetPerm(ctx *Context, keyPrefix string) {
	runGetConfig(ctx, permPathPrefix, keyPrefix)
}

// RunGetZone gets the zone from the given key.
func RunGetZone(ctx *Context, keyPrefix string) {
	runGetConfig(ctx, zonePathPrefix, keyPrefix)
}

// runLsConfigs invokes the REST API with GET action and no path, which
// fetches a list of all configuration prefixes.
// The type of config that is listed is based on the passed in prefix.
// The optional regexp is applied to the complete list and matching prefixes
// displayed.
func runLsConfigs(ctx *Context, prefix, pattern string) {
	friendlyName := getFriendlyNameFromPrefix(prefix)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s%s", ctx.RequestScheme(), ctx.Addr, prefix), nil)
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	b, err := sendAdminRequest(ctx, req)
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
	if len(pattern) > 0 {
		if re, err = regexp.Compile(pattern); err != nil {
			log.Warningf("invalid regular expression %q; skipping regexp match and listing all %s prefixes", pattern, friendlyName)
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

// RunLsAcct lists accounts.
func RunLsAcct(ctx *Context, pattern string) {
	runLsConfigs(ctx, acctPathPrefix, pattern)
}

// RunLsPerm lists permissions.
func RunLsPerm(ctx *Context, pattern string) {
	runLsConfigs(ctx, permPathPrefix, pattern)
}

// RunLsZone lists zones.
func RunLsZone(ctx *Context, pattern string) {
	runLsConfigs(ctx, zonePathPrefix, pattern)
}

// runRmConfig invokes the REST API with DELETE action and key prefix as path.
// The type of config that is removed is based on the passed in prefix.
func runRmConfig(ctx *Context, prefix, keyPrefix string) {
	friendlyName := getFriendlyNameFromPrefix(prefix)
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s://%s%s/%s", ctx.RequestScheme(), ctx.Addr, prefix, keyPrefix),
		nil)
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	_, err = sendAdminRequest(ctx, req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "removed %s config for key prefix %q\n", friendlyName, keyPrefix)
}

// RunRmAcct removes the account with the given key.
func RunRmAcct(ctx *Context, keyPrefix string) {
	runRmConfig(ctx, acctPathPrefix, keyPrefix)
}

// RunRmPerm removes the permission with the given key.
func RunRmPerm(ctx *Context, keyPrefix string) {
	runRmConfig(ctx, permPathPrefix, keyPrefix)
}

// RunRmZone removes the zone with the given key.
func RunRmZone(ctx *Context, keyPrefix string) {
	runRmConfig(ctx, zonePathPrefix, keyPrefix)
}

// runSetConfig invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
// The type of config that is set is based on the passed in prefix.
func runSetConfig(ctx *Context, prefix, keyPrefix, configFileName string) {
	friendlyName := getFriendlyNameFromPrefix(prefix)
	// Read in the config file.
	body, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Errorf("unable to read %s config file %q: %s", friendlyName, configFileName, err)
		return
	}
	// Send to admin REST API.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s/%s", ctx.RequestScheme(), ctx.Addr, prefix, keyPrefix),
		bytes.NewReader(body))
	if err != nil {
		log.Errorf("unable to create request to admin REST endpoint: %s", err)
		return
	}
	req.Header.Add("Content-Type", "text/yaml")
	_, err = sendAdminRequest(ctx, req)
	if err != nil {
		log.Errorf("admin REST request failed: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "set %s config for key prefix %q\n", friendlyName, keyPrefix)
}

// RunSetAcct sets the account to the key given the yaml filename.
func RunSetAcct(ctx *Context, keyPrefix, configFileName string) {
	runSetConfig(ctx, acctPathPrefix, keyPrefix, configFileName)
}

// RunSetPerm sets the permission to the key given the yaml filename.
func RunSetPerm(ctx *Context, keyPrefix, configFileName string) {
	runSetConfig(ctx, permPathPrefix, keyPrefix, configFileName)
}

// RunSetZone sets the zone to the key given the yaml filename.
func RunSetZone(ctx *Context, keyPrefix, configFileName string) {
	runSetConfig(ctx, zonePathPrefix, keyPrefix, configFileName)
}

// putConfig writes a config for the specified key prefix (which is
// treated as a key). The config is parsed from the input "body". The
// config is stored proto-encoded. The specified body must validly
// parse into a config struct and must pass a given validation check (if
// validate is not nil).
func putConfig(db *client.KV, configPrefix proto.Key, config gogoproto.Message,
	path string, body []byte, r *http.Request,
	validate func(gogoproto.Message) error) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for Put")
	}
	if err := util.UnmarshalRequest(r, body, config, util.AllEncodings); err != nil {
		return util.Errorf("config has invalid format: %+v: %s", config, err)
	}
	if validate != nil {
		if err := validate(config); err != nil {
			return err
		}
	}
	key := engine.MakeKey(configPrefix, proto.Key(path[1:]))
	if err := db.Run(client.PutProto(key, config)); err != nil {
		return err
	}
	return nil
}

// getConfig retrieves the configuration for the specified key. If the
// key is empty, all configurations are returned. Otherwise, the
// leading "/" path delimiter is stripped and the configuration
// matching the remainder is retrieved. Note that this will retrieve
// the default config if "key" is equal to "/", and will list all
// configs if "key" is equal to "". The body result contains a listing
// of keys and retrieval of a config. The output format is determined
// by the request header.
func getConfig(db *client.KV, configPrefix proto.Key, config gogoproto.Message,
	path string, r *http.Request) (body []byte, contentType string, err error) {
	// Scan all configs if the key is empty.
	if len(path) == 0 {
		sr := &proto.ScanResponse{}
		if err = db.Run(client.Call{
			Args: &proto.ScanRequest{
				RequestHeader: proto.RequestHeader{
					Key:    configPrefix,
					EndKey: configPrefix.PrefixEnd(),
					User:   storage.UserRoot,
				},
				MaxResults: maxGetResults,
			},
			Reply: sr}); err != nil {
			return
		}
		if len(sr.Rows) == maxGetResults {
			log.Warningf("retrieved maximum number of results (%d); some may be missing", maxGetResults)
		}
		var prefixes []string
		for _, kv := range sr.Rows {
			trimmed := bytes.TrimPrefix(kv.Key, configPrefix)
			prefixes = append(prefixes, url.QueryEscape(string(trimmed)))
		}
		// Encode the response.
		body, contentType, err = util.MarshalResponse(r, prefixes, util.AllEncodings)
	} else {
		configkey := engine.MakeKey(configPrefix, proto.Key(path[1:]))
		call := client.Get(configkey)
		if err = db.Run(call); err != nil {
			return
		}
		reply := call.Reply.(*proto.GetResponse)
		if reply.Value == nil {
			err = util.Errorf("%s: no value present", configkey)
			return
		}
		if reply.Value.Integer != nil {
			err = util.Errorf("%s: unexpected integer value: %+v", configkey, reply.Value)
			return
		}
		if err = gogoproto.Unmarshal(reply.Value.Bytes, config); err != nil {
			return
		}
		body, contentType, err = util.MarshalResponse(r, config, util.AllEncodings)
	}

	return
}

// deleteConfig removes the config specified by key.
func deleteConfig(db *client.KV, configPrefix proto.Key, path string, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for config Delete")
	}
	if path == "/" {
		return util.Errorf("the default configuration cannot be deleted")
	}
	configKey := engine.MakeKey(configPrefix, proto.Key(path[1:]))
	return db.Run(client.Call{
		Args: &proto.DeleteRequest{
			RequestHeader: proto.RequestHeader{
				Key:  configKey,
				User: storage.UserRoot,
			},
		},
		Reply: &proto.DeleteResponse{}})
}
