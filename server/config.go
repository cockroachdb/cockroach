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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package server

import (
	"bytes"
	"net/http"
	"net/url"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// putConfig writes a config for the specified key prefix (which is
// treated as a key). The config is parsed from the input "body". The
// config is stored proto-encoded. The specified body must validly
// parse into a config struct and must pass a given validation check (if
// validate is not nil).
func putConfig(db *client.DB, configPrefix proto.Key, config gogoproto.Message,
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
	key := keys.MakeKey(configPrefix, proto.Key(path[1:]))
	return db.Put(key, config)
}

// getConfig retrieves the configuration for the specified key. If the
// key is empty, all configurations are returned. Otherwise, the
// leading "/" path delimiter is stripped and the configuration
// matching the remainder is retrieved. Note that this will retrieve
// the default config if "key" is equal to "/", and will list all
// configs if "key" is equal to "". The body result contains a listing
// of keys and retrieval of a config. The output format is determined
// by the request header.
func getConfig(db *client.DB, configPrefix proto.Key, config gogoproto.Message,
	path string, r *http.Request) (body []byte, contentType string, err error) {
	// Scan all configs if the key is empty.
	if len(path) == 0 {
		var rows []client.KeyValue
		if rows, err = db.Scan(configPrefix, configPrefix.PrefixEnd(), maxGetResults); err != nil {
			return
		}
		if len(rows) == maxGetResults {
			log.Warningf("retrieved maximum number of results (%d); some may be missing", maxGetResults)
		}
		var prefixes []string
		for _, row := range rows {
			trimmed := bytes.TrimPrefix(row.Key, configPrefix)
			prefixes = append(prefixes, url.QueryEscape(string(trimmed)))
		}
		// Encode the response.
		body, contentType, err = util.MarshalResponse(r, prefixes, util.AllEncodings)
	} else {
		configkey := keys.MakeKey(configPrefix, proto.Key(path[1:]))
		if err = db.GetProto(configkey, config); err != nil {
			return
		}
		body, contentType, err = util.MarshalResponse(r, config, util.AllEncodings)
	}

	return
}

// deleteConfig removes the config specified by key.
func deleteConfig(db *client.DB, configPrefix proto.Key, path string, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for config Delete")
	}
	if path == "/" {
		return util.Errorf("the default configuration cannot be deleted")
	}
	configKey := keys.MakeKey(configPrefix, proto.Key(path[1:]))
	return db.Del(configKey)
}
