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
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// An acctHandler implements the adminHandler interface.
type acctHandler struct {
	db storage.DB // Key-value database client
}

// Put writes an accounting config for the specified key prefix (which is
// treated as a key). The accounting config is parsed from the input "body".
// The accounting config is stored gob-encoded. The specified body must must
// validly parse into an acctConfig struct.
func (ah *acctHandler) Put(path string, body []byte, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for accounting Put")
	}
	configStr := string(body)
	var err error
	var config *proto.AcctConfig
	switch GetContentType(r) {
	case "application/json", "application/x-json":
		config, err = proto.AcctConfigFromJSON(body)
	case "text/yaml", "application/x-yaml":
		config, err = proto.AcctConfigFromYAML(body)
	default:
		err = util.Errorf("invalid content type: %q", GetContentType(r))
	}
	if err != nil {
		return util.Errorf("accounting config has invalid format: %s: %s", configStr, err)
	}
	acctKey := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.Key(path[1:]))
	if err := storage.PutProto(ah.db, acctKey, config); err != nil {
		return err
	}
	return nil
}

// Get retrieves the accounting configuration for the specified key.
// If the key is empty, all accounting configurations are returned.
// Otherwise, the leading "/" path delimiter is stripped and the
// accounting configurations matching the remainder is retrieved.
// Note that this will retrieve the default accounting config if "key"
// is equal to "/", and will list all configs if "key" is equal to "".
// The body result contains JSON-formatted output for a listing of keys
// and JSON-formatted output for retrieval of an accounting config.
func (ah *acctHandler) Get(path string, r *http.Request) (body []byte, contentType string, err error) {
	// Scan all accts if the key is empty.
	if len(path) == 0 {
		sr := <-ah.db.Scan(&proto.ScanRequest{
			RequestHeader: proto.RequestHeader{
				Key:    engine.KeyConfigAccountingPrefix,
				EndKey: engine.KeyConfigAccountingPrefix.PrefixEnd(),
				User:   storage.UserRoot,
			},
			MaxResults: maxGetResults,
		})
		if sr.Error != nil {
			err = sr.GoError()
			return
		}
		if len(sr.Rows) == maxGetResults {
			log.Warningf("retrieved maximum number of results (%d); some may be missing", maxGetResults)
		}
		var prefixes []string
		for _, kv := range sr.Rows {
			trimmed := bytes.TrimPrefix(kv.Key, engine.KeyConfigAccountingPrefix)
			prefixes = append(prefixes, url.QueryEscape(string(trimmed)))
		}
		// JSON-encode the prefixes array.
		contentType = "application/json"
		if body, err = json.Marshal(prefixes); err != nil {
			err = util.Errorf("unable to format accouting configurations: %s", err)
		}
	} else {
		acctKey := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.Key(path[1:]))
		var ok bool
		config := &proto.AcctConfig{}
		if ok, _, err = storage.GetProto(ah.db, acctKey, config); err != nil {
			return
		}
		// On get, if there's no perm config for the requested prefix,
		// return a not found error.
		if !ok {
			err = util.Errorf("no config found for key prefix %q", path)
			return
		}
		// TODO(spencer): until there's a nice (free) way to parse the Accept
		//   header and properly use the request's preference for a content
		//   type, we simply find out which of "yaml" or "json" appears first
		//   in the Accept header. If neither do, we default to JSON.
		accept := r.Header.Get("Accept")
		jsonIdx := strings.Index(accept, "json")
		yamlIdx := strings.Index(accept, "yaml")
		if (jsonIdx != -1 && yamlIdx != -1 && yamlIdx < jsonIdx) || (yamlIdx != -1 && jsonIdx == -1) {
			// YAML-encode the config.
			contentType = "text/yaml"
			if body, err = config.ToYAML(); err != nil {
				err = util.Errorf("unable to marshal acct config %+v to json: %s", config, err)
				return
			}
		} else {
			// JSON-encode the config.
			contentType = "application/json"
			if body, err = config.ToJSON(); err != nil {
				err = util.Errorf("unable to marshal acct config %+v to json: %s", config, err)
				return
			}
		}
	}

	return
}

// Delete removes the accouting config specified by key.
func (ah *acctHandler) Delete(path string, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for accounting Delete")
	}
	if path == "/" {
		return util.Errorf("the default accounting configuration cannot be deleted")
	}
	acctKey := engine.MakeKey(engine.KeyConfigAccountingPrefix, engine.Key(path[1:]))
	dr := <-ah.db.Delete(&proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:  acctKey,
			User: storage.UserRoot,
		},
	})
	if dr.Error != nil {
		return dr.GoError()
	}
	return nil
}
