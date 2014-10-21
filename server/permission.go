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

// A permHandler implements the adminHandler interface.
type permHandler struct {
	db storage.DB // Key-value database client
}

// Put writes a perm config for the specified key prefix (which is treated as
// a key). The perm config is parsed from the input "body". The perm config is
// stored gob-encoded. The specified body must validly parse into a
// perm config struct.
func (ph *permHandler) Put(path string, body []byte, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for permission Put")
	}
	configStr := string(body)
	var err error
	var config *proto.PermConfig
	switch GetContentType(r) {
	case "application/json", "application/x-json":
		config, err = proto.PermConfigFromJSON(body)
	case "text/yaml", "application/x-yaml":
		config, err = proto.PermConfigFromYAML(body)
	default:
		err = util.Errorf("invalid content type: %q", GetContentType(r))
	}
	if err != nil {
		return util.Errorf("permission config has invalid format: %s: %s", configStr, err)
	}
	permKey := engine.MakeKey(engine.KeyConfigPermissionPrefix, proto.Key(path[1:]))
	if err := storage.PutProto(ph.db, permKey, config); err != nil {
		return err
	}
	return nil
}

// Get retrieves the perm configuration for the specified key. If the
// key is empty, all perm configurations are returned. Otherwise, the
// leading "/" path delimiter is stripped and the perm configuration
// matching the remainder is retrieved. Note that this will retrieve
// the default perm config if "key" is equal to "/", and will list all
// configs if "key" is equal to "". The body result contains
// JSON-formatted output for a listing of keys and JSON-formatted
// output for retrieval of a perm config.
func (ph *permHandler) Get(path string, r *http.Request) (body []byte, contentType string, err error) {
	// Scan all perms if the key is empty.
	if len(path) == 0 {
		sr := <-ph.db.Scan(&proto.ScanRequest{
			RequestHeader: proto.RequestHeader{
				Key:    engine.KeyConfigPermissionPrefix,
				EndKey: engine.KeyConfigPermissionPrefix.PrefixEnd(),
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
			trimmed := bytes.TrimPrefix(kv.Key, engine.KeyConfigPermissionPrefix)
			prefixes = append(prefixes, url.QueryEscape(string(trimmed)))
		}
		// JSON-encode the prefixes array.
		contentType = "application/json"
		if body, err = json.Marshal(prefixes); err != nil {
			err = util.Errorf("unable to format permission configurations: %s", err)
		}
	} else {
		permKey := engine.MakeKey(engine.KeyConfigPermissionPrefix, proto.Key(path[1:]))
		var ok bool
		config := &proto.PermConfig{}
		if ok, _, err = storage.GetProto(ph.db, permKey, config); err != nil {
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
				err = util.Errorf("unable to marshal perm config %+v to json: %s", config, err)
				return
			}
		} else {
			// JSON-encode the config.
			contentType = "application/json"
			if body, err = config.ToJSON(); err != nil {
				err = util.Errorf("unable to marshal perm config %+v to json: %s", config, err)
				return
			}
		}
	}

	return
}

// Delete removes the perm config specified by key.
func (ph *permHandler) Delete(path string, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for permission Delete")
	}
	if path == "/" {
		return util.Errorf("the default permission configuration cannot be deleted")
	}
	permKey := engine.MakeKey(engine.KeyConfigPermissionPrefix, proto.Key(path[1:]))
	dr := <-ph.db.Delete(&proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:  permKey,
			User: storage.UserRoot,
		},
	})
	if dr.Error != nil {
		return dr.GoError()
	}
	return nil
}
