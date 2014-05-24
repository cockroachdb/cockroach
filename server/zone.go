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
	"net/http"
	"net/url"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/golang/glog"
)

const (
	maxGetResults = 1 << 16 // TODO(spencer): maybe we need paged query support
)

// A zoneHandler implements the adminHandler interface
type zoneHandler struct {
	kvDB kv.DB // Key-value database client
}

// Put writes a zone config for the specified key prefix "key".  The
// zone config is parsed from the input "body". The zone config is
// stored as YAML text. The specified body must be valid utf8 and
// must validly parse into a zone config struct.
func (zh *zoneHandler) Put(path string, body []byte, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for zone Put")
	}
	configStr := string(body)
	if !utf8.ValidString(configStr) {
		return util.Errorf("config contents not valid utf8: %q", body)
	}
	_, err := storage.ParseZoneConfig(body)
	if err != nil {
		return util.Errorf("zone config has invalid format: %s: %v", configStr, err)
	}
	zoneKey := storage.MakeKey(storage.KeyConfigZonePrefix, storage.Key(path[1:]))
	pr := <-zh.kvDB.Put(&storage.PutRequest{Key: zoneKey, Value: storage.Value{Bytes: body}})
	if pr.Error != nil {
		return pr.Error
	}
	return nil
}

// Get retrieves the zone configuration for the specified key. If the
// key is empty, all zone configurations are returned. Otherwise, the
// leading "/" path delimiter is stripped and the zone configuration
// matching the remainder is retrieved. Note that this will retrieve
// the default zone config if "key" is equal to "/", and will list all
// configs if "key" is equal to "". The body result contains
// JSON-formmatted output via the GetZoneResponse struct.
func (zh *zoneHandler) Get(path string, r *http.Request) (body []byte, contentType string, err error) {
	contentType = "application/json"

	// Scan all zones if the key is empty.
	if len(path) == 0 {
		sr := <-zh.kvDB.Scan(&storage.ScanRequest{
			StartKey:   storage.KeyConfigZonePrefix,
			EndKey:     storage.PrefixEndKey(storage.KeyConfigZonePrefix),
			MaxResults: maxGetResults,
		})
		if sr.Error != nil {
			err = sr.Error
			return
		}
		if len(sr.Rows) == maxGetResults {
			glog.Warningf("retrieved maximum number of results (%d); some may be missing", maxGetResults)
		}
		var prefixes []string
		for _, kv := range sr.Rows {
			trimmed := bytes.TrimPrefix(kv.Key, storage.KeyConfigZonePrefix)
			prefixes = append(prefixes, url.QueryEscape(string(trimmed)))
		}
		// JSON-encode the prefixes array.
		if body, err = json.Marshal(prefixes); err != nil {
			err = util.Errorf("unable to format zone configurations: %v", err)
		}
	} else {
		zoneKey := storage.MakeKey(storage.KeyConfigZonePrefix, storage.Key(path[1:]))
		gr := <-zh.kvDB.Get(&storage.GetRequest{Key: zoneKey})
		if gr.Error != nil {
			return
		}
		// On get, if there's no zone config for the requested prefix,
		// return a not found error.
		if gr.Value.Bytes == nil {
			err = util.Errorf("no config found for key prefix %q", path)
			return
		}
		if !utf8.ValidString(string(gr.Value.Bytes)) {
			err = util.Errorf("config contents not valid utf8: %q", gr.Value)
			return
		}
		body = gr.Value.Bytes
	}

	return
}

// Delete removes the zone config specified by key.
func (zh *zoneHandler) Delete(path string, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for zone Delete")
	}
	if path == "/" {
		return util.Errorf("the default zone configuration cannot be deleted")
	}
	zoneKey := storage.MakeKey(storage.KeyConfigZonePrefix, storage.Key(path[1:]))
	dr := <-zh.kvDB.Delete(&storage.DeleteRequest{Key: zoneKey})
	if dr.Error != nil {
		return dr.Error
	}
	return nil
}
