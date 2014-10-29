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
	"net/http"
	"net/url"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// A zoneHandler implements the adminHandler interface.
type zoneHandler struct {
	db *client.KV // Key-value database client
}

// Put writes a zone config for the specified key prefix (which is
// treated as a key). The zone config is parsed from the input
// "body". The specified body must validly parse into a zone config
// struct.
func (zh *zoneHandler) Put(path string, body []byte, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for zone Put")
	}
	config := &proto.ZoneConfig{}
	if err := util.UnmarshalRequest(r, body, config, util.AllEncodings); err != nil {
		return util.Errorf("zone config has invalid format: %q: %s", body, err)
	}
	zoneKey := engine.MakeKey(engine.KeyConfigZonePrefix, proto.Key(path[1:]))
	if err := zh.db.PutProto(zoneKey, config); err != nil {
		return err
	}
	return nil
}

// Get retrieves the zone configuration for the specified key. If the
// key is empty, all zone configurations are returned. Otherwise, the
// leading "/" path delimiter is stripped and the zone configuration
// matching the remainder is retrieved. Note that this will retrieve
// the default zone config if "key" is equal to "/", and will list all
// configs if "key" is equal to "". The body result contains
// JSON-formatted output for a listing of keys and JSON-formatted
// output for retrieval of a zone config.
func (zh *zoneHandler) Get(path string, r *http.Request) (body []byte, contentType string, err error) {
	// Scan all zones if the key is empty.
	if len(path) == 0 {
		sr := &proto.ScanResponse{}
		if err = zh.db.Call(proto.Scan, &proto.ScanRequest{
			RequestHeader: proto.RequestHeader{
				Key:    engine.KeyConfigZonePrefix,
				EndKey: engine.KeyConfigZonePrefix.PrefixEnd(),
				User:   storage.UserRoot,
			},
			MaxResults: maxGetResults,
		}, sr); err != nil {
			return
		}
		if len(sr.Rows) == maxGetResults {
			log.Warningf("retrieved maximum number of results (%d); some may be missing", maxGetResults)
		}
		var prefixes []string
		for _, kv := range sr.Rows {
			trimmed := bytes.TrimPrefix(kv.Key, engine.KeyConfigZonePrefix)
			prefixes = append(prefixes, url.QueryEscape(string(trimmed)))
		}
		// Encode the response.
		body, contentType, err = util.MarshalResponse(r, prefixes, util.AllEncodings)
	} else {
		zoneKey := engine.MakeKey(engine.KeyConfigZonePrefix, proto.Key(path[1:]))
		var ok bool
		config := &proto.ZoneConfig{}
		if ok, _, err = zh.db.GetProto(zoneKey, config); err != nil {
			return
		}
		// On get, if there's no zone config for the requested prefix,
		// return a not found error.
		if !ok {
			err = util.Errorf("no config found for key prefix %q", path)
			return
		}
		body, contentType, err = util.MarshalResponse(r, config, util.AllEncodings)
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
	zoneKey := engine.MakeKey(engine.KeyConfigZonePrefix, proto.Key(path[1:]))
	return zh.db.Call(proto.Delete, &proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:  zoneKey,
			User: storage.UserRoot,
		},
	}, &proto.DeleteResponse{})
}
