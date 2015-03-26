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
	"net/http"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// minRangeMaxBytes is the minimum value for range max bytes.
	minRangeMaxBytes = 1 << 20
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
	if len(config.ReplicaAttrs) == 0 {
		return util.Errorf("attributes for at least one replica must be specified in zone config")
	}
	if config.RangeMaxBytes < minRangeMaxBytes {
		return util.Errorf("RangeMaxBytes %d less than minimum allowed %d", config.RangeMaxBytes, minRangeMaxBytes)
	}
	if config.RangeMinBytes >= config.RangeMaxBytes {
		return util.Errorf("RangeMinBytes %d is greater than or equal to RangeMaxBytes %d",
			config.RangeMinBytes, config.RangeMaxBytes)
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
	return getConfig(zh.db, engine.KeyConfigZonePrefix, &proto.ZoneConfig{}, path, r)
}

// Delete removes the zone config specified by key.
func (zh *zoneHandler) Delete(path string, r *http.Request) error {
	return deleteConfig(zh.db, engine.KeyConfigZonePrefix, path, r)
}
