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
	"net/http"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// An acctHandler implements the adminHandler interface.
type acctHandler struct {
	db *client.KV // Key-value database client
}

// Put writes an accounting config for the specified key prefix (which is
// treated as a key). The accounting config is parsed from the input "body".
// The accounting config is stored gob-encoded. The specified body must must
// validly parse into an acctConfig struct.
func (ah *acctHandler) Put(path string, body []byte, r *http.Request) error {
	if len(path) == 0 {
		return util.Errorf("no path specified for accounting Put")
	}
	config := &proto.AcctConfig{}
	if err := util.UnmarshalRequest(r, body, config, util.AllEncodings); err != nil {
		return util.Errorf("accounting config has invalid format: %+v: %s", config, err)
	}
	acctKey := engine.MakeKey(engine.KeyConfigAccountingPrefix, proto.Key(path[1:]))
	if err := ah.db.PutProto(acctKey, config); err != nil {
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
	return getConfig(ah.db, engine.KeyConfigAccountingPrefix, &proto.AcctConfig{}, path, r)
}

// Delete removes the accouting config specified by key.
func (ah *acctHandler) Delete(path string, r *http.Request) error {
	return deleteConfig(ah.db, engine.KeyConfigAccountingPrefix, path, r)
}
