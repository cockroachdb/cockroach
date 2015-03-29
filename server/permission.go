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
)

// A permHandler implements the adminHandler interface.
type permHandler struct {
	db *client.KV // Key-value database client
}

// Put writes a perm config for the specified key prefix (which is treated as
// a key). The perm config is parsed from the input "body". The perm config is
// stored gob-encoded. The specified body must validly parse into a
// perm config struct.
func (ph *permHandler) Put(path string, body []byte, r *http.Request) error {
	return putConfig(ph.db, engine.KeyConfigPermissionPrefix, &proto.PermConfig{},
		path, body, r, nil)
}

// Get retrieves the perm configuration for the specified key. If the
// key is empty, all perm configurations are returned. Otherwise, the
// leading "/" path delimiter is stripped and the perm configuration
// matching the remainder is retrieved. Note that this will retrieve
// the default perm config if "key" is equal to "/", and will list all
// configs if "key" is equal to "". The body result contains
// JSON-formatted output for a listing of keys and JSON-formatted
// output for retrieval of a perm config.
func (ph *permHandler) Get(path string, r *http.Request) ([]byte, string, error) {
	return getConfig(ph.db, engine.KeyConfigPermissionPrefix, &proto.PermConfig{}, path, r)
}

// Delete removes the perm config specified by key.
func (ph *permHandler) Delete(path string, r *http.Request) error {
	return deleteConfig(ph.db, engine.KeyConfigPermissionPrefix, path, r)
}
