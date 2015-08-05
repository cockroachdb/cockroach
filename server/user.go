// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package server

import (
	"net/http"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/util"
)

// userHandler implements the adminHandler interface.
type userHandler struct {
	db *client.DB // Key-value database client
}

// Put writes a user config for the specified user. The body must be
// a valid UserConfig.
func (uh *userHandler) Put(path string, body []byte, r *http.Request) error {
	if len(path) == 0 || path == "/" {
		return util.Errorf("cannot write to root path")
	}
	return putConfig(uh.db, keys.ConfigUserPrefix, &config.UserConfig{},
		path, body, r, nil)
}

// Get retrieves the user config for the specified key.
// If the key is empty, return all user configurations.
func (uh *userHandler) Get(path string, r *http.Request) (body []byte, contentType string, err error) {
	return getConfig(uh.db, keys.ConfigUserPrefix, &config.UserConfig{}, path, r)
}

// Delete removes the user config specified by key.
func (uh *userHandler) Delete(path string, r *http.Request) error {
	return deleteConfig(uh.db, keys.ConfigUserPrefix, path, r)
}
