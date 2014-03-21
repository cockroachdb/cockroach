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
// Author: Andrew Bonventre (andybons@gmail.com)

package db

import (
	"github.com/goraft/raft"
)

func init() {
	raft.RegisterCommand(&deleteCommand{})
}

type deleteCommand struct {
	Key string `json:"key"`
}

// NewDeleteCommand allocates and returns a new delete command.
func NewDeleteCommand(key string) raft.Command {
	return &deleteCommand{Key: key}
}

// CommandName returns the name of the command in the log.
func (c *deleteCommand) CommandName() string {
	return "cockroach:delete"
}

// Apply writes a value to a key.
func (c *deleteCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(DB)
	return nil, db.Delete(c.Key)
}
