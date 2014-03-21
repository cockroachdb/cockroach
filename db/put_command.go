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
	raft.RegisterCommand(&putCommand{})
}

type putCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewPutCommand allocates and returns a new put command.
func NewPutCommand(key string, value string) raft.Command {
	return &putCommand{
		Key:   key,
		Value: value,
	}
}

// CommandName returns the name of the command in the log.
func (c *putCommand) CommandName() string {
	return "cockroach:put"
}

// Apply writes a value to a key.
func (c *putCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(DB)
	return nil, db.Put(c.Key, c.Value)
}
