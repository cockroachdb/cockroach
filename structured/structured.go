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
// Author: Tamir Duberstein (tamird@gmail.com)

package structured

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
)

// Structured provides Cockroach's Structured Data API.
type Structured struct {
	// db is a client to the monolithic key-value map.
	db *client.DB
}

// NewStructured returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewStructured(db *client.DB) *Structured {
	return &Structured{db: db}
}

// CreateTable creates a table from the specified schema. Table creation will
// fail if the table name is already in use.
func (s *Structured) CreateTable(args *proto.CreateTableRequest, reply *proto.CreateTableResponse) error {
	return s.db.CreateTable(args.Schema)
}
