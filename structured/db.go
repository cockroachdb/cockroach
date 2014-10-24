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
// Author: Andrew Bonventre (andybons@gmail.com)

package structured

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// A DB interface provides methods to access a datastore
// using a structured data API.
type DB interface {
	PutSchema(*Schema) error
	DeleteSchema(*Schema) error
	GetSchema(string) (*Schema, error)
}

// A structuredDB satisfies the DB interface using the
// Cockroach kv client API.
type structuredDB struct {
	// kvDB is a client to the monolithic key-value map.
	kvDB storage.DB
}

// NewDB returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDB(kvDB storage.DB) DB {
	return &structuredDB{kvDB: kvDB}
}

// PutSchema inserts s into the kv store for subsequent
// usage by clients.
func (db *structuredDB) PutSchema(s *Schema) error {
	if err := s.Validate(); err != nil {
		return err
	}
	k := engine.MakeKey(engine.KeySchemaPrefix, proto.Key(s.Key))
	return storage.PutI(db.kvDB, k, s)
}

// DeleteSchema removes s from the kv store.
func (db *structuredDB) DeleteSchema(s *Schema) error {
	return (<-db.kvDB.Delete(&proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key: engine.MakeKey(engine.KeySchemaPrefix, proto.Key(s.Key)),
		},
	})).GoError()
}

// GetSchema returns the Schema with the given key, or nil if
// one does not exist. A nil error is returned when a schema
// with the given key cannot be found.
func (db *structuredDB) GetSchema(key string) (*Schema, error) {
	s := &Schema{}
	k := engine.MakeKey(engine.KeySchemaPrefix, proto.Key(key))
	found, _, err := storage.GetI(db.kvDB, k, s)
	if err != nil || !found {
		s = nil
	}
	return s, err
}
