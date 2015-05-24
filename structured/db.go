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
	"bytes"
	"encoding/gob"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
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
	kvDB *client.KV
}

// NewDB returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDB(kvDB *client.KV) DB {
	return &structuredDB{kvDB: kvDB}
}

// PutSchema inserts s into the kv store for subsequent
// usage by clients.
func (db *structuredDB) PutSchema(s *Schema) error {
	if err := s.Validate(); err != nil {
		return err
	}
	k := keys.MakeKey(keys.KeySchemaPrefix, proto.Key(s.Key))
	// TODO(pmattis): This is an inappropriate use of gob. Replace with
	// something else.
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return err
	}
	return db.kvDB.Run(client.Put(k, buf.Bytes()))
}

// DeleteSchema removes s from the kv store.
func (db *structuredDB) DeleteSchema(s *Schema) error {
	return db.kvDB.Run(client.Call{
		Args: &proto.DeleteRequest{
			RequestHeader: proto.RequestHeader{
				Key: keys.MakeKey(keys.KeySchemaPrefix, proto.Key(s.Key)),
			},
		},
		Reply: &proto.DeleteResponse{}})
}

// GetSchema returns the Schema with the given key, or nil if
// one does not exist. A nil error is returned when a schema
// with the given key cannot be found.
func (db *structuredDB) GetSchema(key string) (*Schema, error) {
	s := &Schema{}
	k := keys.MakeKey(keys.KeySchemaPrefix, proto.Key(key))
	call := client.Get(k)
	if err := db.kvDB.Run(call); err != nil {
		return nil, err
	}
	reply := call.Reply.(*proto.GetResponse)
	if reply.Value == nil {
		// No value present.
		return nil, nil
	}
	if reply.Value.Integer != nil {
		return nil, util.Errorf("%s: unexpected integer value: %+v", k, reply.Value)
	}
	// TODO(pmattis): This is an inappropriate use of gob. Replace with
	// something else.
	if err := gob.NewDecoder(bytes.NewBuffer(reply.Value.Bytes)).Decode(s); err != nil {
		return nil, err
	}
	return s, nil
}
