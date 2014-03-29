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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

/*
Package client provides a Go-based client for easy access to the
Cockroach structured data API. With the client package, data schemas
can be declared in a style natural to Go, but which map directly to
the canonical Cockroach YAML schema format. Methods are provided for
easily querying individual items by key, ranges of items by key if key
is sorted, or by secondary index if indexed.

Schemas may be defined simply by declaring Go structs with appropriate
field tags and then invoking NewGoSchema(...). The struct field tags
specify column properties, such as whether the column is part of the
primary key, a foreign key, etc. See below for a full list of tag
specifications.

  import "github.com/cockroach/schema"

  // User is a top-level table. User IDs are scattered, meaning a two
  // byte hash of the ID from the UserID sequence is prepended to yield
  // a randomly distributed keyspace.
  type User struct {
  	ID   int64 `roach:"pk,seq=UserID,scatter"`
  	Name string
  }

  // Identity key takes form of "email:<valid-email-address>"
  // or "phone:<valid-phone-number>". A single user may have multiple
  // identities.
  type Identity struct {
  	Key    string `roach:"pk,scatter"`
  	UserID int64  `roach:"fk=User.ID,ondelete=setnull"`
  }

  // Get the schema from the go struct declarations.
	sm := map[string]interface{}{
    'us': User{},
    'id': Identity{},
  }
  s := NewGoSchema('db', sm)

Cockroach tags for struct fields have tag name "roach" followed by the
column key and a comma-separated list of <key>[=<value>]
specifications, where value can be optional depending on the key. The
general form is as follows:

  roach:"<column-key>,[<key1[=value1]>,<key2[=value2]>...]"

The <column-key> is a 1-3 letter designation for the column (e.g. 'ui'
for 'UserId' and unique within a table) which maintains stable, human
readable keys for debugging but which takes minimal bytes for
storage. Each instance of this struct will have non-default values
encoded using the column keys. Keeping these small can make a
significant difference in storage efficiency.

Tag Specifications

  fk=<table[.column]>: (Foreign Key) specifies the field is a foreign
  key. <table.column> specifies which table and column the foreign key
  references. If a foreign key references an object with a composite
  primary key, all fields comprising the composite primary key must be
  included in the struct with "fk" tags. "column" may be omitted if
  the referenced table contains a single-valued primary key.
  "ondelete" optionally specifies behavior if referenced object is
  deleted. "interleave" optionally specifies that all of the data for
  structs of this type will be placed "next to" the referenced table
  for data locality.

  fulltextindex: the column is indexed by segmenting the text into
  words and indexing each word separately. The index supports phrase
  queries using word position data for each indexed term.

  index: a secondary index on the column value. Tuples from this table
  (or alternatively, instances of this struct) may be queried by this
  column value using the resulting index.

  interleave: specified with foreign keys to co-locate dependent data
  within a single "entity group" (to use Google's Megastore parlance).
  Interleaved data yields faster lookups when querying complete sets
  of data (e.g. a comment topic and all comments posted to it), and
  faster transactional writes in certain common cases.

  locationindex: the column is indexed by location. The details are
  implementation-dependent, but the reference impl uses S2 geometry
  patches to canvas the specified location. The column type must be
  client.Location.

  ondelete[=<behavior>]: the behavior in the event that the object
  which a foreign key column references is deleted. The two supported
  values are "cascade" and "setnull". "cascade" deletes the object
  with the foreign key reference. "setnull" is less destructive,
  merely setting the foreign key column to nil. If "interleave" was
  specified for this foreign key, then "cascade" is the mandatory
  default value; specifying setnull result in a schema validation
  error.

  pk: (Primary Key) specifies the field is the primary key or part of
  a composite primary key. The first field with pk specified will form
  the prefix of the key, each additional field with pk specified will
  be appended in order. "scatter" may be included only on the first
  field with pk specified.

  scatter: randomizes the placement of the data within the table's
  keyspace by prepending a two-byte hash of the entire primary key to
  the actual key used to store the value in cockroach. "scatter" may
  only be specified on the first field with "pk" specified.

  seq=<seq-name>: specifies that the value of this field
  auto-increments from a monotonically increasing sequence of the
  specified name.

Client may be used in a disconnected mode, which uses internal data
structures to stand in for an actual Cockroach cluster. This is useful
for unittesting data schemas and migration functions.
*/
package client
