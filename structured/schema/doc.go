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
Package schema provides structured data schemas for application access
to an underlying Cockroach datastore. Data schemas are declared in
YAML files with support for familiar RDBMS concepts such as tables,
columns, and indexes. Tables in Cockroach's structured data API are
semi-relational. Every row (synonymous with "tuple") in a table must
have a unique primary key.

The term "schema" used here is synonymous with "database". A cockroach
cluster may contain multiple schemas (or "databases"). Schemas are
comprised of tables. Tables are comprised of columns. Columns store
values corresponding to basic types including integer (int64), float
(float64), string (utf8), blob ([]byte). Columns also store certain
composite types including Time and LatLong (for location), NumberSet
(map[int64]struct{}), StringSet (map[string]struct{}), NumberMap
(map[string]int64) and StringMap (map[string]string).

Columns can be designated to form an index. Indexes include secondary
indexes, unique secondary indexes, location indexes, and full-text
indexes. Additional index types may be added.

An important convention is the use of both a name and a key to
describe schemas, tables and columns. The name is used for all
external access; the key is used internally for storing keys and
values efficiently. Both are specified in the schema declaration; each
(name, key) pair for a schema, table or column should be chosen to
correspond; the key should be reminiscent of the name in order to aid
when debugging. Keys should be short; you should limit them to 1-3
characters and may not contain the '/' or ':' characters. Both names
and keys must be unique according to the following rules: schemas must
be unique in a Cockroach cluster; tables must be unique in a schema;
columns must be unique in a table.

Cockroach configuration zones can be specified for the schema as a
whole and overridden for individual tables.

YAML Schema Declaration

The general structure of schema files is as follows:

  db:     <DB Name>
  db_key: <DB Key>
  tables: [<Table>, <Table>, ...]

Tables are specified as follows:

  - table:     <Table Name>
    table_key: <Table Key>
    columns: [<Column>, <Column>, ...]

Columns are specified as follows:

    - column:       <Column Name>
      column_key:   <Column Key>
      type:         <{integer,
                      float,
                      string,
                      blob,
                      time,
                      latlong,
                      numberset,
                      stringset,
                      numbermap,
                      stringmap}>
      [primary_key: true]
      [foreign_key: <Table>.<Column>]
      [index:       <{secondary,
                      uniquesecondary,
                      location,
                      fulltext}>]

YAML configuration files are specified as follows:

  db:     PhotoDB
  db_key: pdb
  tables:
  - table:     User
    table_key: us
    columns:
    - column:      ID
      column_key:  id
      type:        integer
      primary_key: true

    - column:     Name
      column_key: na
      type:       string

    - column:     Email
      column_key: em
      type:       string
      index:      uniquesecondary

  - table:     Identity
    table_key: id
    columns:
    - column:      Key
      column_key:  ke
      type:        string
      primary_key: true

    - column:      UserID
      column_key:  ui
      type:        integer
      foreign_key: User.ID

Indexes

Indexes provide efficient access to rows in the database by columnar
data other than primary keys. In the example above, we might want to
lookup a User by Email instead of ID, as in the case of login by email
address. Email is not the primary key, so without an index, there
would be no way to lookup a user without scanning the entire User
table to find the email address in question.

A sensible alternative would be to create another table, UserByEmail,
with (email, user) as a composite primary key. We could then do a
range scan on UserByEmail starting at (<email>, _) to find a matching
email and discover the associated user ID in order to then lookup the
user for login. This is exactly how indexes work, but Cockroach takes
care of all of the necessary work to create and maintain them behind
the scenes.

An index on email address for users would require a unique secondary
index. Unique because no two users should be allowed the same email
address for login. Secondary simply means that the exact value of the
email address must be supplied in order to do the lookup. Non-unique
secondary indexes are also possible. For example, If User.Name were
indexed, two or more users might have identical names. A non-unique
secondary index yields a list of users on lookup.

Other index types include "location", which allows queries by latitude
and longitude; and "fulltext", which segments string-valued columns
into "words", any subset of which may be used to query the index. If
fulltext were specified on User.Name, then user IDs could be queried
by first, middle or last name separately.
*/
package schema
