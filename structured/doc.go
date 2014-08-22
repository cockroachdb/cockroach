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

/*
Package structured provides a high-level API for application access to
an underlying cockroach datastore. Cockroach itself provides a single
monolithic, sorted key-value map. Structured translates between
familiar RDBMS concepts such as tables, columns and indexes to the
key-value store.

Schemas

Schemas declare the structure of data for application access to an
underlying Cockroach datastore. Data schemas are declared in YAML
files with support for familiar RDBMS concepts such as tables,
columns, and indexes. Tables in Cockroach's structured data API are
semi-relational. Every row (synonymous with "tuple") in a table must
have a unique primary key.

The term "schema" used here is synonymous with "database". A cockroach
cluster may contain multiple schemas (or "databases"). Schemas are
comprised of tables. Tables are comprised of columns. Columns store
values corresponding to basic types including integer (int64), float
(float64), string (utf8), blob ([]byte). Columns also store certain
composite types including Time and LatLong (for location), IntegerSet
(map[int64]struct{}), StringSet (map[string]struct{}), IntegerMap
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
when debugging. Keys should be short; they are limited to 1-3
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
    columns:   [<Column>, <Column>, ...]

Columns are specified as follows:

    - column:          <Column Name>
      column_key:      <Column Key>
      type:            (integer |
                        float |
                        string |
                        blob |
                        time |
                        latlong |
                        integerset |
                        stringset |
                        integermap |
                        stringmap)>
      auto_increment:  <start-value>
      foreign_key:     <Table>.<Column>
      index:           (secondary |
                        unique |
                        location |
                        fulltext)
      interleave       true
      on_delete:       (cascade |
                        setnull)
      primary_key:     true
      scatter:         true

An example YAML schema configuration file:

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

Go-Centric Schema Declarations

Package structured also provides Go-centric access via support of
struct field tags with the designation "roach". Data schemas may be
declared in a style natural to Go, but which map directly to the
canonical Cockroach YAML schema format. Methods are provided for
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
  	ID   int64 `roach:"pk,auto,scatter"`
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

"roach" tag specifications are as follows:

  fk=<table[.column]>: (Foreign Key) specifies the field is a foreign
  key. <table.column> specifies which table and column the foreign key
  references. If a foreign key references an object with a composite
  primary key, all fields comprising the composite primary key must be
  included in the struct with "fk" tags. "column" may be omitted if
  the referenced table contains a single-valued primary key.
  "ondelete" optionally specifies behavior if referenced object is
  deleted. Foreign keys create a secondary index. It isn't necessary
  to specify secondaryindex; however, to specify that a foreign key
  denotes a one-to-one relation, specify "uniqueindex". "interleave"
  optionally specifies that all of the data for structs of this type
  will be placed "next to" the referenced table for data locality.

  fulltextindex: the column is indexed by segmenting the text into
  words and indexing each word separately. The index supports phrase
  queries using word position data for each indexed term.

  interleave: specified with foreign keys to co-locate dependent data
  within a single "entity group" (to use Google's Megastore parlance).
  Interleaved data yields faster lookups when querying complete sets
  of data (e.g. a comment topic and all comments posted to it), and
  faster transactional writes in certain common cases.

  locationindex: the column is indexed by location. The details are
  implementation-dependent, but the reference impl uses S2 geometry
  patches to canvas the specified location. The column type must be
  schema.LatLong.

  ondelete=<behavior>: the behavior in the event that the object which
  a foreign key column references is deleted. The two supported values
  are "cascade" and "setnull". "cascade" deletes the object with the
  foreign key reference. "setnull" is less destructive, merely setting
  the foreign key column to nil. If "interleave" was specified for
  this foreign key, then "cascade" is the mandatory default value;
  specifying "setnull" for an interleaved foreign key results in a
  schema validation error.

  pk: (Primary Key) specifies the field is the primary key or part of
  a composite primary key. The first field with pk specified will form
  the prefix of the key, each additional field with pk specified will
  be appended in order. "scatter" may be included only on the first
  field with pk specified.

  scatter: randomizes the placement of the data within the table's
  keyspace by prepending a two-byte hash of the entire primary key to
  the actual key used to store the value in cockroach. "scatter" may
  only be specified on the first field with "pk" specified.

  secondaryindex: a secondary index on the column value. Tuples from
  this table (or alternatively, instances of this struct) may be
  queried by this column value using the resulting index.

  auto[=<start-value>]: specifies that the value of this field
  auto-increments from a monotonically increasing sequence starting
  at the optional start value.

  uniqueindex: a secondary index where uniqueness of the column value
  is enforced.

Disconnected Mode

Client may be used in a disconnected mode, which uses internal data
structures to stand in for an actual Cockroach cluster. This is useful
for unittesting data schemas and migration functions.

How Data is Stored

The structured package contrives to co-locate an arbitrary number of
schemas within a single Cockroach cluster without namespace
collisions. An example schema:

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

All data belonging to a schema is stored with keys prefixed by
<db_key>. In the configuration example above, this would correspond to
a key prefix of "pdb". Tables are further prefixed by the table key. A
tuple from table User would have key prefix "pdb/us". The tuple's key
suffix is an encoded concatenation of column values marked as part of
the primary key. For a user with ID=531, the full key would be
"pdb/us/<OrderedEncode(531)>" (from here on out, we'll shorten
OrderedEncode to just "E"). The value stored with the key is a
gob-encoded map[string]interface{}, with each column value (excepting
primary keys) as an entry in the map, indexed by column key.

  pdb/us/<E(529)>: <data for user 529>
  pdb/us/<E(530)>: <data for user 530>
  pdb/us/<E(531)>: <data for user 531>

If a primary key column has the "scatter" option specified, the
encoded value for the key is additionally prefixed with the first two
bytes of a hash of the entire primary key value. For example, If the
hash of 531 is 1532079858. The first two bytes are 0xf2 and 0xae. The
key would be "pdb/us/\xf2\xae<E(531)>". This provides an essentially
random location for the columnar data of User 531 within the
"pdb/us/..." keyspace. Using the "scatter" option prevents hotspots in
non-uniformly distributed data. For example, primary keys generated
from a monotonically-increasing sequence or from the current
time. Keep in mind, however, that using "scatter" makes range scans
impossible.

  pdb/us/\x09\x31<E(530)>: <data for user 530>
  ...
  pdb/us/\x50\xae<E(531)>: <data for user 531>
  ...
  pdb/us/\xf2\xb9<E(529)>: <data for user 529>

If a foreign key column has the "interleave" option specified, the
data for the table is co-located with the table referenced by the
foreign key. For example, let's consider an additional table in the
schema, Address, containing addresses for Users (abbreviated for
concision).

  - table: Address
    table_key: ad
    columns:
    - column:      ID
      column_key:  id
      type:        integer
      primary_key: true

    - column:      UserID
      column_key:  ui
      type:        integer
      foreign_key: User.ID
      interleave:  true

    - column:     Street
      column_key: st
      type:       string

    - column:     City
      column_key: cy
      type:       string

Because an address has no meaning outside of a user, there is value in
co-locating keys for a user's addresses with the user data.
Transactions modifying both a user and an address (on account
creation, for example) are likely to be part of the same cockroach key
range and therefore will be committed without requiring a distributed
transaction. If "interleave" is specified, the referenced table
entity's key is used as a key prefix. This makes for longer keys, but
guarantees all data is proximately located. If User 531 has two
addresses, with ids 35 & 56 respectively, the underlying data would
look like:

  pdb/us/<E(529)>: <data for user 529>
  ...
  pdb/us/<E(531)>: <data for user 531>
  pdb/us/<E(531)>/ad/<E(35)>: <data for address 35>
  pdb/us/<E(531)>/ad/<E(56)>: <data for address 56>
  ...
  pdb/us/<E(600)>: <data for user 600>
  ...

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

How Index Data is Stored

As discussed in the thought experiment about creating the UserByEmail
table, index data is simply stored as another table. In the example of
a secondary index on User.Email, a new table with table_key 'us:em' is
created. The primary key for this table is a composite of the index
term (User.Email) and the primary key of the User table (User.ID).

  pdb/us/<E(531)>: <data for user 531, email foo@bar.com>
  pdb/us/<E(10247)>: <data for user 10247, email baz@fubar.com>
  ...
  pdb/us:em/<E(baz@fubar.com)><E(10247)>: nil
  pdb/us:em/<E(foo@bar.com)><E(531)>: nil

As can be seen above, the keyspace taken by the UserEmail index table
follows the User table. In order to lookup the user with email
"foo@bar.com", a range scan is initiated for the lower bound of
"foo@bar.com", and keys with matching prefix are decoded to yield the
list of matching user IDs.

Secondary indexes have a single term and which exactly mirrors their
value. User.Email is an example of this. For email=X, the term is X.
Secondary indexes contain only keys, no values. If the secondary index
is unique, then a range scan is done on insert to verify that no other
index term with the same prefix is already present.

Index types other than secondary, such as "location" and "fulltext",
may yield multiple index terms for a column value. Full text indexes,
for example, yield one term per segmented word. With a fulltext index
on User.Name, "Spencer Woolley Kimball" would yield three terms:
{"Spencer", "Woolley", "Kimball"}. Full text indexing also stores a
special value for each term which indicates the list of positions that
term appears in the source string. Further, index types which yield
multiple terms store the indexed terms in a special additional "term"
column (named <column_key>:t). This list of terms is used to delete
terms from the index in the event the column is deleted or updated. If
Spencer had user ID 1:

  pdb/us/<E(1)>: {em: "spencer.kimball@gmail.com,
                  na: "Spencer Woolley Kimball",
                  na:t: ["kimball", "spencer", "woolley"]}
  ...
  pdb/us:na/<E(kimball)><E(1)>: {tp: [2]}
  pdb/us:na/<E(spencer)><E(1)>: {tp: [0]}
  pdb/us:na/<E(woolley)><E(1)>: {tp: [1]}

Terms can and should efficiently combine multiple source ids into a
list instead of requiring a separate key for every instance. This is
left as future work, as it's non-trivial to do efficiently.
*/
package structured
