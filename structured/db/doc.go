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
Package db provides a translation between the structured data API schema
and the underlying key-value store. See
http://godoc.org/github.com/cockroachdb/cockroach/structured/schema
for details on the structured data schema.

How Data is Stored

Cockroach provides a single monolithic, sorted key-value map. The
schema package contrives to co-locate an arbitrary number of schemas
within a single Cockroach cluster without namespace collisions. An
example schema:

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
package db
