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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter.mattis@gmail.com)

/*

Package sql provides the user-facing API for access to a Cockroach datastore. As
the name suggests, the API is based around SQL, the same SQL you find in
traditional RDMBS systems like Oracle, MySQL or Postgres. The core Cockroach
system implements a distributed, transactional, monolithic sorted key-value
map. The sql package builds on top of this core system (provided by the storage
and kv packages) adding parsing, query planning and query execution as well as
defining the privilege model.

Databases and Tables

The two primary objects are databases and tables. A database is a namespace
which holds a series of tables. Conceptually, a database can be viewed as a
directory in a filesystem plus some additional metadata. A table is like a file
on steroids: it contains a structured layout of rows and columns along with
secondary indexes.

Like a directory, a database has a name and some metadata. The metadata is
defined by the DatabaseDescriptor:

  message DatabaseDescriptor {
    optional string name;
    optional uint32 id;
    optional PrivilegeDescriptor privileges;
  }

As you can see, currently the metadata just consists of privileges.

Similarly, tables have a TableDescriptor:

  message TableDescriptor {
    optional string name;
    optional uint32 id;
    repeated ColumnDescriptor columns;
    optional IndexDescriptor primary_index;
    repeated IndexDescriptor indexes;
    optional PrivilegeDescriptor privileges;
  }

Both the database ID and the table ID are allocated from the same "ID space" and
IDs are never reused.

The namespace in which databases and tables exist contains only two levels: the
root level contains databases and the database level contains tables. The
"system.namespace" and "system.descriptor" tables implement the mapping from
database/table name to ID and from ID to descriptor:

  CREATE TABLE system.namespace (
    "parentID" INT,
    "name"     CHAR,
    "id"       INT,
    PRIMARY KEY (parentID, name)
  );

  CREATE TABLE system.descriptor (
    "id"         INT PRIMARY KEY,
    "descriptor" BLOB
  );

The ID 0 is a reserved ID used for the "root" of the namespace in which the
databases reside. In order to look up the ID of a database given its name, the
system runs the following query:

  SELECT id FROM system.namespace WHERE parentID = 0 AND name = <database-name>

And given a database/table ID, the system looks up the descriptor using the
following query:

  SELECT descriptor FROM system.descriptor WHERE id = <ID>

Primary Key Addressing

All of the SQL data stored in tables is mapped down to individual keys and
values. We call the exact mapping converting any table or row to a key value
pair "key addressing". Cockroach's key addressing relies upon a primary key, and
thus all tables have a primary key, whether explicitly listed in the schema or
automatically generated. Note that the notion of a "primary key" refers to the
primary key in the SQL sense, and is unrelated to the "key" in Cockroach's
underlying key-value pairs.

Primary keys consist of one or more non-NULL columns from the table. For a given
row of the table, the columns for the primary key are encoded into a single
string (the encoding is specified in the `util` package in
`util/encoding`). These routines allow for the encoding of NULL values,
integers, floating point numbers and strings such that the lexicographic
ordering of the encoded strings corresponds to the same ordering of the
unencoded data. Using `system.namespace` as an example, the primary key columns
would be encoded as:

  /parentID/name

[Note that "/" is being used to disambiguate the components of the key. The
actual encodings do not use the "/" character].

Before being stored in the monolithic key-value space, the encoded primary key
columns are prefixed with the table ID and an ID indicating that the key
corresponds to the primary index:

  /TableID/PrimaryIndexID/parentID/name

The column data associated with a row in a table is stored within the primary
index. Additionally every column has a unique ID (local to the table). The
values for every column is stored at the key:

  /TableID/PrimaryIndexID/parentID/name/ColumnID -> Value

A column containing a NULL value is not stored in the monolithic map. In order
to detect rows which only contain NULL values in non-primary key columns, every
row has a sentinel key indicating its existence. The sentinel key is simply the
primary index key: TODO(Arjun): I do not understand this paragraph, it needs
clarification.

  /TableID/PrimaryIndexID/parentID/name -> NULL

As an optimization, columns that are part of the primary key are not stored
separately as their data can be decoded from the sentinel value.

Secondary Indexes

Despite not being a formal part of the SQL standard, secondary indexes are one
of its most powerful features. Secondary indexes are a level of indirection that
allow quick lookups of a row using something other than the primary key. As an
example, here is a secondary index on the "system.namespace" table, using only
the "name" column:

  CREATE INDEX name ON system.namespace (name);

This secondary index allows fast lookups based on just the "name". We use the
following key addressing scheme for this non-unique index:

  /TableId/SecondaryIndexID/name/parentID -> NULL

Notice that while the index is on "name", the key contains both "name" and
"parentID". This is done to ensure that each row for a table has a unique key
for the non-unique index (and therefore, a conditional put operation on the
underlying key-value store is guaranteed to fail if the row already exists). In
general, in order to guarantee that a non-unique index is unique, we encode the
index's columns followed by any primary key columns that have not already been
mentioned. Since the primary key must be uniquely define a row, this transforms
any non-unique index into a unique index.

Let's suppose that we had instead defined the index as:

  CREATE UNIQUE INDEX name ON system.namespace (name, id);

Since this index is defined on creation as a unique index, we do not need to
append the rest of the primary key columns to ensure uniqueness; instead, any
insertion of a row into the table that would result in a duplication in the
index will fail. However, we still need to be able to decode the full primary
key by reading this index, for example, in order to efficiently execute the
following query:

  SELECT parentID FROM system.namespace WHERE name = "foo";

The solution is to put any remaining primary key columns into the value. Thus,
the key addressing for this unique index looks like this:

  /TableID/SecondaryIndexID/name/ID -> /parentID

The value for a unique index is composed of any primary key columns that are not
already part of the index ("parentID" in this example). The goal of this key
addressing scheme is to ensure that the primary key is fully specified by the
key-value pair, and that the key portion is unique. However, any lookup of a
non-primary and non-index column requires two reads, first to decode the primary
key, and then to read the full row for the primary key, which contains all the
columns. For instance, suppose there was another column "bar" in this table, and
we wanted to do the following query:

  SELECT bar FROM system.namespace WHERE name = "foo";

Looking this up by the index on "name" does not give us the value of the "bar"
column. Instead, to process this query, Cockroach does two key-value reads,
which are morally equivalent to the following SQL:

  SELECT (parentID, name) FROM systemnamespace WHERE "name = "foo";

Then we use the values for the primary key that we received from the first query
to perform the lookup:

  SELECT bar FROM system.namespace WHERE parentID = "..." AND name = "...";

Query Planning and Execution

SQL queries are executed by converting every SQL query into a set of
transactional key-value operations. The Cockroach distributed transactional
key-value store provides a few operations, of which we shall discuss execution
using two important ones: conditional puts, and ordered scans.

Query planning is the system which takes a parsed SQL statement (described by an
abstract syntax tree) and creates an execution plan which is itself a tree of
operations. The execution tree consists of leaf nodes that are SCANs and PUTs,
and internal nodes that consist of operations such as join, groupby, sort, or
projection.. For the bulk of SQL statements, query planning is straightforward:
the complexity lies in SELECT.

At one end of the performance spectrum, an implementation of SELECT can be
straightforward: do a full scan of the (joined) tables in the FROM clause,
filter rows based on the WHERE clause, group the resulting rows based on the
GROUP BY clause, filter those rows using the HAVING clause, and sort the
remaining rows using the ORDER BY clause. There are a number of steps, but they
all have well defined semantics and are mostly just an exercise in software
engineering: retrieve the rows as quickly as possible and then send them through
the pipeline of filtering, grouping, filtering and sorting.

However, this naive execution plan would have poor performance if the first
scans return large amounts of data: if we are scanning orders of magnitude extra
data, only to discard the vast majority of rows as we filter out the few rows
that we need, this is needlessly inefficient. Instead, the query planner
attempts to take advantage of secondary indexes to limit the data retrieved by
the leafs. Additionally, the query planner makes joins between tables faster by
taking advantage of the different sort orders of various secondary indexes, and
avoiding re-sorting or (or taking advantage of partial sorts to limit the amount
of sorting done). As query planning is under active development, the details of
how we implement this are in flux and will continue to be in flux for the
foreseeable future. This section is intended to provide a high-level overview of
a few of the techniques involved.

For a SELECT query, after parsing it, the query planner performs semantic
analysis to statically verify if the query obeys basic type-safety checks, and
to resolve names within the query to actual objects within the system. Let's
consider a query which looks up the ID of a database named "test":

  SELECT id FROM system.namespace WHERE parentID = 0 AND name = 'test'

The query planner first needs to resolve the "system.namespace" qualified name
in the FROM clause to the appropriate TableDescriptor. It also needs to resolve
the "id", "parentID" and "name" column references to the appropriate column
descriptions with the "system.namespace" TableDescriptor. Lastly, as part of
semantic analysis, the query planner verifies that the expressions in the select
targets and the WHERE clause are valid (e.g. the WHERE clause evaluates to a
boolean).

From that starting point, the query planner then analyzes the GROUP BY and ORDER
BY clauses, adding "hidden" targets for expressions used in those clauses that
are not explicit targets of the query. Our example query does not have any GROUP
BY or ORDER BY clauses, so we move straight to the next step: index
selection. Index selection is the stage where the query planner selects the best
index to scan and selects the start and end keys that minimize the amount of
scanned data.  Depending on the complexity of the query, the query planner might
even select multiple ranges to scan from an index or multiple ranges from
different indexes.

How does the query planner decide which index to use and which range of the
index to scan? We currently use a restricted form of value propagation
(TODO(arjun): Elaborate, this is difficult to understand) in order to determine
the range of possible values for columns referenced in the WHERE clause. Using
this range information, each index is examined to determine if it is a potential
candidate and ranked according to its specificity. In addition to ranking
indexes by the column value range information, they are ranked by how well they
match the sorting required by the ORDER BY clause. Back to the example above,
the range information would determine that:

  parentID >= 0 AND parentID <= 0 AND name >= 'test' and name <= 'test

Since there is only a single index on the "system.namespace" table, it is
selected as the candidate for performing a scan. To perform this scan, we need a
start (inclusive) and end key (exclusive). The start key is computed using the
constraints on the range information above:

  /system.descriptor/primary/0/test

The end key is:

  /system.descriptor/primary/0/tesu

The "tesu" suffix is not a typo: the end key is computed as the "prefix end
key" for the key "/TableID/PrimaryIndexId/0/test". This is done by incrementing
the final byte of the key such that "t" becomes "u".

Our example scan will return two key-value pairs:

  /system.descriptor/primary/0/test    -> NULL
  /system.descriptor/primary/0/test/id -> <ID>

The value from the second key returned by the scan is the result we need to
return as the result of this SQL query.

*/
package sql
