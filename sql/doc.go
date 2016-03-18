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

Package sql provides the user-facing API for access to a Cockroach
datastore. As the name suggests, the API is based around SQL, the same SQL you
find in traditional RDMBS systems like Oracle, MySQL or Postgres. The core
Cockroach system implements a distributed, transactional, monolithic sorted
key-value map. The sql package builds on top of this core system adding
parsing, query planning and query execution as well as defining the privilege
model.

Databases and Tables

The two primary objects are databases and tables. A database is a namespace
which holds a series of tables. Conceptually, a database can be viewed like a
directory in a filesystem plus some additional metadata (privileges). A table
is like a file on steroids: containing a structured layout of rows and columns
along with secondary indexes.

Like a directory, a database has a name and metadata. The metadata is defined
by the DatabaseDescriptor:

  message DatabaseDescriptor {
    optional string name;
    optional uint32 id;
    optional PrivilegeDescriptor privileges;
  }

Similarly, tables have a TableDescriptor:

  message TableDescriptor {
    optional string name;
    optional uint32 id;
    repeated ColumnDescriptor columns;
    optional IndexDescriptor primary_index;
    repeated IndexDescriptor indexes;
    optional PrivilegeDescriptor privileges;
  }

Both the database ID and the table ID are allocate from the same "ID space" and
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

The reserved ID of 0 is used for the "root" of the namespace in which the
databases reside. In order to lookup the ID of a database given its name, the
system effectively does a query like:

  SELECT id FROM system.namespace WHERE parentID = 0 AND name = <database-name>

And given a database/table ID, the system looks up the descriptor using a query
like:

  SELECT descriptor FROM system.descriptor WHERE id = <ID>

Primary Key Addressing

All of the SQL data stored in tables is mapped down to keys and values. This
mapping is referred to as key addressing. All tables have a primary key,
whether explicitly listed in the schema or automatically generated. Note that a
primary key is unrelated to the core Cockroach key-value functionality and is
instead referring to the primary key for a SQL table.

The primary key consists of one or more non-NULL columns from the table. For a
given row of the table, the columns for the primary key are encoded into a
single string using the routines in util/encoding. These routines allow for the
encoding of NULL values, integers, floating pointer numbers and strings in such
a way that lexicographic ordering of the encoded strings corresponds to the
same ordering of the unencoded data. Using "system.namespace" as an example,
the primary key columns would be encoded as:

  /parentID/name

[Note that "/" is being used to disambiguate the components of the key. The
actual encodings do not use "/"].

Before being stored in the monolothic key-value space, the encoded primary key
columns are prefixed with the table ID and an ID indicating that the key
corresponds to the primary index:

  /TableID/PrimaryIndexID/parentID/name

The column data associated with a row in a table is stored within the primary
index which is the index associated with the primary key. Every column has a
unique ID (that is local to the table). The value for a column is stored at the
key:

  /TableID/PrimaryIndexID/parentID/name/ColumnID -> Value

A column containing a NULL value is not stored in the monolithic map. In order
to detect rows which only contain NULL values in non-primary key columns, every
row has a sentinel key indicating its existence. The sentinel key is simply the
primary index key:

  /TableID/PrimaryIndexID/parentID/name -> NULL

As an optimization, columns that are part of the primary key are not stored
separately as their data can be decoded from the sentinel value.

Secondary Indexes

Despite not being a formal part of SQL, secondary indexes are one of its most
powerful features. Secondary indexes are a level of indirection that allow
quick lookup of a row using something other than the primary key. As an
example, we can imagine creating a secondary index on the "system.namespace"
table:

  CREATE INDEX name ON system.namespace (name);

This would create a "name" index composed solely of the "name" column. The key
addressing for this non-unique index looks like:

  /TableId/SecondaryIndexID/name/parentID -> NULL

Notice that while the index is on "name", the key contains both "name" and
"parentID". This is done to ensure that each row for a table has a unique key
for the non-unique index. In general, for a non-unique index we encoded the
index's columns followed by any primary key columns that have not already been
mentioned. This effectively transforms any non-unique index into a unique
index.

Let's suppose that we had instead defined the index as:

  CREATE UNIQUE INDEX name ON system.namespace (name, id);

The key addressing for a unique index looks like:

  /TableID/SecondaryID/name/ID -> /parentID

Unique index keys are defined like this so that a conditional put operation can
fail if that key already exists for another row, thereby enforcing the
uniqueness constraint. The value for a unique index is composed of any primary
key columns that are not part of the index ("parentID" in this example).

Query Planning and Execution

Query planning is the system which takes a parsed SQL statement (described by
an abstract syntax tree) and creates an execution plan which is itself a tree
consisting of a set of scan, join, group, sort and projection operations. For
the bulk of SQL statements, query planning is straightforward: the complexity
lies in SELECT.

At one end of the performance spectrum, an implementation of SELECT can be
straightforward: do a full scan of the (joined) tables in the FROM clause,
filter rows based on the WHERE clause, group the resulting rows based on the
GROUP BY clause, filter those rows using the HAVING clause, sort using the
ORDER BY clause. There are a number of steps, but they all have well defined
semantics and are mostly just an exercise in software engineering: retrieve the
rows as quickly as possible and then send them through the pipeline of
filtering, grouping, filtering and sorting.

At the other end of the performance spectrum, query planners attempt to take
advantage of secondary indexes to limit the data retrieved, make joining of
data between two tables easier and faster and to avoid the need to sort data by
retrieving it in a sorted or partially sorted form. The details of how we
implement this are in flux and will continue to be in flux for the foreseeable
future. This section is intended to provide a high-level overview of a few of
the techniques involved.

After parsing a SELECT query, the query planner performs semantic analysis to
verify the queries correctness and to resolve names within the query to actual
objects within the system. Let's consider the query:

  SELECT id FROM system.namespace WHERE parentID = 0 AND name = 'test'

This query would look up the ID of the database named "test". The query planner
needs to resolve the "system.namespace" qualified name in the FROM clause to
the appropriate TableDescriptor. It also needs to resolve the "id", "parentID"
and "name" column references to the appropriate column descriptions with the
"system.namespace" TableDescriptor. Lastly, as part of semantic analysis, the
query planner verifies that the expressions in the select targets and the WHERE
clause are valid (e.g. the WHERE clause evaluates to a boolean).

From that starting point, the query planner then analyzes the GROUP BY and
ORDER BY clauses, adding "hidden" targets for expressions used in those clauses
that are not explicit targets of the query. In our example without a GROUP BY
or ORDER BY clause we move straight to the next step: index selection. Index
selection is the stage where the query planner selects the best index to scan
and selects the start and end keys to use for scanning the index. Depending on
the query, the query planner might even select multiple ranges to scan from an
index or multiple ranges from different indexes.

How does the query planner decide which index to use and which range of the
index to scan? We currently use a restricted form of value propagation in oder
to determine the range of possible values for columns referenced in the WHERE
clause. Using this range information, each index is examined to determine if it
is a potential candidate and ranked according to its specificity. In addition
to ranking indexes by the column value range information, they are ranked by
how well they match the sorting required by the ORDER BY clause. Back to the
example above, the range information would determine that:

  parentID >= 0 AND parentID <= 0 AND name >= 'test' and name <= 'test

Notice that each column has a start and end value associated with it. Since
there is only a single index on the "system.namespace" table, it is always
selected. The start key is computed using the range information as:

  /system.descriptor/primary/0/test

The end key is computed as:

  /system.descriptor/primary/0/tesu

The "tesu" suffix is not a typo: the end key is computed as the "prefix end
key" for the key "/TableID/PrimaryIndexId/0/test". This is done by incrementing
the final byte of the key such that "t" becomes "u".

Our example query thus only scans two key-value pairs:

  /system.descriptor/primary/0/test    -> NULL
  /system.descriptor/primary/0/test/id -> <ID>

*/
package sql
