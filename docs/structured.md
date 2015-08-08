##About##

This document defines the structured data layer that Cockroach exposes on top of
its distributed key:value datastore.

##Overview##

The structured data layer is an abstraction over the internal key:value store
that provides familiar data organization tools. The structured data layer
implements a tabular model of data storage which somewhat resembles a relational
database at a high level. The terms namespace, table, column, and index/key
collectively define the structured data abstraction, each roughly akin to
database, table, column, and index/key in a relational database. The structured
data layer introduces concepts of data storage that many
database users are familiar with, and greatly increases the range of use-cases
that Cockroach is able to ergonomically service.

##Goals##

Add support for the following entities: *namespaces, tables, columns, and
indexes*. These notions are chosen for their similarity to *databases, tables,
columns, and indexes* in relational databases, but they are not intended to be
implementations thereof. Each entity will support:

- Creation: of a namespace, table, column in O(1).

- Renaming: in O(1)

-  Most configuration changes in O(1): with some exceptions like creating a
   new index will take O(n) time, where n is the number of rows in the table.

- Deletion: in O(n) where n is the number of subordinate entities (tables for a
  namespace, rows for a table).

The client API will be changed to use tabular based addressing instead of
global-key addressing. Non-primary-keyed queries will be supported through the use of secondary indexes.

##Non-Goals##

For now we don't intend on developing the following:

- Add support for triggers, stored-procedures, and integrity constraints.

- Add the notion of column types beyond *a sequence of bytes*. Add support for
  unbounded sub-collections of columns.

- Add support for non-tabular storage. This might be supported in the future.

- Add support for column locality-groups to improve data layout for performance.

- Add support for interleaved tables.

##Design##

We support high level entities like Namespace, Table, Index, and Column.
{Namespace,Table}Descriptor stores the entity metadata for {Namespace,Table}.

```proto
NamespaceDescriptor {
  NamespaceID = ...;
  Permissions = ...;
  ...
}
```

```proto
TableDescriptor {
  TableID = ...;
  Columns = [{ ColumnID = ..., Name = ... }, ...];
  Indexes = [{ IndexID = ..., Name = ..., ColumnIDs = [ ... ]}, ...];
  Permissions = ...;
  NextFreeColumnId = ...;
  NextFreeIndexID = ...;
  ...
}
```

In order to support fast renaming, we indirect entity access through a
hierarchical name-to-identifier mapping. NamespaceID and TableID are
allocated from the same global ID space. ColumnID and IndexID are local
to each Table.

To simplify our implementation, our Tables will require the presence of a
primary key. Follow-up work may relax this requirement. An investigation of use
cases not requiring a primary key is required to specify this work.

Initially, all the Table metadata in a cluster will be distributed by gossip and
cached by each Cockroach node. To reduce contention on the Table metadata, a
database transaction on the data within a Table will only contain the data being
addressed, and will not include a database read of the Table metadata. Care will
be taken to not expose these implementation details to the user.

##Addressing: Anatomy of a key##

The "/" separator is used below to disambiguate the components forming
each key, and to stress an order to the way the components line up to
form the key. The actual code does not use a "/" to separate key
components, but instead uses the encodings founding in
[util/encoding](https://github.com/cockroachdb/cockroach/tree/master/util/encoding).

###Global metadata key###

The metadata addressing will use the following key:Descriptor mappings:

`\x00name-<Parent-DescriptorID><Name>` : `<DescriptorID>`

`\x00desc-<DescriptorID>` : `{Namespace,Table}Descriptor`

The root namespace has the fixed ID of `1`. The initial implementation will
support only a single level of namespaces, though the key design allows for
arbitrarily many levels. The namespace table, mapping name to descriptor ID,
allows lookup of a descriptor proto from a `<namespace>/<table>` path. The
descriptor table allows for the reverse lookup of a descriptor proto from a
descriptor ID.

As an example, lookup of the `<TableID>` for the table path `"foo/bar"`
(`<namespace>/<table>`) is performed by first looking up the `<NamespaceID>`
using the key:

  `\x00name-<1>"foo"` : `<NamespaceID>`

The resulting `<NamespaceID>` is used to lookup the `<TableID>`:

  `\x00name-<NamespaceID>"bar"` : `<TableID>`

Creating/Renaming/Deleting: Creating a new Namespace or Table involves
allocating a new ID, initializing the {Namespace,Table}Descriptor, and storing
the descriptor at its key defined above. Renaming involves deleting the old key
and creating a new one. A namespace can only be deleted if it does not contain
any children. A table is deleted by marking it deleted in its table
descriptor. This will allow folks to recover their data for a few days/weeks
before we garbage collect the table in the background.

###Data addressing###

The Index data used to find data in the database is stored in the database
itself at keys with the following prefix: `/TableID/IndexID/Key>`, where TableID
represents the Table being addressed and IndexID the index in use. The encoding
used ensures that the data and metadata are well-separated.

**Primary key addressing**

This schema is made possible by the uniqueness constraint inherent to primary
keys. The primary key prefix `/TableID/PrimaryIndexID/Key` keys into a
unique row in the database. A cell within a row under a particular column is
addressed by adding the desired ColumnID to the key prefix:
`/TableID/PrimaryIndexID/Key/ColumnID`. PrimaryIndexID is variable to allow
changing the primary key.

**Secondary key addressing**

Secondary keys are implemented as a layer of indirection to the primary key.
Non-unique secondary keys address multiple primary keys; their anatomy
is prefix-based: `/TableID/SecondaryIndexID/SecondaryKey/PrimaryKey` -> `NULL`.
Unique secondary keys typically map directly to the primary key:
`/TableID/SecondaryIndexID/SecondaryKey` -> `PrimaryKey`. An exception is made
when a secondary index contains a `NULL` constituent; SQL defines that `NULL`
does not compare equal to `NULL`, so these indexes are encoded as though they
are non-unique.

A lookup will involve looking up the secondary index, using the secondary key
to pick up all the primary keys, and further using the primary keys to get to
all the data rows.

A row insertion involves computing the primary and secondary keys for the row,
writing the data under the primary key, and writing the primary key to all the
secondary keys. A row deletion behaves analogously.

**Interleaved table layout**

We will not be implementing interleaved tables as discussed in the
[Spanner](http://static.googleusercontent.com/media/research.google.com/en/us/archive/spanner-osdi2012.pdf)
paper initially but it’s worth discussing how we might arrange their data.
Imagine you have two tables A and B with B configured to be interleaved within
A. The `/TableID-A/PrimaryIndexID/Key` prefix determines the key prefix where
the data from table B will be stored along with an entire row from table A at
primary key Key. All the rows from table B will bear the prefix
`/TableID-A/PrimaryIndexID/Key/TableID-B`, with
`/TableID-A/PrimaryIndexID/Key/TableID-B/KeyB` being a prefix for a particular
row in the table.

##Examples##

**Employee DB**

To represent an employee table for employees at microsoft, an admin might define
a namespace=“microsoft” with a table=”employees”. The employee table might have
columns (id, first-name, last-name, address, state, zip, telephone, etc), where
id is specified as the primary key. Under the covers the “microsoft” namespace
might be given a NamespaceID=101, and the employee table given a TableID=9876.
The primary index has a default IndexID=0 and there could be a secondary index
on lastname with IndexID=1. Column telephone might have a columnID=6. For an
employee with employee-id=3456, the employee’s telephone can be queried/modified
through the API using the query:

```?
  { table: “/microsoft/employees”,
    key: “3456”,
    columns : [“telephone” }
```

The query is converted internally by Cockroach into a global key: /9876/0/3456/6
(`/TableID/PrimaryIndexID/Key/ColumnID`).

Assume a secondary index is built for the last-name column. Telephone numbers of
employees with lastname=”kimball” can be queried using the query:

```?
  { table: “/microsoft/employees”,
    index: “last-name”,
    key: “kimball”,
    columns: [“telephone”] }
```

and this might produce two records for Spencer and Andy. Internally Cockroach
looks up the secondary index using key prefix:
`/TableID/SecondaryIndexID/Key`=/9876/1/kimball to get to the two employee ids for
Spencer and Andy, viz.: 1234 and 2345. The two telephone numbers are looked up
using keys: /9876/0/1234/6 and /9876/0/2345/6.

**Key:Value DB**

It is important to note that a key:value store is simply a degenerate case of
the more fully-featured namespace & table based schema defined here. A user
interested in using Cockroach as a key:value store to store all their documents
keyed by a document-id might define a table=“documents” under
namespace=”published”, with a column “document”. The user can lookup/modify the
database documents using the tuple (“/published/documents”, document-id,
“document”).

**Accounting/Permissions/Zones**

Cockroach currently allows a user to configure accounting, read/write
permissions, and zones by a key prefix. We will allow the user to configure
these permissions, etc, per namespace and per table.

**API**

The Cockroach API will be a protobuf service API. The read API will support a
Get() on a key filtered on a group of columns. It will also support a Scan() of
multiple rows in a table into a stream of ResultSets. The client API written in
a particular language can export iterators over a ResultSet. The write API will
support Put() on a group of cells in a group of rows.

