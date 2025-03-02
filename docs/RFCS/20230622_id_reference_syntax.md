- Feature Name: ID references syntax in UDF/View queries
- Status: in-progress
- Start Date: 2023-06-22
- Authors: Chengxiong Ruan, Marcus Gartner
- RFC PR: [#105353](https://github.com/cockroachdb/cockroach/pull/105353)
- Cockroach Issue: [#83233](https://github.com/cockroachdb/cockroach/issues/83233)

## Summary

This document proposes a new SQL syntax for referencing tables, columns, indexes
and user-defined functions by their descriptor ID. This syntax is necessary in
order to allow tables, columns, indexes and UDFs that are referenced in views and
UDFs to be renamed.

## Motivation

Tables, columns, and UDFs cannot be renamed if they're referenced in a view or
UDF. We currently disallow this because the view and UDF body are stored as SQL
strings in the descriptor, so renaming a referenced object would break the
reference. In order to lift this renaming restriction, we need a syntax to
reference objects by their immutable ID. By rewriting all object references with
this new syntax in the view and UDF body, the reference can be resolved after its
object has been renamed.

We already have syntaxes for referencing sequences and user-defined types by their
IDs. This RFC proposes syntax to achieve the similar goal for table, table columns
and UDF references in UDFs and Views.

## Proposed Syntax

The main idea of proposed serialization syntaxes is pattern `{object_type:object_id}`.
`object_type` includes `TABLE`, `COLUMN`, `INDEX` and `FUNCTION`. `object_id` is
table/function descriptor id, column id or index id. Rewriting a UDF body or view
query means that all object names will be replaced by such id reference syntax, so
that objects can be renamed and UDF and view stays valid since objects will be
resolved by ID during query planning. The rewritten queries will be stored within
the descriptors and can be planned and executed without being translated back to
the original query using name references. That being said, the optimizer needs to
be taught to resolve datasource and columns by IDs.

For example:

| **Queries with name references**                 | **Rewritten queries with id references**                                                                                                   |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| SELECT a FROM t                                  | SELECT {TABLE:123}.{COLUMN:1} FROM {TABLE:123}                                                                                             |
| SELECT a FROM t@idx_a                            | SELECT {TABLE:123}.{COLUMN:1} FROM {TABLE:123}@{INDEX:123}                                                                                 |
| SELECT udf_abc(‘a’, ‘b’)                         | SELECT {FUNCTION:456}(‘a’, ‘b’)                                                                                                            |
| SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.b = t2.b | SELECT {TABLE:123}.{COLUMN:1}, {TABLE:456}.{COLUMN:1} FROM {TABLE:123} JOIN {TABLE:456} ON {TABLE:123}.{COLUMN:2} = {TABLE:456}.{COLUMN:2} |

Note that table and column aliases won’t be rewritten since they don’t matter in
the cross-reference context. Everything from system and virtual tables won’t be
rewritten.

For example:

| **Queries with name references**              | **Rewritten queries with id references**                                    |
|-----------------------------------------------|-----------------------------------------------------------------------------|
| SELECT t_alias.a FROM t t_alias               | SELECT t_alias.{COLUMN:1} FROM {TABLE:123} t_alias                          |
| SELECT a_alias FROM (SELECT a a_alias FROM t) | SELECT a_alias FROM (SELECT {TABLE:123}.{COLUMN.1} a_alias FROM {TABLE:123} |

With the id references stored in UDF and view descriptors, SHOW CREATE function
needs to be adjusted so that it can rewrite the id references of objects back to
names which is more human-readable to users.

## Risk of syntax conflict with other syntax?
`{xxx:yyy}` syntax seems not used by other statement grammars in cockroach’s
`sql.y` file. And, there isn't any use case of curly braces in the postgres' sql
parser or `pl/pgsql` parser.

## Other Problems

### Current sequence and UDT id reference

Currently, we serialize sequence names in sequence builtin functions into ID
references in a view or UDF. We also serialize type names in type casting
expressions and annotate type expressions.

Sequence serialization example:

Original query: `SELECT nextval(‘seq_name’)`

Serialized query: `SELECT nextval(104::REGCLASS)`  -- 104 is the sequence descriptor id

User defined types are serialized in a different manner, for example:

Original query: `SELECT ‘a’::some_enum`

Serialized query: `SELECT ‘a’:::@100105`  -- 100105 is the UDT OID

Similar syntax for annotate type expression:

Original query: `SELECT ‘a’:::some_enum`

Serialized query: `SELECT ‘a’::::::@100105` -- 100105 is the UDT OID

The current syntax for sequence and UDT name serialization are quite different.
We may want to have new syntaxes for them which are consistent with the proposed
syntax for tables and UDFs. But they’re only in very narrow kinds of
expressions, so can be non-goal here. The main goal of the proposed syntax is to
serialize the data source name and column names in sql queries.

Sequence rename can actually break queries

Sequence ID reference today actually does not cover the case where sequence is
used as a table. It is simply because the sequence name is not detected outside
of sequence function expressions. And we thought it’s ok to rename the sequence
since we reference it by ID.
[Here](https://github.com/cockroachdb/cockroach/blob/743486e5a13b9c9c037675462c61753a239ad872/pkg/sql/logictest/testdata/logic_test/udf#L1742)
is a logic test shows such kind of failures : This will be fixed if we reference
the table by id with the new syntax (yes a sequence is technically a table).
