- Feature Name: index_hints
- Status: completed
- Start Date: 2014-03-31
- Authors: Radu
- RFC PR: [#5762](https://github.com/cockroachdb/cockroach/pull/5762)
- Cockroach Issue: [#5625](https://github.com/cockroachdb/cockroach/issues/5625)

# Summary

The proposal is to add syntax to allow the user to force using a specific index
for a query. This is intended as a workaround when the index selection algorithm
results in a bad choice.

# Motivation

The index selection algorithm takes into account a lot of factors but is far
from perfect. One example we saw recently was from the photos app, where a
`LIMIT` clause would have made using an index a much better choice. We have
fixed that problem since, but there are other known issues, e.g. [#5589][5589].

When we wanted a quick workaround for the photos app issue, we had to resort to
using a crafted subquery.

We want to provide an easy way to work around this kind of problem.

[5589]: https://github.com/cockroachdb/cockroach/issues/5589

# Current `@` syntax

We currently support special syntax which allows us to use an index as a
separate table, one which only has those columns that are stored in the index:

```sql
CREATE TABLE test (
    k INT PRIMARY KEY,
    u INT,
    v INT,
    w INT,
    INDEX uv (u, v)
);

INSERT INTO test VALUES (1, 10, 100, 1000), (2, 20, 200, 2000);
SELECT * FROM test@uv;
```

| u  |  v  |
|----|-----|
| 10 | 100 |
| 20 | 200 |

This feature might be of some use internally (for debugging) but it is not of
much value to a user. Notably, if we had a way to force the use of a specific
index, that syntax could be used to produce a query equivalent to the one above:
```sql
SELECT u,v FROM test USING INDEX uv
```

# Other systems

Below is a brief overview of what other DBMSs support. Since there is no common
thread in terms of syntax, we shouldn't feel compelled to be compatible with any
one of them.

### MySQL

Basic syntax:
```sql
-- Restricts index use to one of given indexes (or neither)
SELECT * FROM table1 USE INDEX (col1_index,col2_index)

-- Excludes some indexes from being used
SELECT * FROM table1 IGNORE INDEX (col3_index)

-- Forces use of one of the given indexes
SELECT * FROM table1 FORCE INDEX (col1_index)
```

More syntax and detailed information [here][1].

[1]: http://dev.mysql.com/doc/refman/5.7/en/index-hints.html

### PostgreSQL

PG does not provide support for hints. Instead they provide various knobs for
tuning the optimizer to do the right thing. Details [here][2].

[2]: http://blog.2ndquadrant.com/hinting_at_postgresql/

### Oracle

Oracle provides [various options][3] for hints, a basic example is:
```sql
SELECT /*+ INDEX(v.e2.e3 emp_job_ix) */  * FROM v
```

[3]: http://docs.oracle.com/cd/B19306_01/server.102/b14211/hintsref.htm#i26205

### MS SQL Server

SQL Server supports [many hints][4], the index hint is:

```sql
SELECT * FROM t WITH (INDEX(myindex))
```

[4]: https://msdn.microsoft.com/en-us/library/ms187373.aspx

# Alternatives

We want to address two questions:

1. Change the semantics of the existing `@` syntax?

   We can leave `@` as it is or change `@` so that using it doesn't affect the
   semantics of the query - specifically, all table columns are accessible not
   just those in the index).  Using it simply forces the use of that index
   (potentially in conjunction with the primary index, as necessary).

2. Add new syntax for index hints?

   We would add new syntax for forcing use of an index, and perhaps ignoring a
   set of indexes. The syntax we add must be part of the table clause so it will
   be usable when we have multiple tables or joins.

# Proposal

The current proposal is to do the following, in decreasing order of priority:

 1. change `@` as explained above: `table@foo` forces the use of index `foo`,
    errors out if the index does not exist.
 2. Add `table@{force_index=foo}` as an alias for `table@foo` (same behavior).
 3. Add a `no_index_join` option. When used alone (`table@{no_index_join}`), it
    directs the index selection to avoid all non-covering index. When used with
    `force_index` (`table@{force_index=index,no_index_join}`), it causes an
    error if the given index is non-covering.
 4. Add hints that tolerate missing indexes:
  * `table@{use_index=foo[,bar,...]}`: perform index selection among the
    specified indexes. Any index that doesn't exist is ignored. If none of the
    specified indexes exist, fall back to normal index selection.
  * `table@{ignore_index=foo[,bar,..]}`: do normal index selection but without
   considering the specified indexes.  Any index that doesn't exist is ignored. 
   
We will hold off on 4 until we have a stronger case for their utility.
