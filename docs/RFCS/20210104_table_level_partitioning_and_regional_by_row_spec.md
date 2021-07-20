- Feature Name: Table-level partitioning and row-level regional table specification
- Status: accepted
- Start Date: 2021-01-04
- Authors: Andrei Matei
- RFC PR: [58440](https://github.com/cockroachdb/cockroach/pull/58440)
- Cockroach Issue: 

# Summary

This RFC proposes the expansion of some partitioning concepts and the
introduction of table-level partitioning (in addition to the existing
index-level partitioning). The RFC then proposes the details for specifying
row-level regional tables in terms of partitioned tables.

The RFC also proposes a backwards-incompatible change: reusing the existing
syntax for partitioning a table's Primary Key for table partitioning instead
(i.e. partitioning all indexes).

# Motivation

This all started when trying to specify the PK of row-level regional tables. The
fact that these tables are partitioned (in particular, their primary index is
partitioned) coupled with the fact that we want to optionally hide the partitioning
from users creates a problem for defining their PK.

From there, it gradually became clear that we'd benefit from more generally
improving the partitioning foundations on which the regional tables are built.
In particular, CRDB currently only has the concept of a partitioned index, not
that of a partitioned table. This stands in contrast to all the other databases
that support partitioning, although the passage of time made it clear that
table-level partitioning is the more useful concept. In particular, because
row-level regional tables are table-level partitioned, we take the opportunity
to introduce that concept as a stand-alone feature. We also discuss fixing a
dubious choice in our partitioning syntax - that of using syntax that appears to
be table-level for partitioning a single index (the PK).

The table-level partitioning is also proposed as the formalism on which to build
the important ["locality-optimized
search"](https://github.com/cockroachdb/cockroach/issues/55185) feature of the
query optimizer, and to expand its applicability beyond row-level regional table
to all partitioned
tables.

Generally, the goal is to get as much of the desired behavior for row level
regional tables as possible just purely based on more general SQL concepts, data
structures, and code. Indeed, it seems that we can keep query planning and all
other aspects of SQL processing agnostic of regional tables, and have only the
adding/removing of database regions understand the semantics of regional tables.

# Guide-level explanation

New and/or salient concepts:

- **Index-level partitioning columns.** This concept already exists actually (the
`IndexDescriptor` keeps track of partitioning columns), but we’re going to expand
its role for the optimizer.
  
- **Table-level partitioning columns.** These are partitioning columns that apply to
all indexes. We introduce the smallest bit of new syntax to allow these to be
specified. If a table has table-level partitioning columns, indexes cannot be
partitioned by a different set of columns (or by the same set in a different
order). As spelled out later, this has a role in refusing to create
non-partitioned indexes in some tables, and also in deciding what index gets
created automatically for unique constraints on such tables. Mixing table-level
partitioning and index partitioning generally is not allowed.

- **Index-level implicit partitioning columns.** These are partitioning columns that are part of
the (prefix of an) index only because they are partitioning columns for the
respective index, not because the user has asked for them to be there. This has
implications for how the index gets presented through introspection, what the
semantics of unique indexes are, and what happens when the index partitioning is
removed.

- **Hidden columns.** These are columns that are excluded from `*`-expansion. The
concept is orthogonal to index-level implicitness. They serve to keep the
`crdb_region` column out of star expansion by default so that multi-region
tables don't break apps doing `SELECT *`.

The general idea here is to extend the partitioning concepts such that row-level regional
tables can be built on top of them. One of the goals is to define the
primary key to not include the `crdb_region` column, but do so without
introducing the notion of a "primary index" (at least not as a concept visible
to users). This could be done by expanding the notion of “index partitioning
columns” - prefix columns for the respective index which might be there only
because the index is partitioned, not because the user asked for them
explicitly.

CRDB currently supports index-level partitioning. The proposal here is to
improve index partitioning some, and then add table-level partitioning on top of
it. This would bring our partitioning experience closer to Postgres, MySQL and
others. Currently, partitioning is done per-index because we allow different
indexes to be partitioned differently (and, in particular, we allow tables to
mix partitioned and non-partitioned indexes). This kind of flexibility is not
exposed by the regional tables.

A summary of user-level visible changes:

1. Don't require partitioning columns to be explicitly included in an index.
2. Don't require unique constraints (and, by extension, the PK) to include all
partitioning columns.
3. Introduce table-level partitioning: a set of columns that partition all of the
table's indexes.
4. Row-level regional tables are defined in terms of partitioned tables:
  * The `crdb_region` column is a table partitioning column. It is also a hidden column.
  * Their indexes don't need to be declared as including the partitioning columns.

This RFC introduces new flexibility for the row-level regional tables beyond
what was specified (or implied) by the 
[Multi-region Functional Specification](https://docs.google.com/document/d/1j4XxQtLJIYIsxmj6m3DkMlUsC1XyVHXN_d32YWSKydM/edit#heading=h.hg9l0ubuhbpl).
1. The ability to choose the name of the partitioning column (and it have it be
named `region` instead of `crdb_region` by default).
2. The ability to define the PK as either `(id)` or `(crdb_region,id)`, with
different semantics.
3. The option to make `crdb_region` a computed column.

# Reference-level explanation

## What is the primary key of a row level regional table? What even is a primary key? Let’s explore.

The spec, as it stands, specifies that the crdb_region column is part of the
table’s primary key. The spec has wording like the following:

> Row level regional tables will have a hidden regions column added to the table, which will be prepended to the primary key.

> For REGIONAL BY ROW tables, the `crdb_region` column will be a physical column
that is the prefix of the primary key.

> PRIMARY KEY (col1, col2) for this table pattern will need to be syntactic sugar for PRIMARY KEY (crdb_region, col1, col2), UNIQUE WITHOUT INDEX (col1, col2)

The spec also suggests that the `crdb_region` column is “transparent to the user”
; among other by not showing up in SHOW CREATE TABLE:

> Row level regional tables, and all their indexes (primary and secondary), are
partitioned by the region column. Each distinct region value allowable in the
region column (corresponding to the list of regions added to the database) will
have its own table partition.  These partitioning changes will be applied by the
system (either at CREATE or ALTER TABLE) and will be transparent to the user
(both at command execution, and introspection - i.e. SHOW CREATE TABLE).

These two facts seem to be at odds with each other - if a column is part of the
primary key, can it really be transparent to the user? What is SHOW CREATE TABLE
supposed to list as the primary key? Conveniently, the SHOW CREATE TABLE example
in the spec doesn’t list any PK for a table that a user created with a PK…

Besides issues of transparency, how we define the primary key has implications
on the definitions of foreign key relationships between tables, as there a
table’s primary key definition can be used implicitly. Consider:

```sql
CREATE TABLE products (
product_no INT PRIMARY KEY,
name       STRING,
price      DECIMAL
) SET LOCALITY REGIONAL BY ROW;

CREATE TABLE orders (
order_id   INT PRIMARY KEY,
product_no INT REFERENCES products,
quantity   INT
);
```

Is this supposed to work? If yes, how?

The `product_no INT REFERENCES products` clause implicitly says that
`orders.product_no` references the primary key of products. According to Postgres
semantics, this only works if the product’s PK is exactly one column of type
`int`. If the primary key is, in fact `(crdb_region, product_no)`, then the
reference is invalid.

So the questions, again, are whether this is supposed to be valid, and, if so,
through what rules/mechanisms? The spec lists as one of its principles
compatibility with ORMs’ DML, but explicitly lists “drop in compatibility with
ORM DDL queries” as a non-goal. The former mandates that the crdb_region is not
returned by `SELECT *`. The latter perhaps leaves the door open to declaring our
reference to be invalid. Were the reference to be invalid, the following
definition would work: `product_no INT REFERENCES products(product_no)`.This is
valid because, when spelling out the referenced columns, the definition is valid
if there is a unique constraint on these column and the products table would
indeed have one in the form of an implicitly-added `UNIQUE WITHOUT INDEX
(product_no) constraint`. (Note that the current proposal change the unique
constraint on `product_no` away from a `UNIQUE WITHOUT INDEX`).
Btw, the following is also valid:
```sql
alter table orders add constraint foreign key (crdb_region, product_no) references products (crdb_region, product_no)
 ```
This is different from the reference using only `(product_no)` because
`(crdb_region, product_no)` is now asking for both of `crdb_region` and `product_no`
to match between the tables. If orders is a table-level regional table, then
this is almost certainly not what you want, because you’d only be able to
reference products in one region at a time (i.e. in order’s region). However, if
`orders` is also a row-level regional table, then this may be what you want if you
want tight control over your rows’ homing.

There’s another implication of a PK, that similarly needs to be clarified. The
PK columns are used by the UPSERT statement as the ones who’s uniqueness
violation triggers the UPDATE behavior. A reminder from the docs:
```sql
UPSERT INTO t (a, b, c) VALUES (1, 2, 3);
```
is the same as:
```sql
INSERT INTO t (a, b, c)
VALUES (1, 2, 3)
ON CONFLICT (a, b)
DO UPDATE SET c = excluded.c;
```

where `(a,b)` is the PK. So, again, exactly what the PK is matters for SQL
semantics. Consider:
```sql
create table t (region int, k int primary key, x int unique);
insert into t(region, k, x) values (1,1,1);
upsert into t(region, k, x) values (2,1,1);  -- succeeds, updates the row
```
But now look what happens when region is in the PK (hint: it’s subtle):
```sql
create table t (region int, k int, primary key (region, k), x int unique);
insert into t(region, k, x) values (1,1,1);
upsert into t(region, k, x) values (2,1,1);  -- fails unique check on (x)
```
This makes sense after you think about it: in the second case, the row we’re
upserting is different (according to the PK) from the existing one, and so the
`UPDATE` path is not taken.

So, on the one hand it’d be convenient to make `crdb_region` part of the PK,
because then we get the key encoding that we want, and we can partition the PK
properly. On the other hand, putting it in the PK would presumably force us to
show the column in `SHOW CREATE TABLE` and separate the “hidden from `SHOW CREATE`”
versus “hidden from `SELECT *`” concepts, and it would also cause some references
to fail. What to do? Should we strive to keep the PK as the user
specified it, or should we expose `crdb_region` more?

There’s also a more philosophical point to discuss here: what is the meaning of
a primary key? The exact implications of declaring a primary key are not all
standardized, and depending on what your expectations are, you might favor one
option or another more.
Postgres says, quoting relevant parts:

> A primary key constraint indicates that a column, or group of columns, can be
> used as a unique identifier for rows in the table.

> This requires that the values be both unique and not null. Adding a primary key
> will automatically create a unique B-tree index on the column or group of
> columns listed in the primary key.

> There are also various ways in which the database system makes use of a
> primary key if one has been declared; for example, the primary key defines the
> default target column(s) for foreign keys referencing its table.

In addition to these, there are also some more considerations regarding the index that
accompanies a PK.

### PK clustering semantics

There’s one aspect of PKs on which databases differ: clustering semantics. In
CRDB, currently, PKs are clustered indexes, meaning that scans over contiguous
PK ranges are expected to be fast (and have access to all the columns in the
table). Other databases differ in this regard:
- CRDB - PK is (currently) clustered
- Oracle - clustering optional, but only by PK
- MySQL - PK is clustered
- Postgres - PK not clustered, separate CLUSTER command
- SQL Server - PK is clustered by default
- DB2: PK not necessarily clustered, but “the first index” of a table is
clustered? I don’t really understand it.

Now, if you like to (continue to) assume that a PK is clustered, then you
probably think that the PK should be `(crdb_region, id)` - because that’s the
combination of columns that gives you fast range scans. If we “pretend” that
product’s PK is just `(id)`, then, for example, a user might think that the
following query will be fast when, in fact, it’ll be slow:
```sql
select * from product order by id limit 1
```
Or, if you want to continue advancing a cursor for a paginated query, you’d
expect starting from an id and scanning forward in id order to be cheap.

If we defined the table’s PK as simply `(id)`, then there’s arguably not even an
index on `(id)` (in fact, the fact that Multi-region Functional Spec uses the
`UNIQUE WITHOUT INDEX` clause for `(id)` uniqueness constraint spells out that
there’s no index on it. In the context of regional by row tables, this “lack of
an index” is muddled by the fact that, when data is partitioned across regions,
users need to understand that indexes are not magic. Non-stale lookups by `(id)`
and lookups by `(region, id)` need cross-region communication in the same
circumstances (assuming a locality-optimized search is performed in the `(id)`
case).


## Detailed design
We'll get to a solid definition of row-level regional tables in three steps:
1. index partitioning improvements
2. table-level partitioning
3. build on the previous section to define row-level regional tables

### Index partitioning improvements

Consider this table definition:
```sql
CREATE TABLE t (
  region ENUM,
  id INT,
  x INT,
  PRIMARY KEY (region, id),
  INDEX(region, x) PARTITION BY LIST(region) (partition p1 values in (‘us-west’)...
) PARTITION BY LIST(region) (partition p1 values in (‘us-west’)...
```

We’d introduce three improvements here:

**One - Don’t require partitioning columns to be explicitly included in an index.**

Neither Postgres or MySQL have this restriction. In other words, today we
accept: `INDEX idx_explicit (region, x) PARTITION BY LIST(region) … `.
We don’t currently accept the following, and the proposal is to start accepting it:

   ```sql
   INDEX idx_implicit (x) PARTITION BY LIST(region) …
   ```  

Here `region` would be an implicit partitioning column for the index.  
Implicit partitioning columns would be dropped automatically when the
partitioning is removed. So,

```sql
ALTER INDEX idx_implicit PARTITION BY NONE
``` 

would result in an index on `(x)`. Not so for `idx_explicit`.

**Two - Don’t require unique constraints (and, by extension, the PK) to include all
partitioning columns.**
This builds on the previous bullet, and we’d now get ahead
of MySQL and PG (PG lists it as an implementation limitation).
So, we’d accept:

```sql
UNIQUE INDEX (x) PARTITION BY LIST(region)...
```

This would be different from:

```sql
UNIQUE INDEX (region, x) PARTITION BY LIST(region)...
```

The former means that `(x)` is unique, the latter means that the pair `(region, x)`
is unique. Enforcing the former requires a cross-partition query - it’d be cool
to highlight that in a `NOTICE`.
Note that, in the context of our table definition, adding a unique constraint
(as opposed to a unique index) would result in the creation of a unique,
non-partitioned index (as it does today). This will change in the context of
table-level partitioning; see below.  
Open question: when a partitioned unique index is defined as

```sql
UNIQUE INDEX (x) PARTITION BY LIST(region)...
```

should we allow foreign keys to reference it as `(region,id)` (besides
referencing it simply as `(id)`)?

Doing so would save a fan-out query for validating the FK, but opens the
question about what happens when the partitioning is removed from the index (and
the column removed from the index). One is allowed to declare both indexes, thus
allowing both single and double-column foreign keys, but it seems kinda
wasteful.

**Three - The primary key would similarly not need to specify the implicit partitioning columns.**

For the table above, the key can be specified as `PRIMARY KEY (id)`. The differences
in unique constraints between `(region, id)` and `(id)` apply.

We would not support placing partitioning columns in any other place than the
index's prefix, and in any other order than the partitioning order. So, the
following would not be supported:

```sql
INDEX(x, region) PARTITION BY LIST(region)
```

This is supported by Postgres but, in our case, it follows from another
departure from Postgres - the fact that we don’t allow specifying a column twice
in an index. So, if partitioning columns are explicitly added to an index, they
need to be at the front, in the partitioning order. We could lift this
restriction, but Nathan notes "I suspect that we have at least one case where we
generate an inverted index over each of an index's ordinal positions."

Note that there’s no syntax or procedure for switching an index partitioning
column from being implicit to not implicit, or the other way about - even
through the implementation of that should be trivial.  To go from

```sql
INDEX t(x) PARTITION BY LIST(REGION)...
```

to

```sql
INDEX t(region, x) PARTITION BY LIST(REGION)...
```

a new index has to be created and the original one dropped. This relies on our
existing support for having multiple indexes at once with the same columns (in
this case they’d have the same columns, but different sets of  implicit
columns).

The index partitioning columns would be recognized specifically by the optimizer;
they’d serve as the prompt for the optimizer to take zone configuration into
consideration when planning. For example, if we have an index `(region, id)`, the
fact that region is a partitioning column would eventually lead to the optimizer
planning a speculative lookup in the local region when a query by `(id)` is
performed - more specifically, it’s the fact that the column is a partitioning
column that triggers the [“locality optimized
search”](https://github.com/cockroachdb/cockroach/issues/55185), not simply the
fact that `region` is an enum. This seems to be a way to build the locality
optimized search on top of more general partitioning concepts - and so then it
could hopefully apply to all partitioned tables. This all is consistent with the
fact that partitions are the smallest level of data granularity that can have
zone configs applied to them.

### Table-level partitioning

For regional by row tables, we want to have all indexes partitioned in the same
way (existing indexes and future indexes). We also want the partitioning for
each index to happen implicitly, without the user spelling it out for each
index. We also want unique constraints to be automatically partitioned, like
discussed above. The mechanism for providing all this magic proposed here is
“table-level partitioning columns”. For the syntax, we’d take the existing
primary key partitioning syntax:

```sql
CREATE TABLE t(
region ENUM,
id INT,
-- Not including region in the PK is possible because of the implicit column rules.
PRIMARY KEY id, 
) PARTITION BY LIST(region) …
```

and we’d tweak the partitioning clause to

```sql
PARTITION *ALL* BY LIST(region) …
```

The keyword `ALL` would make the partitioning apply to all indexes (present and future).
We’ll call tables with table-level partitioning columns simply “partitioned
tables”, in contrast with other tables that may have different “partitioned
indexes”.

Beyond helping with multi-region tables, partitioned tables might independently
help once we get more strict about data sovereignity features and guarantees -
at which point data segregation has to be enforced across indexes.

A big open question here is whether it'd be a good idea to go further: break
backwards compatibility and have the partitioning clause specified at the table
level always partition the whole table, not just the PK (in other words, imply
the `ALL` specifier, and don't otherwise introduce it). We could maintain the
ability to partition only the PK by allowing the `PARTITION BY` clause on the
`PRIMARY KEY` specification. Not having done so from the beginning seems like a
mistake; it's entirely confusing that indexes are not automatically partitioned
by the PK-partitioning clause which currently appears as a table-level construct
in the syntax. We've had users trip over this repeatedly.  
A change like this would also be motivated by the fact that, once we have
locality-optimized searches, the uses of non-partitioned indexes on otherwise
partitioned tables is pretty diminished.

Any attempt to partition an index differently (or perhaps any attempt to
partition an index, period) would be rejected. Andrew Werner suggested allowing
sub-partitioning on individual indexes (on top of the table-level partitioning
columns); that can be explored in the future.

Row-level regional tables would be table-level partitioned, which is why
explicit partitioning attempts on them will fail.

Table-level partitioning also has implications for unique constraints (in
addition to unique indexes). What should `UNIQUE (x)` do in the context of a
partitioned table (note that this is the syntax for adding a unique constraint,
not a unique index)? It can’t create a non-partitioned index (which is what it
would do in a non-partitioned table). The answer is clear: it will create a
partitioned index - partitioned by the table’s partitioning columns like all the
other indexes. So, for the table above, `UNIQUE (x)` is largely equivalent to

```sql
UNIQUE INDEX (x) [PARTITIONED BY LIST(region)...]
```

That’s also how primary keys would work for partitioned tables: if the
partitioning columns are not part of the PK definition, they’ll be implicitly
included in the primary index, but without affecting the other `PRIMARY KEY`
rules: the default reference target (and the constraint for `UPSERT` conflicts).
Like discussed in the index partitioning section, it is naturally permitted to
include the partitioning columns in the PK, and to also define a unique index on
a suffix of the PK. For example, for the table above, one could declare both of
the following together:

```sql
PRIMARY KEY (REGION, ID),
UNIQUE INDEX (ID)
```

In this case, foreign keys could reference either the `t(region,id)` pair (the
default when referencing `t` because PK) or just `t(id)`. If they use the latter, a
fan-out query is necessary to enforce them.

Note that, as of recently, we also support the `UNIQUE WITHOUT INDEX (col)`
syntax for specifying unique constraints. Like the syntax says, these
constraints don’t result in an index being created. That feature is orthogonal
to what’s being discussed here - constraints backed by (perhaps partitioned)
indexes. A `UNIQUE (id)` constraint backed by a `UNIQUE INDEX (id) PARTITIONED
BY LIST(region)...` would not be presented by `SHOW CREATE TABLE` as `UNIQUE
WITHOUT INDEX`. (in fact, the table descriptor might only have info on the index
and not on a separate constraint).

### Row level regional tables

The row level regional tables would take advantage of everything discussed
above, and it seems that, when it comes to semantics, there’s nothing special
needed for “multi-region”. There will be a bit of syntactical sugar that will
commonly be used instead of the more verbose equivalents.

An example:

```sql
CREATE TABLE t (
id INT PRIMARY KEY,
x INT,
INDEX idx (x),
) SET LOCALITY REGIONAL BY ROW
```

is syntactic sugar for:

```sql
SHOW CREATE TABLE t:
> CREATE TABLE t (
    id INT NOT NULL,
    crdb_region ENUM DEFAULT gateway_region() NOT VISIBLE,
    x INT,
    CONSTRAINT "primary" PRIMARY KEY (id ASC),
    INDEX idx (x ASC),
  ) SET LOCALITY REGIONAL BY ROW AS crdb_region
```

which, in turn, underneath is the same as:

```sql
CREATE TABLE t (
  id INT NOT NULL,
  crdb_region ENUM DEFAULT gateway_region() NOT VISIBLE,
  x INT,
  -- the PK and index below now include crdb_region as
  -- implicit prefix, by virtue of the new rules for PARTITION ALL
  CONSTRAINT "primary" PRIMARY KEY (id ASC),
  INDEX idx (x ASC),
) PARTITION ALL BY LIST(crdb_region) (partition p1 values (‘region1'), ...)
```

Note that the `SET LOCALITY REGIONAL BY ROW *AS <column name>*` specifier is a minor
extension to the
[Extension syntax for controlling the partitioning concepts](https://docs.google.com/document/d/1j4XxQtLJIYIsxmj6m3DkMlUsC1XyVHXN_d32YWSKydM/edit#heading=h.ih7kylh2exhd).
That section shows how to specify the region values more generally. Here we just
use it to reference a column; the column needs to be defined with the right enum
type.

The `SHOW CREATE TABLE` output lists the `crdb_region` column, with its type and all
info necessary to use it effectively, but then doesn’t include it in the `PRIMARY
KEY` or in any index. This is courtesy of the fact that the column is made a
table partitioning column by the `SET LOCALITY REGIONAL BY ROW AS crdb_region`
clause. Note that the output of `SHOW CREATE TABLE` round-trips (whereas, for
example, a `CREATE TABLE` statement that lists the `crdb_region` column but doesn't
use the `AS crdb_region` specifier would not roundtrip (because a naked `SET
LOCALITY REGIONAL BY ROW` would complain about a `crdb_region` column already
existing).

Note the new `NOT VISIBLE`(*) keyword, meant to keep that column out of `*`
expansion. This keyword would control the `is_hidden` attribute of a column
(presented in `show columns from <table>`). Having this keyword would be
beneficial for other hidden columns (`rowid` for tables without an explicitly
defined PK, `crdb_internal_id_shard_n` for tables with hash-sharded indexes);
currently the `SHOW CREATE TABLE` for these tables leaks the existence of the
column in the `FAMILY` definition, but otherwise doesn’t present the type of the
columns.
And btw, a user would not have to define the `crdb_region` column as `NOT
VISIBLE`. In fact, if they don’t have pre-existing SELECT * queries, there’s no
reason to hide it.

(*) `HIDDEN` was considered as an alternative, but introducing new reserved
keywords that are not always preceded by pre-existing reserved keyword is
difficult for [technical reasons](https://github.com/cockroachdb/cockroach/pull/26644).

What’s attractive about this is that, from the user’s perspective, the primary
key is exactly as they’ve defined it: `(id)`. That leaves the door open for the
system to update values in the `crdb_region` column behind the user’s back
(think auto-homing); if we’d present the column as being part of a “key”, then
that door would be less open. Also, `SHOW CREATE TABLE` would present just enough
information about the `crdb_region` to let people use it, but without
over-burdening them with it by listing it over and over in every index.

As explained in the previous sections, if for table `t` the PK is defined as `(id)`,
then another table cannot have a FK to it `t` referencing `(crdb_region, id)`. In
order for that FK to be valid, either the PK needs to be defined as `PRIMARY KEY
(crdb_region, id)`, or another unique index on `(crdb_region, id)` can be defined.
This is not entirely satisfying, but also probably not very important.

#### Default expression for the `crdb_region` column

Something to note is that the user can customize the default expression for the
crdb_region to their liking. It can be quite beneficial to allow the
customization of the DEFAULT expression in order to achieve effects similar, but
not identical, to the computed crdb_region column described in [Extension syntax
for controlling the partitioning](https://docs.google.com/document/d/1j4XxQtLJIYIsxmj6m3DkMlUsC1XyVHXN_d32YWSKydM/edit#bookmark=kix.c2otft71wvgg):
- When the partitioning column is computed, the user cannot update it in order to
change the homing of a row. Similarly, auto-homing would not work. On the flip
side, the region could be uniquely computed from other columns at query time by
the optimizer, saving the need for fanout queries when the source columns are
specified during queries and during uniqueness checks associated with mutations.
  
- When the partitioning column is not computed, but has a **custom `DEFAULT`
expression** (for example, the same expression as one that could be used for the
computed column), the original home of a row would be the same as what it’d be
in the computed column case (unless the insertion query explicitly specifies the
region), but from the insertion moment on, the homing semantics are different.
The user and/or system is now free to update the column at will in order to
change the home. The optimizer cannot compute the home region based on other
columns at query time, so fanouts are necessary. Trade-offs.

Specifying a custom default expression for `crdb_region` could be useful when
importing data. For example, imagine we’re importing a dataset (a pgdump, say)
into a multi-region database. Tweaking the schema in the dump just slightly - by
specifying the `crdb_region` column with a suitable default, can be used to get
best-effort row-level homing (which could be a lot better than getting all the
rows in the database’s primary region). If, say, there’s a country column, that
can be used. Or the top-level domain from the email address could be parsed and
mapped to regions. Or perhaps there’s some ID ranges for which an affinity is
known.

### Notes

This proposal provides a lot of expressivity:
- The ability to choose the name of the partitioning column (and it have it be
named region instead of crdb_region by default).
- The ability to define FK relationships using either (id) or (crdb_region,id),
with different semantics.
- The option to make crdb_region a computed column.
- The ability to express that (id) by itself is not unique, but only the
(crdb_region, id) pair is.
  
Using the name `region` instead of `crdb_region` for the case when the
respective column is auto-generated was discussed. We decided against it on the arguments that:
- Since it was created behind the user's back, a suggestive prefix seems reasonable.
- Also since it was created behind the user's back, a easily google-able name
seems like a good idea.

It also defines the semantics of regional by row tables exclusively in terms of
partitioned tables, and it provides a coherent theory about how
locality-optimized search would be triggered (and how it applies to partitioned
tables in general).

### Partitioning column name in regional by row tables

The `SET LOCALITY REGIONAL BY ROW AS <column name>` clause allows us to use a
nicer name for the partitioning column: `region` instead of `crdb_region` (which
we’ve used so far in all multi-region docs). We've been mangling the name with
the `crdb_` prefix because of the risk of a region column already existing; we
didn’t want tables with such a column to be ineligible for MR. But now we have
another option: have `SET LOCALITY REGIONAL BY ROW` use `region` by default, and
error out if that column exists. The user would then have to use a long form `SET
LOCALITY REGIONAL BY ROW AS <column name>` clause, and chose another name.

At the moment, table-level regional tables are also specified to have a
`crdb_region` column. Wanting the default for row-level regional tables to be
consistent with those would speak against using `region` by default by `SET
LOCALITY REGIONAL BY ROW AS <column name>`. I'm personally hoping that the
table-level regional tables will not end up having this column created
automatically for them.


## Rationale and Alternatives

It's unclear whether there was ever a more straight-forward alternative for
sanely defining row-level regional tables. One could perhaps build things in a more
ad-hoc way, but at the very least you wouldn't get all the flexibility that you get
from improving the foundations.


## Unresolved questions

- Should we retro-fit the PK-partitioning syntax to mean table-level
partitioning?
- When a partitioned unique index is defined as UNIQUE INDEX (x)
  PARTITION BY LIST(region)..., should we allow foreign keys to reference it as
  (region,id) (besides referencing it simply as id)? Doing so would save a fan-out
  query for validating the FK, but opens the question about what happens when the
  partitioning is removed from the index (and the column removed from the index).
  One is allowed to declare both indexes, thus allowing both single and
  double-column foreign keys, but it seems kinda wasteful.
