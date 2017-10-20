- Feature Name: SQL plan properties
- Status: draft
- Start Date: 2017-10-15
- Authors: Peter Mattis, Radu Berinde, knz
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes an *inventory of logical and physical plan properties*
that can be useful during logical planning. Reminder: the properties
are annotations next to logical and physical (sub-)plans that are
used during plan transformations, mainly for the purpose of
optimization.

As such, this RFC is complementary to:

- [RFC: SQL Logical planning](sql_query_planning.md)
- [RFC: Data structures for logical planning](data_structures_for_logical_planning.md)

and focuses on the definition of "properties", with the other RFCs
providing context on how they are to be used / integrated.

# Motivation

The situation we have to contend with is that 1) the set of properties
that are useful is (and will be) changing over time, in tandem with
the development of new optimizations 2) new understanding gained over
time will lead to tweaking ever slightly the technical or formal
definitions of properties. It is thus hard to propose a fixed plan, in
the traditional shape of an RFC, of a set of properties ahead of time
and commit to that plan ahead of time.

What will not change however, is the need to share *the same words
across the team to designate the same things over time*, whether or
not the "things" in question are currently in use, not in use any
more, or not in use yet.

The main purpose of this RFC is thus not to provide implementation
details nor an overarching integration story. Instead, **its main
purpose is to give stable names to definitions.**

A secondary purpose is to provide some context to these definitions,
so that the contributors who end up working with them **gain an
understanding of their properties more easily than by just reading the
code where these properties and their uses are being prototyped.**

A third purpose is to establish a baseline when the implementation
will need verification / review. By having the description
of the properties in prose, a reviewer can then compare the code
to the specification of the property to ascertain that the
code is indeed matching the intent.

# Guide-level explanation

## Summary

| Name                    | Type      | Category  | Origin    | Kind       | Level | Used by (example)         | Synthetic notation     |
|-------------------------|-----------|-----------|-----------|------------|-------|---------------------------|------------------------|
| injectivity             | intset    | L, sem    | derived   | scalar     | opt   | TBD                       | `x INJECTIVE{...}`     |
| monotonicity            | cf. below | L, sem    | derived   | scalar     | opt   | TBD                       | `x MONOTONIC(...)`     |
| constness               | bool      | L, sem    | derived   | scalar     | opt   | TBD                       |                        |
| known value             | int       | L, sem    | derived   | scalar     | opt   | predicate push-down       | `x = v`                |
| nullability             | intset    | L, sem    | derived   | common     | opt   | TBD                       | `x NULL` / `NOT NULL`  |
| constant columns        | intset    | L, sem    | derived   | common     | opt   | TBD                       | `x CONST`              |
| variable dependencies   | intset    | L, x-func | derived   | common     | mand. | decorrelation             |                        |
| result columns          | intset    | L, sem    | opt req.  | relational | mand. | equivalence comparisons   |                        |
| used columns            | intset    | L, x-func | derived   | relational | opt   | physical planning         |                        |
| required columns        | intset    | L, x-func | derived   | relational | opt   | join transformations      |                        |
| unique sets             | intsetset | L, sem    | derived   | relational | opt   | TBD                       | `UNIQUE {...}`         |
| key sets                | intsetset | L, sem    | derived   | relational | opt   | TBD                       | `KEY {...}`            |
| equivalency groups      | intsetset | L, sem    | derived   | relational | opt   | TBD                       | `EQ {...}`             |
| range constraints       | cf. below | L, sem    | derived   | rel(comm?) | opt   | TBD                       | `CHECK ...`            |
| req. presentation order | []int     | L, x-func | opt req.  | relational | opt   | physical planning         | `REQUIRE PRESENT(...)` |
| required ordering       | cf. below | L, sem    | opt req.  | relational | opt   | ordering transformations  | `REQUIRE ORDER BY(...)` |
| required algorithm      | enum      | L, x-func | opt req.  | relational | opt   | physical planning, search |                        |
| required scan index     | cf. below | L, x-func | opt req.  | relational | opt   | physical planning, search |                        |
| apparent presentation   | []int     | P, x-func | derived   | relational | mand. | physical planning         | `PRESENTING(...)`      |
| apparent orderings      | cf. below | P, sem    | derived   | relational | opt   | TBD                       | `ORDERED BY(...)`      |
| algorithm               | enum      | P, x-func | derived.  | relational | mand. | physical planning         |                        |
| scan index              | string    | P, x-func | derived   | relational | mand. | physical planning         |                        |
| spans for scans         | []span    | P, sem    | derived   | relational | opt   | TBD                       |                        |

Note: in the literature "functional dependencies" is the group formed
by variable dependencies, equivalency groups and range constraints.

We distinguish the *category*:

- *logical* (L) properties which define equivalency classes, and are
  shared among all m-expressions in the class.
- *physical* (P) properties which can be different per m-expression in a
  class.

We distinguish two *sub-categories*:

- *semantic* (sem) properties which describe the relationships between input
  and output values of (sub-)queries.
- *extra-functional* (x-func) properties that do not carry information about
  the input/output values.

We distinguish the *origin*:

- *optionally required* properties which can be specified in the
  query.  For example, presentation order. Optionally required
  properties are necessarily logical -- when specified, they must not
  disappear or be replaced in alternatives within an equivalency
  class. We use **verbs** in their synthetic notation, if any,
  e.g. ORDER BY / PRESENT.

- purely *derived* properties that are computed during query analysis
  and never specified explicitly, for example needed columns.  Can be
  either logical or physical.  All physical properties are necessarily
  derived (possibly from optionally required logical properties).  We
  use **adjectives** / **gerunds** in their synthetic notation, if
  any, e.g. ORDERED BY / PRESENTING.

(Mismatches between required and derived properties cause the
introduction of "enforcer nodes". See the planning RFC for more information.)

We distinguish the *level* (term to be refined!):

- *mandatory* properties must be present in the expression. A
  mandatory property that is not also specified explicitly in the
  query (if it's optionally requried) must be derived for every
  expression in the final plan.

- *optional* properties can be omitted. The absence of the property is
  not equivalent to the presence of any value for the property, however,
  i.e. an expression where the property is omitted is in a different
  class than all the expressions that have the property.

We distinguish the *kind*:

- *relational* properties that are useful for relational expressions.
- *scalar* properties that are useful for scalar expressions.

Properties can be both relational and scalar; we'll call them *common*
in contexts where a separate word is needed.

## Injectivity

- Type: intset
- Category: logical, semantic
- Origin: derived
- Kind: scalar
- Level:  optional

An injective function is a function that preserves distinctness: it
never maps distinct elements of its domain to the same element of its
codomain.

If an expression is injective, then if it is known that the operand is
UNIQUE, then the result is also UNIQUE.

For example, the function `exp(x) = e^x` is injective.

A multi-argument function can be injective on one argument or a set
thereof. For example the `tuple` function is injective on the tuple of
its arguments; the `pow` function is injective on its first argument
and second argument separately if the other argument is constant.

Example synthetic notation:

```
-- query:
> select exp(k) as a, log(k) as b, pow(k, 3) as c, pow(3, k) as d, (v, k) as e FROM kv

-- properties for the result:
PROPERTIES(
  a INJECTIVE{k}
  b INJECTIVE{k}
  c INJECTIVE{k},
  d INJECTIVE{k},
  e INJECTIVE{k, v}
)
```

## Monotonicity

- Type: ordering tuple
- Category: logical, semantic
- Origin: derived
- Kind: scalar
- Level:  optional

This property indicates, for a scalar operator, how any known ordering
of the operands can yield knowledge about the ordering of the result.

We distinguish further:

- strict vs non-strict monotonicity.

  If `f` is strict-monotonic, then if `x` is sorted and unique, then
  `f(x)` is sorted and unique. For example, `f(x) = x * 2` is
  strict-monotonic.

  If `f` is nonstrict-monotonic, then if `x` is sorted and unique,
  then `f(x)` is sorted but not necessarily unique. (i.e. doesn't
  propagate unicity) For example, the `floor` function is non-strict
  monotonic.

  Note that it is not sufficient to know that a function is injective
  to determine whether it is strictly or non-strictly monotonic.
  This is because two different SQL values can be equivalent for the
  purpose of sorting (e.g. with collated strings).

- positive vs negative monotonic function of the operand(s): if the
  function is positive monotonic and the operand sorts ascending, the
  result sorts ascending. With a negative monotonic, the operand sorts
  descending.

  For example, `f(x) = x * 2` is positive monotonic, `f(x) = 1 - x` is
  negative monotonic.

If the operator has multiple arguments, it is still possible for it to
be monotonic in various ways, including but not limited to:

- the N-ary `tuple` operator is positive strict-monotonic over its arguments.
- COALESCE/IFNULL is positive strict-monotonic over its arguments.
- the `CASE` operator is positive nonstrict-monotonic over all its THEN+ELSE expressions
  if all of them are sorted in the same order.
- `rtrim` is positive nonstrict-monotonic over its first argument if
  the second argument is constant.

Example synthetic notation:

```
-- query:
> select exp(k) as a, log(k) as b, pow(k, 0.5) as c, pow(0.5, k) as d, (v, k) as e FROM kv

-- properties for the result:
PROPERTIES(
  a MONOTONIC(k ASC)
  b MONOTONIC(k DESC)
  c MONOTONIC(k ASC),
  d MONOTONIC(k DESC),
  e MONOTONIC(v ASC, k ASC)
)
```

## Constness for scalars

- Type: boolean
- Category: logical, semantic
- Origin: derived
- Kind: scalar
- Level:  optional

This properly indicates that the expression produces the same value in
all its evaluations.

We don't really need to compute this property; it is equivalent to
having an empty *variable dependency set* (see below).

(No synthetic notation)

## Known value

- Type: datum
- Category: logical, semantic
- Origin: derived
- Kind: scalar
- Level:  optional

This property indicates the actual value of a constant
expression, if such a value is known.

Example synthetic notation:

```
> select 3 + txn_timestamp() as x

PROPERTIES(
  x = '2017-10-19 20:12:31...'
)
```

## Nullability

- Type: intset
- Category: logical, semantic
- Origin: derived
- Kind: scalar
- Level:  optional

This property indicates which result(s) can contain NULL values.

For scalar expressions the int set is either empty or contains the
expression's index.

For relational expressions, the intset contains the set of columns
which can contain NULL values.


Example synthetic notation:

```
-- query:
> select k + 2 as a, v - 2 as b, ifnull(v, 42) as c from kv

PROPERTIES(
  a NOT NULL, -- assuming k is not nullable
  b NULL,     -- assuming v is nullable
  c NOT NULL
)
```

## Constant columns

- Type: intset
- Category: logical, semantic
- Origin: derived
- Kind: common
- Level:  optional

This property indicates the set of columns for which every value is
the same in the result relation.

(Intuitively, the const set for a relational expression is the set of
columns that have the CONST scalar property. It works both ways: if
the constness is known for the relational expression, it can be
derived for the member scalar expressions.)

Example synthetic notation:

```
-- query:
> select k, v, 'foo' as c from kv where v = 42

PROPERTIES(
  v CONST,
  c CONST
)
```

## Result columns

- Type: intset
- Category: logical, semantic
- Origin: derivable
- Kind: relational
- Level:  mandatory

This property indicates which expressions compose each row in the result relation.

This does not define a presentation order, see the separate
presentation order properties for that purpose.

(No synthetic notation)

Example with memo:

```sql
> select v, k from (select k, v from kv where k > 3) where v < 4

                    Result columns
0: (var k)          n/a
1: (var v)          n/a
2: (scan kv [0 1])  {0,1}
3: (literal "3")    n/a
4: (> 0 3)          n/a
5: (filter 2 4)     {0,1}
6: (literal "4")    n/a
7: (< 1 7)          n/a
8: (filter 5 7)     {0,1}
root: 8
```

## Variable dependencies

- Type: intset
- Category: logical, semantic
- Origin: derived
- Kind: common
- Level:  mandatory

This property indicates which other expressions are necessary to
compute the result.
This is needed to represent (de)correlation.

For example in:

```sql
> select * from kv where exists (select * from ab where ab.a = kv.v)
```

- the where clause `ab.a = kv.v` has both `kv.v` and `ab.a` in its variable dependencies
- the entire sub-query `select * from ab...` has `kv.v` in its variable dependencies

The implementation should strive to restrict the property to those
expressions that are also sufficient to compute the result, otherwise
simplifications may be impaired.

In general, at every select clause, the set of variable dependencies
is the union of the sets of variable dependencies of all scalar
subexpressions at that level, minus (set difference) the set of
variables provided by its FROM clause and its projection list.

(No synthetic notation)

Example with memo:

```
> select * from kv where exists (select * from ab where ab.a = kv.v)

   M-expression        Results columns Variable dependencies
0: (var k)             n/a             {0}
1: (var v)             n/a             {1}
2: (scan kv [0 1])     {0,1}           {}
3: (var a)             n/a             {3}
4: (var b)             n/a             {4}
5: (scan ab [3 4])     {3,4}           {}
6: (= 3 1)             n/a             {3,1}
7: (filter 5 6)        {3,4}           {1}  = deps(@6) - results(@5)
8: (exists 7)          n/a             {1}
9: (filter 2 8)        {0,1}           {}
root: 9
```

## Used columns

- Type: intset
- Category: logical, extra-functional
- Origin: derived
- Kind: relational
- Level:  optional

This property indicates, for a relational expression X, which of the result
columns of its data source(s) is used by an immediate scalar sub-expression of X.

This can be computed as the intersection of the union of the result columns of its source(s),
with the union of the variable dependencies of its immediate scalar sub-expressions.

No synthetic notation.

Example with memo:

```
> select a+k, v from ab, kv

   M-expression        Results columns Variable dependencies Used columns
0: (var k)             n/a             {0}                   n/a
1: (var v)             n/a             {1}                   n/a
2: (scan kv [0 1])     {0,1}           {}                    {}
3: (var a)             n/a             {3}                   n/a
4: (var b)             n/a             {4}                   n/a
5: (scan ab [3 4])     {3,4}           {}                    {}
6: (cross [2 5])       {0,1,3,4}       {}                    {}
7: (+ 3 0)             n/a             {0,3}                 n/a
6: (project 6 [7 1])   {7,1}           {0,1,3}               {0,3} = inter({0,1,3,4}, {0,3})
root: 6
```

## Required columns

- Type: intset
- Category: logical, extra-functional
- Origin: derived
- Kind: relational
- Level:  optional

This property indicates, for a relational expression X, which of its result
columns is used by an expression that depends on X.

This property is *defined* inductively as follows:

```
required(node) = union(used(node), intersect(required(parent), results(node))
```

Example with memo:

```
> select a+k, v from ab, kv

   M-expression        Results   Vardeps  Used     Required columns
0: (var k)             n/a       {0}      n/a      n.a
1: (var v)             n/a       {1}      n/a      n/a
2: (scan kv [0 1])     {0,1}     {}       {}       {0}
3: (var a)             n/a       {3}      n/a      n.a
4: (var b)             n/a       {4}      n/a      n/a
5: (scan ab [3 4])     {3,4}     {}       {}       {3}
6: (cross [2 5])       {0,1,3,4} {}       {}       {0,3}
7: (+ 3 0)             n/a       {0,3}    n/a      n/a
6: (project 6 [7 1])   {7,1}     {0,1,3}  {0,3}    {0,3}
root: 6
```

## Unique sets

- Type: set of intsets
- Category: logical, semantic
- Origin: derived
- Kind: relational
- Level:  optional

This property indicates which groups of columns form a unique
index, that is, no two rows are equal when projected on that group.
The rows may contain NULL values though.

A side property of the unique sets is that two unique sets necessarily
differ in at least one column. If that wasn't the case, one would
be a subset of the other, and the unicity of the smaller set would
imply unicity of the larger set.

This arises when reading from a UNIQUE index, or using DISTINCT.

Example with synthetic syntax:

```
> select * from (select distinct v from kv) with ordinality

PROPERTIES(
   UNIQUE {v},
   UNIQUE {ordinality}
)
```

## Key sets

- Type: set of intsets
- Category: logical, semantic
- Origin: derived
- Kind: relational
- Level:  optional

This property indicates which groups of columns form a key, that is,
no two rows are equal when projected on that group and there
are no NULL values.

It occurs e.g. when reading from a primary index.


This property can be derived from the unique sets and the non-nullable
column set: as soon as *any* column in a unique set is non-nullable,
the entire unique set is also a key.

Conversely, if a column group is a key, it is also unique.


Like for the unique sets, a side property of the key sets is that two
key groups necessarily differ in at least one column. If that wasn't
the case, one would be a subset of the other, and the unicity of the
smaller set would imply unicity of the larger set.


```
> select * from (select distinct k, v from kv) with ordinality

PROPERTIES(
   KEY {k,v},
   KEY {ordinality}
)
```

## (Column) equivalency groups

- Type: set of intsets
- Category: logical, semantic
- Origin: derived
- Kind: relational

This property indicates which groups of columns are equivalent on every
row according to the SQL equality operator `=`.

This information information can come from filter predicates, join
conditions and foreign key information in schemas.

Note that in general, equality doesn't necessarily mean the values are
identical/interchangeable (e.g. collated strings).

Also, if any two columns land in an equivalency group, then one can
derive that they are not nullable. This is because NULL is not equal
to any value, not even itself.

XXX: should we extend the definition to *require* the columns to be
known non-nullable upfront?

Example synthetic notation:

```sql
> select * from kv, ab where k = a and v = b

PROPERTIES(
   EQ {k,a}
   EQ {v,b}
)
```

## Range constraints

- Type: TBD (?)
- Category: logical, semantic
- Origin: derived
- Kind: relational (maybe common?)

This property indicates scalar range properties verified collectively
by multiple expressions/variables.

The precise definition TBD; this is the property from which we
wish to extract span information in scans, and which
we will likely use to determine the cardinality of results from
table statistics.

Immediately useful to encode the following:

- boundaries: `select * from kv where k > 10`
- tuple boundaries: `select * from kv where (k, v) > (10, 123)
- alternatives: `select * from kv where k in (10, 20, 30)
- conjunctions: `select * from kv,ab where k > 10 and a < 30`

Maybe useful to encode the following:

- predicates over separate columns: `select * from kv,ab where k < a`
- predicates over sub-expressions: `select * from kv,ab where k < 1-a`
- predicates on non-normalizable sub-expressions: `select * from kv,ab where ceil(k) = floor(a)`

Proposed synthetic notation:

```sql
> select * from kv where k > 10 and v < 30

PROPERTIES(
  CHECK k > 10
  CHECK v < 30
)
```

## Required presentation order

- Type: int list
- Category: logical, extra functional
- Origin: required
- Kind: relational
- Level:  optional

This property indicates in which order the result columns must be presented
when producing results.

This is usually only present on the top-level expression of a query,
since only the results flowing to the clients are usually labeled.
However there are exeptions:

- the data source to INSERT must be present in the order required by INSERT
- subqueries in scalar contexts that compare to a tuple (e.g. `(1,2,3)
  = (select a,b,c from abc)`) must present their results in the order
  specified, to align with the requirements of the surrounding scalar
  expressions.

See also: apparent presentation order.

Synthetic notation:

```sql
> select k, v, k from kv

PROPERTIES(
  REQUIRE PRESENT(k, v, k)
)
```

## Required ordering

- Type: ordering info
- Category: logical, semantic
- Origin: required
- Kind: relational
- Level:  optional

This property indicates in which order the result rows must be sorted.
This is computed from the top-level ORDER BY clauses in the query text.

The property is structured as an array of (column index, direction).

Note: not all ORDER BY clauses yield required orderings. For example
the SQL standard says that in `select * from (select * from kv order
by v)`, there is no required ordering because there is no ORDER BY
clause on the outer query.

The locations where ORDER BY matter are:

- at the toplevel of the query;
- at the toplevel of a sub-query in scalar context where order matters (including `tuple = subquery`, `array(subquery)` but e.g. not operand of EXISTS / NOT EXISTS)
- at the toplevel of the data source for DELETE, INSERT, UPDATE, UPSERT if LIMIT is also specified;
- at the toplevel of a sub-query in relational context where order matters (including WITH ORDINALITY)

Synthetic notation:

```
> select * from kv order by v desc, k asc

PROPERTIES(
  REQUIRE ORDER BY (v DESC, k ASC)
)
```

Note that in contrast with apparent orderings (see below), there can
be only one ordering info array in this property.

## Required algorithm

- Type: enum (dependent on relational operator)
- Category: logical, extra-functional
- Origin: required
- Kind: relational
- Level:  optional

This property indicates which algorithm must be used for a given
relational operator. This is computed from query annotations in the
query text (we usually call them "hints" but this is a misnomer -- in
practice we've always meant them as mandatory things).

See also: actual algorithm, below.

No synthetic notation.

Example with memo:

```
> select * from ab cross join @{hash} kv

                                     Logical property
0: (var a)
1: (var b)
2: (scan ab [0 1])
3: (var k)
4: (var v)
5: (scan kv [3 4])
6: (cross|enforced {2 5} [0 1 3 4])  @{req-alg: 'hash'}
root: 6

> select * from kv order @{iterative} by v

                            Logical property
0: (var k)
1: (var v)
2: (scan kv [0 1])
3: (sort|enforced 2)        @{req-alg: 'iterative'}
root: 3
```

(The notation `|enforced` in the operator indicates that the
m-expression cannot be eliminated during simplifications / search.)

## Required scan index

- Type: string
- Category: logical, semantic
- Origin: required
- Kind: relational
- Level:  optional

This property indicates an explicit index to use during a scan. This
is computed from query annotations in the query text (we usually call
them "hints" but this is a misnomer -- in practice we've always meant
them as mandatory things.)

See also: (actual) scan index, below.

No synthetic notation.

Example with memo:

```
> select * from ab@b_idx

                                     Logical property
0: (var a)
1: (var b)
2: (scan ab [0 1])                   @{req-idx: 'b_idx'}
root: 2
```

## Apparent presentation order

- Type: int list
- Category: physical, extra functional
- Origin: derived
- Kind: relational
- Level:  optional

This property indicates in which order the result columns are actually
presented when producing results. This is initially derived from the
order of columns in table/view descriptors, the order of join operands and
the order of projection targets, and is used to expand stars `*`
in SELECT clauses.

After expanding stars, this property becomes irrelevant during rewrite/search.

However it is populated for all intermediate relational expressions in
the final physical plan, to arrange the layout of buffers in memory.

See also: required presentation order.

Synthetic notation:

```sql
> select k, v, k from kv

PROPERTIES(
  PRESENTING (k, v, k)
)
```

## Apparent ordering

- Type: (set of) ordering information(s) - TBD
- Category: physical, semantic
- Origin: derived
- Kind: relational
- Level:  optional

This property indicates in which order the result rows happen to be
sorted. This is computed from the order of the scans and the
properties of the different algorithms in use.

The property is structured as a set of ordering arrays (array of
column index, direction).

Radu suggests that we may be OK with just one array, and dismiss any
incidental additional orderings discovered during query analysis
(e.g. those that emerge from the use of WITH ORDINALITY).

See also: required ordering.

Synthetic notation:

```sql
> select * from kv@v_idx

PROPERTIES(
  ORDERED BY (v ASC)
)

> select * from kv@v_idx WITH ORDINALITY

PROPERTIES(
  ORDERED BY (v ASC)
  ORDERED BY (ordinality ASC)
)
```

## (Actual) algorithm

- Type: enum (dependent on relational operator)
- Category: physical, extra-functional
- Origin: derived
- Kind: relational
- Level:  mandatory

This property determines which algorithm to use for a given
m-expression node. This is annotated during search and physical
planning.

Must be set and remain equal to the required algorithm, if any specified.

The final physical plan must have this annotated for every remaining
relational expression in the plan.

## Scan index

- Type: string
- Category: physical, extra-functional
- Origin: derived
- Kind: relational
- Level:  mandatory

This property determines which index to use during a table scan.
This is annotated during search and physical planning.

Must be set and remain equal to the required index scans, if any specified.

The final physical plan must have this annotated for every scan in the
query plan.

## Spans for scans

- Type: list of key spans
- Category: physical, semantic
- Origin: derived
- Kind: relational
- Level:  mandatory

This property determines which spans to scan from KV.
The list can be empty.

The final physical plan must have this annotated for every scan in the
query plan.

## Non-properties

The following attributes of a plan can be computed, but need not be
stored in the memo as properties.

- column labels. Labels disappear from the expressions during
  the prep phase, and only remains at the top level, as
  an attribute of the results for the entire query.

## Rationale and Alternatives

The rationale is to be found in the two RFCs linked to in the summary
section.

The entire text of the RFC is a set of alternatives, so no further
enumeration is needed here.

## Unresolved questions

TBD
