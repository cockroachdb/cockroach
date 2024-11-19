- Feature Name: Generic Query Plans
- Status: in-progress
- Start Date: 2024-01-03
- Authors: Marcus Gartner (@mgartner)
- RFC PR: [#117284](https://github.com/cockroachdb/cockroach/pull/117284)

# RFC: Generic Query Plans

## Summary

**Generic query plans** are designed to reduce the computational burden of query
optimization that is required to service a workload. Generic query plans are:

  - Fully optimized once and cached
  - Reused in later executions of the same statement without being copied or 
    re-optimized
  - Sometimes less optimal than non-generic query plans

## Motivation

Query planning is a computationally expensive step in answering queries in
real-world workloads. In high-throughput workloads, profiles have shown query
planning consuming a large fraction of CPU time. In latency sensitive workloads,
query optimization often accounts for a sizable portion of a query's total
latency. Query planning latency increases with the complexity of a query, and in
some cases, such as queries with many joins, the time to plan a query is
significantly longer than the time to execute the query plan. Any reductions in
query planning overhead can significantly improve the overall performance of
CockroachDB.

There are a handful of existing workarounds for reducing query planning
overhead. Our suggestions typically include using prepared statements to cache
partially optimized query plans for reuse during `EXECUTE`, adding join hints to
avoid exploration of certain types of joins or join orderings, and lowering the
`reorder_joins_limit` setting to reduce the number of join orderings explored.
These tricks can be highly effective at reducing query planning overhead, but
they don't eliminate it entirely.

We've also combated query planning overhead with constant efforts to improve the
optimizer's performance. The optimizations thus far have largely focused on
special case fast-paths (e.g., [the placeholder
fast-path](https://github.com/cockroachdb/cockroach/blob/8389ac4a8c922529eb368437976135403fcb1b68/pkg/sql/opt/xform/placeholder_fast_path.go))
and alleviating inefficiencies without sacrificing the quality of query plans
(e.g., [#113319](https://github.com/cockroachdb/cockroach/pull/113319),
[#114666](https://github.com/cockroachdb/cockroach/pull/114666), and
[#87920](https://github.com/cockroachdb/cockroach/pull/87920)). Work in these
areas will continue to yield progress, but there are caveats to these
strategies. Special case fast-paths, by definition, only apply to a narrow
subset of queries, not all queries in general. Optimizations for specific
inefficiencies can only be addressed once the inefficiencies are discovered,
which, because of the breadth of the SQL language and the diversity in schema
designs, typically happens when a customer runs into them in production.
Therefore, these optimizations are usually applied reactively rather than
proactively.

Despite the efficacy of workarounds and planning optimizations, recent customer
feedback has revealed that, for some workloads, more general and drastic
improvements are required (see support issue
[#2726](https://github.com/cockroachdb/cockroach/pull/2726)). Reoptimizing a
query on every execution is too costly. For complex queries, even the cost of
copying the cached query plan to ready it for optimization is too high. Generic
query plans will eliminate a large portion of query planning overhead by caching
a fully optimized plan and reusing it for each execution.

Additional applications of this particular design of generic query plans are a
secondary motivation. Recursive CTEs and apply-joins are notoriously slow
operations because they reoptimize a subtree of the query plan for each input
row. Using a generic query plan to avoid reoptimization could significantly
improve performance.

## Technical Design

### Overview

The current cache strategy of the optimizer is to cache normalized plans during
the `PREPARE` phase, then during the `EXECUTE` phase copy the cached plan,
re-normalize it, and apply transformation rules (the exceptions to this strategy
are queries without placeholders and when the placeholder fast-path is
applied—in both cases the query is fully optimized during `PREPARE`). Note that
copying the cached plan is more involved than the terseness of "copy" implies.
It is an expensive
[deep-copy](https://github.com/cockroachdb/cockroach/blob/f83b13a90aee34b6ebdcdb3352b7331cf4bdf903/pkg/sql/opt/norm/factory.go#L336)
that replaces placeholders with their associated values and recalculates logical
properties such as functional dependencies and statistics. Postgres has a
similar strategy and calls the plans produced from it custom plans. Custom plans
reduce some of the work to plan a query during `EXECUTE`, like parsing, semantic
analysis, and partial optimization with normalization rules.

Generic plans, also supported by Postgres, are fully optimized once with both
normalization and exploration rules, and reused for future executions of the
query. Thus, the overhead of optimization is not incurred after the generic plan
is initially generated.

Importantly, generic plans can be reused without being copied. A mutable copy of
a cached plan is not needed because it is not reoptimized at execution time.
`execbuilder` can build execution nodes directly from the cached generic plan, as
it does when the placeholder fast-path is used. This avoids the deep-copy of
cached plans that is required under the current plan-reuse strategy.

Generic plans may be less optimal than custom plans for reasons including
inferior row count estimates compared to custom plans and the inability to apply
some exploration rules. Mitigations for these limitations are discussed in more
detail below.

### Enabling Generic Query Plans

As in Postgres, the `plan_cache_mode` session setting will control the use of
generic query plans. Three options will be available: `force_custom_plan`,
`force_generic_plan`, and `auto`. The first two force the use of a custom or
generic plan, respectively. CockroachDB's current behavior matches the
`force_custom_plan` setting, so it will be the default value initially to avoid
plan regressions in early versions of generic query plans.

There is one exception for `force_custom_plan` with statements that either have
no placeholders nor fold-able, stable expressions, or statements that can
utilize the placeholder fast path. These statements can be fully optimized into
_ideal_ generic plans that can be reused without re-optimization because the
optimal query plan will not change between executions.

If `plan_cache_mode` is set to `auto`, the optimizer will automatically choose
between a custom and generic plan, using simple heuristics. Similar to
[Postgres's
logic](https://github.com/postgres/postgres/blob/cca97ce6a6653df7f4ec71ecd54944cc9a6c4c16/src/backend/utils/cache/plancache.c#L1017-L1070)
for `auto` (see
[documentation](https://www.postgresql.org/docs/current/sql-prepare.html)), the
optimizer will use a generic plan if its cost is less than the average cost of
the first five custom plans plus some additional overhead for the cost of custom
planning (e.g., based on the number of joins or relations in the plan). This
should minimize false-positives (i.e., cases where an inefficient generic query
plan is chosen over a more efficient custom query plan), which are far more
critical to reduce than the false-negatives (i.e., cases where a custom plan is
chosen over a generic query plan that is just as efficient) because a bad query
plan can be catastrophic to a workload.

The `plan_cache_mode` setting is considered at execution time, not at prepare
time. This means that prepared statements aren't locked to a specific mode when
they are created and users have the flexibility to change modes when using
client libraries and ORMs that give little or no ability to control when
statements are prepared.

### Prepared Statement Namespace

The prepared statement namespace will be expanded to store both a "base memo"
and a "generic memo". The base memo will be a normalized, yet unoptimized memo
that can be used as a starting point for build custom plans. The generic memo
will be fully optimized as an ideal generic query plan or non-ideal generic
query plan.

### Using Lookup-Joins as "Constrained Scans"

Constraints are some of the most critical details in a fully optimized query
plan. They are used to represent a concrete range of KVs to scan in constrained
scans and other expressions. In order to make useful generic query plans, the
optimizer must create some form of _parameterized constraints_ with placeholder
values that can be filled in later.

Unfortunately, using the same or similar constraint data structure to represent
parameterized constraints is fraught with limitations. We can trivially convert
the expression `a = 'foo' OR a = 'bar'` into the constraint `/a: [/'foo' -
/'foo'] [/'bar' - /'bar']`. So it's tempting to think we can simply convert the
expression `a = $1 OR a = $2` into the constraint `/a: [/$1 - /$1] [/$2 - /$2]`
and fill in the constraint's placeholders at execution time. Not so fast! If the
same value is provided for `$1` and `$2`, then the constraint would have two
duplicate spans, and scanning over them would produce incorrect results. We'll
also run into trouble if either placeholder value is `NULL`. We'll scan over
`NULL` keys when we shouldn't because `a = NULL` is always falsey.

As another example, consider the expression `a <= 0 OR a >= 10` which can be
represented as the constraint `/a: [ - /0] [/10 - ]`. The naive conversion of `a
<= $1 OR a >= $2` would yield `/a: [ - /$1] [/$2 - ]`. If `$1` is greater than
`$2`, then the spans overlap and we'll produce incorrect results.

To avoid these pitfalls, we'll convert Select-Scan expressions with placeholders
in filters into joins where the left-hand side is a Values expression producing
the placeholders and the right-hand side is the Scan. This allows further
transformation rules to convert the joins into lookup-joins. Below is an example
showing the progress of transformation rules:

<img width="1297" alt="image" src="https://github.com/cockroachdb/cockroach/assets/1128750/a7d96ca5-dd56-45dc-9bc5-2c50cbdbf53f">

The lookup join effectively fills the role of a _parameterized constrained
scan_, unlocking optimizations in the presence of placeholders. The lookup key
columns or lookup expression are a form of parameterized constraints. In many
cases, the theoretical performance of these parameterized lookup joins is the
same as a traditional constrained scan.

### Stable Expressions

Stable expressions (e.g., `now()`), cannot be folded to constant values in query
plans that are reused because they can produce different values during each
execution of the query. With custom query plans, we avoid folding the expression
until execution time, and use the resulting value to aid in generating index
scan constraints during optimization of the cached plan. Generic query plans
will include unfolded stable expressions in the Values expressions beneath
parameterized lookup-joins, allowing for optimizations similar to the
optimizations with placeholders described in the previous section.

### Statistics

In general, generic query plans will be less optimal than custom query plans
because they are optimized without knowledge of placeholder values. This will
mostly hinder the optimizer's ability to generate accurate row count estimates
for each relational expression. Most notably, histograms in table statistics
cannot be filtered with parameterized constraints, so the optimizer will
fallback to estimating row counts with simpler stats. Also, the number of
distinct values in a constraint span, which is used to better inform row count
estimates, is impossible to determine from a parameterized constraint (unless
it's an equality condition). The best tools for fixing bad generic query plans
will be index hints and join hints.

### Casting Placeholder Values

Placeholder values that do not match the type of the column that they are
constraining can be implicitly cast to the column type. This is only allowed if
an assignment cast from the placeholder value to the column's type is allowed.
Note that a regular cast is performed, however, not an assignment cast which can
have slightly different behavior. If an assignment cast is not allowed, then the
query will result in an error.

### Partial Indexes

A partial index can only be used in a query plan when it can be proven that the
query filters imply the partial index's predicate. In general, it is impossible
to prove that filters with placeholders imply predicates. For example, it is
impossible to determine whether or not the expression `a > $1` implies the
predicate `a > 0` without knowing the value of `$1`. Therefore, generic plans in
general will only use partial indexes if the query contains filters with
constant values that imply the predicate.

However, there is one common form of partial index predicate where we can do
better. Predicates like `a IS NOT NULL`, which are quite common in real-world
workloads, are implied by any NULL-rejecting filter for column `a`. For example,
the expression `a = $1` is falsey if `a` is `NULL`, therefore, it implies `a IS
NOT NULL`. Accounting for this special case will allow partial indexes to be
used in generic query plans in more cases.

### Testing

In addition to typical unit and integration tests, we can gain confidence in the
correctness of the generic query plans implementation by creating a roachtest
that compares the results of a randomly generated query when run with a custom
query plan and a generic query plan.

## Future Work

### Lookup-join Improvements

Lookup-joins cannot currently be constrained in all the same ways that scans can
be constrained. Generic query plans could be suboptimal because of this. As one
example, hard limits cannot be pushed into lookup joins. Queries with hard
limits may have plans with full table scans or lookup-joins that produce an
unbounded number of rows.

We can incrementally add capabilities to lookup joins in order to address these
shortcomings. A positive side effect is that custom query plans can also benefit
from any additional capabilities. [Preliminary analysis of 23 types of
queries](https://gist.github.com/mgartner/1e62d3e70db8c1f25447c37ca0f4cb7a#file-q05-sql)
shows 7 types of queries that will have suboptimal generic query plans when
using lookup-joins as-is. This is a good starting place for future improvements.

### Parameterized Constraints

At some point, we may hit a limit on the types of constraints a lookup-join can
represent. Or we may find that the overhead of parameterized lookup-join
execution is simply too great compared to the overhead of a simpler scan
operation. In this event, we can explore implementing proper parameterized
constraints. This section describes one possible implementation.

A scalar `FiltersExpr` field on a `ScanExpr` could represent a parameterized
constraint. The conversion from a filter into a proper constraint can be
deferred until execution time (specifically during the `execbuilder` phase). The
filters will include explicit filters and optional filters (derived from check
constraints and computed column expressions) which are likely to constrain a
specific index when placeholder values are filled in.

The existing `idxconstraints` package, currently used when generating
constrained scans, can be used for building constraints from the parameterized
filters after making a few tweaks. New logic will be added for picking filters
that are likely to constrain the index at execution time. At optimization time
the optimizer will be unable to determine if the constraint will be "tight"
(i.e., it exactly represents the filters) so the query plan will always include
a filter operator above the scan, even if it ends up being unnecessary. At
execution time, it may be determined that the parameterized constraint is a
contradiction given the placeholder values (e.g., `a > $1 AND a < $2` where `$1`
and `$2` are both `0`). In this case, the scan will have no spans or become an
empty values operator.

There are a few downsides to this approach. First, the filters have to be
traversed for every execution of the query in order to build the constraint.
This should typically be very fast, but we have seen cases where constraint
building can be slow and consume a lot of memory (see
[#106887](https://github.com/cockroachdb/cockroach/pull/106887)). Second,
because the logic for picking parameterized filters is separate from the logic
that actually generates the constraint, there may be cases where the
parameterized filters cannot constrain the index and the scan becomes a full
table scan at execution time. Building an alternative constraint-building
library that is closely coupled with the selection of parameterized filters
could mitigate both these downsides in the long-term.

Another downside is that the filter operator that is always applied above scans
may add non-negligible overhead in cases where it is not necessary. To mitigate
this we may be able to prove at optimization time that some filters will always
produce tight constraints and the filter operator can be omitted. Alternatively,
we may be able to conditionally remove the filter operator during the
`execbuilder` phase at execution time if tight constraints were generated for
the scan.

### Using Base Memos as Starting Points for Generic Memo Optimization

Initially, generic query plans will be built from the SQL string when an
existing, non-stale generic plan is not available. In the future, we can reduce
some of this work if a normalized, base query plan is available. The base plan
can be used as a starting point for building the generic plan, eliminating some
of the overhead of `optbuilder` and normalization.

### Replacing the placeholder fast path

Generic query plans should make the placeholder fast path obsolete. Once we have
confidence that generic query plans cover all the same cases, we should remove
the placeholder fast path to reduce complexity.
