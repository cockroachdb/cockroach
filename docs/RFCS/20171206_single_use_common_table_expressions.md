- Single-Use Common Table Expressions
- Status: completed
- Start Date: 2017-11-30
- Authors: Jordan Lewis
- RFC PR: #20374
- Cockroach Issue: #7029

# Summary

Implement a simple subset of common table expressions that permits naming
result sets and using each of them at most once in a statement. Full support
for common table expression (henceforth CTEs) requires temporary table
infrastructure, which is currently missing from CockroachDB. This proposal aims
to fast-track the implementation of the subset of CTEs that doesn't require
temporary tables, providing our users with partial compatibility and query
readability boons at little cost.

The supported subset will be known as single-use CTEs, and consists of:
- Full `WITH ... AS` syntax support, with unlimited clauses
- Each clause can be referenced as a data source at most once

Features that are not present in single-use CTEs:
- Using a CTE more than once in a data source will not be included because
  implementing it requires temporary tables
- Correlated CTEs will not be supported because we don't yet support correlated
  subqueries in general.
- `WITH RECURSIVE` will not be included because implementing it requires
  temporary tables and a complex new execution strategy.

# Motivation

Common table expressions (CTEs), or `WITH` clauses, are a standard SQL 1999
feature that permit naming the result sets of intermediate statements for use
in a single larger `SELECT`, `INSERT/UPSERT`, `UPDATE` or `DELETE` statement.
In full CTEs, the named statements can be referred to unlimited times
throughout the rest of the statement. To preserve the referential integrity of
the names of the statements, the execution engine must ensure that each impure
clause (either one that modifies data or one that uses an impure builtin
function) is not executed more than once. CTEs increase the expressivity of SQL
by adding some syntactic sugar and new access patterns:

1. Statements with many subqueries can be made more readable by extracting the
   subqueries into named, top-level CTE clauses
2. The results of `INSERT/UPSERT/UPDATE/DELETE` statements that use `RETURNING`
   can be captured in a named result set, which is not possible with ordinary
   subuqeries
3. Recursive statements for tasks like graph traversal can be written with a CTE
   clause that references itself, using the `WITH RECURSIVE` syntax
4. Statements can reference a named CTE clause more than once, enabling patterns
   that join complex datasets to themselves

CTEs are a frequently requested feature for CockroachDB, both for compatibility
and developer quality-of-life reasons. Implementing CTEs in full is quite
involved, because of the requirement that each CTE clause is referentially
transparent. Impure statements can have arbitrarily large result sets, so
temporarily accumulating these result sets for use by other statements requires
infrastructure for temporary tables to ensure that CockroachDB doesn't run out
of memory while executing them.

However, many CTE use cases only need the syntactic sugar provided in points 1
and 2 above. None of the queries mentioned in CockroachDB's CTE issue #7029 use
features missing from the proposed subset, for example. Also, several ORMs
including ActiveRecord use single-use CTEs as part of their schema introspection
routines.

Therefore, this proposal aims to implement just the syntactic sugar in points 1
and 2 above, additionally imposing the restriction that each CTE clause may not be
referred to as a data source more than once. We believe that providing this
subset of CTEs will be so beneficial for our users that we shouldn't delay
implementing the feature until after temporary tables are available at some
undetermined time in the future.

An
[initial implementation](https://github.com/cockroachdb/cockroach/pull/20359)
of this subset was straightforward and weighed in at less than 300 lines of new
non-test code.

# Guide-level explanation

Implementing this proposal would enable the `WITH` statement syntax for
unlimited named result sets per statement, as long as each result set is not
accessed more than once.

The following syntax sketch aims to show the salient features of the CTE syntax:

```
WITH name1 AS (<dml_with_results>),
     name2 AS (<dml_with_results> that can reference name1),
     name3 AS (<dml_with_results> that can reference name1 and name2),
     ...
     nameN AS (<dml_with_results> that can reference all names above)
<dml> that can reference all names above
```

where `<dml_with_results>` is any `SELECT` or `INSERT`, `UPSERT`, `UPDATE` or
`DELETE` that uses the `RETURNING` clause, and `<dml>` is any of the above
statements with or without `RETURNING`.

The following example demonstrates the query-factoring capability of CTEs:

```
--- Original query:

INSERT INTO v SELECT * FROM
  (SELECT c FROM u JOIN
    (SELECT a, b FROM t WHERE a < 5) AS x(a, b)
    ON u.a = x.a WHERE b > 10)
  AS y

--- CTE'd equivalent of the above:
WITH x(a, b) AS (SELECT a, b FROM t WHERE a < 5),
     y       AS (SELECT c from u JOIN x ON u.a = x.a WHERE b > 10)
INSERT INTO v SELECT * from y
```

The second version is more readable, since the subquery nesting is replaced
with a lateral set of result set declarations.

Here's an example with `RETURNING` clauses in the CTE clauses:
```
WITH x AS (INSERT INTO t(a) VALUES(1) RETURNING a),
     y(c) AS (DELETE FROM u WHERE a IN (x) RETURNING b),
     z AS (SELECT a FROM v WHERE a < 10)
SELECT * FROM z JOIN y ON z.a = y.c;
```

In this example, the outputs of an `INSERT` and `DELETE` statement are each
used as a named result set, something that's not possible with ordinary
subqueries.

Each CTE clause can itself have nested `WITH` clauses or subqueries, in which
case the names from the outer CTE are still made available to the inner
queries. For example:

```
WITH x AS (SELECT c FROM a),
     y AS (SELECT d FROM b WHERE e IN (SELECT p from c WHERE q IN x)
SELECT * FROM y WHERE d > 5;
```

In this case, the subquery in clause `y` can still reference the clause `x`
from the outer CTE.

Each clause can only reference named result sets that were defined before in
the statement, and clauses can't reference themselves. Additionally, each
result set can only be used as a data source once by subsequent CTE clauses and
the main statement clause. For example, the following CTEs would not be
supported by the proposal:

```
--- Sum the integers from 1 to 10
--- Not supported: clauses can't reference themselves.
WITH RECURSIVE t(n) AS (
    SELECT 1
  UNION ALL
    SELECT n+1 FROM t
)
SELECT SUM(n) FROM t LIMIT 10;

--- Not supported: can't reference a clause more than once.
WITH x(a) AS (SELECT a FROM t),
     y(b) AS (SELECT a + 1 FROM x)
SELECT * FROM x JOIN y ON x.a = y.b;
```

As a more realistic example, implementing this proposal would permit
CockroachDB to execute the complete sample query suggested by the popular CTE
blog post
[The Best Postgres Feature You're Not Using – CTEs Aka WITH Clauses](http://www.craigkerstiens.com/2013/11/18/best-postgres-feature-youre-not-using/):

```sql
--- Initial query to grab project title and tasks per user
WITH users_tasks AS (
  SELECT
         users.id as user_id,
         users.email,
         array_agg(tasks.name) as task_list,
         projects.title
  FROM
       users,
       tasks,
       project
  WHERE
        users.id = tasks.user_id
        projects.title = tasks.project_id
  GROUP BY
           users.email,
           projects.title
),

--- Calculates the total tasks per each project
total_tasks_per_project AS (
  SELECT
         project_id,
         count(*) as task_count
  FROM tasks
  GROUP BY project_id
),

--- Calculates the projects per each user
tasks_per_project_per_user AS (
  SELECT
         user_id,
         project_id,
         count(*) as task_count
  FROM tasks
  GROUP BY user_id, project_id
),

--- Gets user ids that have over 50% of tasks assigned
overloaded_users AS (
  SELECT tasks_per_project_per_user.user_id,

  FROM tasks_per_project_per_user,
       total_tasks_per_project
  WHERE tasks_per_project_per_user.task_count > (total_tasks_per_project / 2)
)

SELECT
       email,
       task_list,
       title
FROM
     users_tasks,
     overloaded_users
WHERE
      users_tasks.user_id = overloaded_users.user_id
```

This query is executable by the single-use CTE implementation since every named
result set is used at most once. As you can see, trying to represent this
massive query as a single nested set of subselects would be much more difficult
to read. This improvement in readability is a major reason why CTEs are
popular, and something that we could easily provide today without waiting for
temporary tables.

# Reference-level explanation

CockroachDB already supports using arbitrary plans as plan data sources, so the
bulk of the implementation of the single-use CTEs is adding syntactic sugar and
a name resolution mechanism for CTE subclauses.

Specifically, the planner will be augmented to include a naming environment
that is composed of one frame per CTE statement. Each frame will consist of a
mapping from CTE clause name to a query plan for executing the statement
corresponding to the name, as well as any column aliases the user might have
provided. The environment will be treated as a stack. Planning a CTE pushes
a new frame onto the stack, and finishing planning that CTE pops the frame.

Data source resolution will be augmented to search through the naming
environment from the top of the stack to the bottom before searching for
tables. CTE names can shadow table names and other CTE names in an outer scope,
but an individual CTE statement can't contain more than one clause with any
given name.

To enforce this proposal's restrictions, the naming environment will also
include a flag on each named clause that is set when it is used as a data
source in another clause or the main statement. This flag will allow the
planner to detect when a query tries to reference a table more than once and
return a suitable error.

Because of this proposal's restrictions, temporary table infrastructure is not
necessary as each CTE clause will stream its output to the plan that references
it just like an ordinary CockroachDB plan tree.

Performance of planning ordinary changes will not be meaningfully affected,
since the naming environment doesn't have to get checked if its empty.

## Drawbacks

Despite the fact that completing this proposal would provide strictly more
functionality to our users, it might be risky to ship an incomplete version of
common table expressions from an optics perspective. We wouldn't want to give
users the impression that we don't care about fully implementing features that
we claim to support.

This risk can be mitigated by setting expectations carefully in the docs and
feature release notes. As long as we don't claim to have full CTE support,
people won't be unduly surprised when they can't use some of the more complex
functionality that CTEs offer.

## Rationale and Alternatives

This design is a simple incremental step toward providing common table
expressions. Whether or not we choose to ship this partial CTE implementation,
it's a good idea to start with this simpler set of functionality to establish a
baseline for testing.

As an alternative, we could punt on CTEs entirely until temporary tables are
available for internal use, and then deliver a full implementation of CTEs all
at once.

The impact of waiting to implement this functionality is that we might turn
away potential users that expect to be able to use CTEs.

## Unresolved questions

None.

## Future work

Implementing the rest of CTEs has 2 stages: temporary storage to enable
multi-use clauses, and execution engine changes to enable `WITH RECURSIVE`.

Temporary storage may be less involved than fully implementing temporary
tables. DistSQL processors can be configured to use temporary storage in a
processor if necessary, so it's possible that all this will take is plugging
the temporary storage infrastructure into the local execution engine in front
of clauses that need to be used more than once, or waiting until the two
engines are merged.

`WITH RECURSIVE` will take some additional thought, and possibly another RFC.
