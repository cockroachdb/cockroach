// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

/***

cdceval package is a library for evaluating various expressions in CDC.

The expression evaluation and execution is integrated with planner/distSQL.

First, when starting changefeed, CDC expression is planned and normalized
(NormalizeAndPlan).  The normalization step involves various sanity checks
(ensuring for example that expression does not target multiple column familes).
The expression is planned as well because this step, by leveraging optimizer,
may simplify expressions, and, more importantly, can detect if the expression
will not match any rows (this results in an error being returned).

The normalized expression is persisted into job record.
When changefeed job starts running, it once again plans expression execution.
Part of the planning stage figures out which spans need to be scanned.  If the
predicate restricted primary index span, then we will scan only portion
of the table.

Then, once aggregators start up, they will once again plan the expression
(sql.PlanCDCExpression), but this time each incoming KV event will be evaluated
by DistSQL to produce final result (projection).
PlanCDCExpression fully integrates with optimizer, and produces a plan
that can then be used to execute the "flow" via normal
distSQL mechanism (PlanAndRun).

What makes CDC expression execution different is that CDC is responsible for
pushing the data into the execution pipeline.  This is accomplished via
execinfra.RowReceiver which is returned as part of the plan.
CDC will receive rows (encoded datums) from rangefeed, and then "pushes" those
rows into execution pipeline.

CDC then reads the resulting projection via distSQL result writer.

Evaluator is the gateway into the evaluation logic;  it takes care of running
execution flow, caching (to reuse the same plan as long as the descriptor version
does not change), etc.

Expressions can contain functions.  We restrict the set of functions that can be used by CDC.
Volatile functions, window functions, aggregate functions are disallowed.
Certain stable functions (s.a. now(), current_timestamp(), etc) are allowed -- they will always
return the MVCC timestamp of the event.

Access to the previous state of the row is accomplished via (typed) cdc_prev tuple.
This tuple can be used to build complex expressions around the previous state of the row:
   SELECT * FROM foo WHERE status='active' AND cdc_prev.status='inactive'

During normalization stage, we determine if the expression has cdc_prev access.
If so, the expression is rewritten as:
  SELECT ... FROM tbl, (SELECT ((crdb_internal.cdc_prev_row()).*)) AS cdc_prev
The crdb_internal.cdc_prev_row function is created to return a tuple based on
the previous table descriptor.  Access to this function is arranged via custom
function resolver.

In addition, prior to evaluating CDC expression, the WHERE clause is rewritten as:
  SELECT where, ... FROM tbl, ...
That is, WHERE clause is turned into a boolean datum.  When projection results are
consumed, we can determine if the row ought to be filtered.  This step is done to
ensure that we correctly release resources for each event -- even the ones that
are filtered out.

Virtual computed columns can be easily supported but currently are not.
To support virtual computed columns we must ensure that the expression in that
column references only the target changefeed column family.

***/
