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
Namely, this package concerns itself with 3 things:
  * Filter evaluation -- aka predicates: does the event match boolean expression.
  * Projection evaluation:  given the set of projection expressions, evaluate them.
  * (Soon to be added) Evaluation of computed virtual columns.

Evaluator is the gateway into the evaluation logic; it has 3 methods matching
the above use cases. Before filtering and projection can be used, Evaluator must
be configured with appropriate predicate and filtering expressions via configureProjection.

If the Evaluator is not configured with configureProjection, then each event is assumed
to match filter by default, and projection operation is an identity operation returning input
row.

Evaluator constructs a helper structure (exprEval) to perform actual evaluation.
One exprEval exists per cdcevent.EventDescriptor  (currently, a new exprEval created
whenever event descriptor changes; we might have to add some sort of caching if needed).

Evaluation of projections and filter expressions are identical.

First, we have "compilation" phase:
  1. Expression is "walked" to resolve the names and replace those names with tree.IndexedVar expressions.
     The "index" part of the indexed var refers to the "index" of the datum in the row.
     (note however: the row is abstracted under cdcevent package).  IndexedVar allows the value of that
      variable to be bound later once it is known; it also associates the type information
      with that variable.
  2. Expression is typed check to ensure that it is of the appropriate type.
     * Projection expressions can be of tree.Any type, while filters must be tree.DBool.
  3. Expression is then normalized (constants folded, etc).

It is an error to have a filter expression which evaluates to "false" -- in this case, Evaluator
will return a "contradiction" error.

After expressions "compiled", they can be evaluated; and again, both projections and filters use the same
logic (evalExpr() function); basically, all IndexedVars are bound to the Datums in the updated row, and the
expression is evaluated to the appropriate target type.

Expressions can contain functions.  We restrict the set of functions that can be used by CDC.
Volatile functions, window functions, aggregate functions are disallowed.
Certain stable functions (s.a. now(), current_timestamp(), etc) are allowed -- they will always
return the MVCC timestamp of the event.
We also provide custom, CDC specific functions, such as cdc_prev() which returns prevoius row as
a JSONB record.  See functions.go for more details.

***/
