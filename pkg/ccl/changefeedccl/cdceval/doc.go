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
be configured with appropriate predicate and filtering expressions via ConfigurePredicates.

If the Evaluator is not configured with ConfigurePredicates, then each event is assumed
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

// TODO(yevgeniy): Various notes/questions/issues and todos.
//  1. Options issues:
//     * key_in_value: makes no sense; just "select *"
//     * key_only: currently unsupported by this flavor; would be nice to support it though
//       i.e. you only want the key, but you need "where" clause filtering.  Not clear how to express in sql.y
//     * VirtualColumnVisibility: null or omit -- both can be accomplished
//        * null: currently emitting null, but you can choose to emit null via "select ... null as vcolumn"
//        * omit: well, just don't select.
//     * On the other hand, we can also support "emit" mode, where we can compute vcolumn expression.
//     * updated and mvcc_timestamp options -- both supported via select
//     * Wrapped option -- does it make sense here.
//  3. Probably need to add more custom functions.
//     * Determine what to do with stable function overrides (now()) vs cdc_mvcc_timestamp.  Keep both? drop one?
//  4. How to surface previous row -- it's an open question.
//     * Option 1: provide cdc_prev() builtin which returns JSON encoding of previous row.
//       One of the negatives is that we are adding an additional JSONB encoding cost, though, this may not
//       be that horrible.  One interesting thing we could do with this approach is to also have a function
//       cdc_delta which reduces JSONB to contain only modified columns (cdc_delta(cdc_prev()).
//       Of course you could do something like this with "prev" table, but you'd have to "(case ...)" select
//       for each table column.
//       And since composition is so cool, you could use cdc_delta to determine if an update is not actually
//       and update, but an upsert event.
//     * Option 2: provide "prev" table.  Also has negatives.  Name resolution becomes more complex.  You could
//       legitimately have "prev" table, so you'd always need to alias the "real prev" table.  The prev table
//       is not specified via sql.y, so that's confusing.
//     * Regardless of the option, keep in mind that sometimes prev is not set -- e.g. w/out diff option
//       (here, we can return an error), but also not set during initial scan.  So, the query must handle
//       nulls in prev value.  Just something to keep in mind.
//   5. We must be able to return permanent errors from this code that cause changefeed to fail.
//      If filtering fails for a row (e.g. "select ... where col_a/col_b > 0" results in divide by 0),
//      this will fail forever, and so we must be able to return permanent error.
//   6. Related to 5, we must have poison message handling so we won't kill feed in cases like that.
//   7. Schema changes could cause permanent failures.
//   8. Multiple *'s are allowed.  But should they?
//   9. It is interesting to consider what access to prev does when we then send that data to encoder.
//      Right now, we hard code before/after datums; with predicates, we should probably change how things are encoded.
//      I.e. no "before"/"after" fields in json/avro -- instead, you select what you want to select.
//  10. Multi family support -- sort of breaks down because you get datums only for 1 family at a time.  Any expressions
//      comparing columns across families will fail.
//  11. Span constraints -- arguably the "holy grail" -- something that depends on the optiizer, but perhaps we
//      can find a way of using that w/out significant refactor to expose entirety of changefeed to opt.
//      Basically, given the set of predicates over primary key span, try to narrow the span(s) to those that can
//      satisfy predicates.
//  12. UI/Usability: Simple contradictions are detected -- but not all.  Even w/out contradiction, the user
//      may want to know which events match/not match, and how does the data look like.  We might need a mode
//      where the data always emitted, but it is marked somehow, indicating how the data will be handled.
// 13. We should walk expressions to determine if we need to turn on an option.  E.g. if we know user wants to filter
//     out deletes, we could push this predicate down to KV (once kv supports filtering).
//     Another idea is potentially detect if cdc_prev() is used and if so, turn on with diff option.
