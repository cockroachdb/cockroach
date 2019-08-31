// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package ordering contains operator-specific logic related to orderings - whether
ops can provide Required orderings, what orderings do they need to require from
their children, etc.

The package provides generic APIs that can be called on any RelExpr, as well as
operator-specific APIs in some cases.

Required orderings

A Required ordering is part of the physical properties with respect to which an
expression was optimized. It effectively describes a set of orderings, any of
which are acceptable. See OrderingChoice for more information on how this set is
represented.

An operator can provide a Required ordering if it can guarantee its results
respect at least one ordering in the OrderingChoice set, perhaps by requiring
specific orderings of its inputs and/or configuring its execution in a specific
way. This package implements the logic that decides whether each operator can
provide a Required ordering, as well as what Required orderings on its input(s)
are necessary.

Provided orderings

In a single-node serial execution model, the Required ordering would be
sufficient to configure execution. But in a distributed setting, even if an
operator logically has a natural ordering, when multiple instances of that
operator are running on multiple nodes we must do some extra work (row
comparisons) to maintain their natural orderings when their results merge into a
single node. We must know exactly what order must be maintained on the streams
(i.e. along which columns we should perform the comparisons).

Consider a Scan operator that is scanning an index on a,b. In query:
  SELECT a, b FROM abc ORDER BY a, b
the Scan has Required ordering "+a,+b". Now consider another case where (as part
of some more complicated query) we have the same Scan operator but with Required
ordering "+b opt(a)"¹, which means that any of "+b", "+b,±a", "±a,+b" are
acceptable. Execution would still need to be configured with "+a,+b" because
that is the ordering for the rows that are produced², but this information is
not available from the Required ordering "+b opt(a)".

¹This could for example happen under a Select with filter "a=1".
²For example, imagine that node A produces rows (1,4), (2,1) and node B produces
rows (1,2), (2,3). If these results need to be merged on a single node and we
configure execution to "maintain" an ordering of +b, it will cause an incorrect
ordering or a runtime error.

To address this issue, this package implements logic to calculate Provided
orderings for each expression in the lowest-cost tree. Provided orderings are
calculated bottom-up, in conjunction with the Required ordering at the level of
each operator.

The Provided ordering is a specific opt.Ordering which describes the ordering
produced by the operator, and which intersects the Required OrderingChoice (when
the operator's FDs are taken into account). A best-effort attempt is made to
keep the Provided ordering as simple as possible, to minimize the comparisons
that are necessary to maintain it.
*/
package ordering
