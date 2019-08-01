Optimizer operator definitions
==============================

This directory contains definitions for the optimizer operators.

The syntax should be fairly self-evident from the existing definitions. Try to
keep the formatting consistent. In particular:
 - use 4 spaces (no tabs) for indentation;
 - include a comment describing the operator;
 - include comments for non-trivial fields;
 - put Private definitions after the corresponding operator.

If a new type is necessary for a Private definition, an entry must be added in
optgen (`opt/optgen/cmd/optgen/metadata.go`), and `HashXX / IsXXEqual` functions
must be added to the interner (`opt/memo/interner.go`).

## Tags

Operators can have various tags. This section lists all the used tags and their
intended semantics.

General operator type tags (each operator has exactly one of these tags):
 - `Relational`: used for relational operators.
 - `Scalar`: used for scalar operators.
 - `Enforcer`: used for enforcers.

Tags for scalar operators:
 - `List`: used for scalar lists operators.
 - `ListItem`: used for scalar list items.
 - `Binary`: used for binary operators.
 - `Bool`: used for operators that always return a boolean.
 - `Int`: used for operators that always return an integer.
 - `Float`: used for operators that always return a float.
 - `Comparison`: used for binary operators that return a boolean.
 - `ConstValue`: used for operators that represent a constant value.
 - `Aggregate`: used for operators that represent an aggregation function.
 - `Window`: used for operators that represent a window function.

Tags for relational operators:
 - `Join`: used for logical variations of join, including apply joins (but not
   physical variations like lookup join).
 - `JoinApply`: used for logical variations of apply join.
 - `JoinNonApply`: used for logical variations of join, not including apply
   joins.
 - `Grouping`: used for aggregation operators.
 - `Set`: used for set operation operators (like Union, Intersect).
 - `Telemetry`: if used, triggers operator usage tracking via telemetry.
 - `Mutation`: used for operators that can mutate data as part of the
   transaction. This includes DML operations like INSERT, as well as DDL
   operations. In general, anything that can't execute in a read-only
   transaction must have this tag.
 - `DDL`: used for schema change operations; these operators cannot be executed
   following a mutation in the same transaction. Should always be used in
   conjunction with the `Mutation` tag.
