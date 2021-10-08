Optimizer operator definitions
==============================

This directory contains definitions for the optimizer operators.

The syntax should be fairly self-evident from the existing definitions. Each
operator can contain, in this order:
 - any number of "child" expressions (relational and/or scalar);
 - at most one "private" field. If multiple fields are necessary, the private
   can be an embedded, separately defined structure (using similar syntax and
   the `Private` tag). The private fields are interned with the expression and
   can be used by rules; they must be initialized before construction of the
   expression. Private fields can be accessed by rules.
 - at most one `Typ` field (only for Scalar operators). If this field is not
   present, the scalar type of the operator is inferred from its inputs.
 - any number of unexported fields. Unexported fields are typically used to
   cache information that can be deduced from the children and the private. If
   there are unexported fields, an `initUnexportedFields(*Memo)` method must be
   implemented for the operator (in `opt/memo/expr.go`). This method is used to
   initialize these fields as necessary. The unexported fields cannot be used by
   rules (they are initialized after any normalization rules run).

Try to keep the formatting consistent. In particular:
 - use 4 spaces (no tabs) for indentation;
 - include a comment describing the operator;
 - include comments for non-trivial fields;
 - put Private definitions after the corresponding operator.

If a new type is necessary for a field definition, an entry must be added in
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
 - `CompositeInsensitive`: used for operators that only depend on the logical
   value of composite value inputs (see memo.CanBeCompositeSensitive).

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
 - `WithBinding`: used for operators which associate a `WithID` with the
   expression in the first child. Such expressions must implement a
   `WithBindingID()` method.

