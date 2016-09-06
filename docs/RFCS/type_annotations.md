- Feature Name: SQL Type Annotations
- Status: completed
- Start Date: 2016-05-25
- Authors: Nathan, knz
- RFC PR: #6895
- Cockroach Issue: #6189


# Summary

This RFC proposes to add a new "type annotation" syntax to our SQL dialect
(referred to as CockroachSQL from now on). This was originally proposed as
part of the Summer typing system, but is being split up separately because
it is purely an extension which was not necessary for the immediate
correctness of the type system.

# Motivation

In order to clarify the typing rules during the design of the Summer
type system and to exercise the proposed system, we found it was useful
to "force" certain expressions to be of a certain type.

Unfortunately, the SQL cast expression (`CAST(... AS ...)` or
`...::...`) is not appropriate for this, because although it
guarantees a type to the surrounding expression it does not constrain
its argument. For example `sign(1.2)::int` does not disambiguate which
overload of `sign` to use.

Therefore we propose the following SQL extension, which was not
required for the correctness of the Summer typing system but offers 
opportunities to better exercise it in tests. The extension also gives
users of our SQL dialect more control over the type inference decisions
of the type system.

The need for this type of extension was also implicitly
present/expressed in the alternate proposals Rick and Morty.

# Detailed design

The extension is a new "type annotation" expression node.

### Operator Syntax

We define the following SQL syntax: `E ::: T`, where `E` is a sub-expression
and `T` is a type. We chose this syntax because it does not collide (visually
or logically) with other SQL syntax, and because it draws parallels to the
cast operator syntax.

For example: `1:::int` or `1 ::: int`.

The meaning of this at a first order approximation is "interpret the
expression on the left using the type on the right".

### Keyword Syntax

In addition, we define the keyword syntax: `ANNOTATE_TYPE(E, T)`, where `E`
is a sub-expression and `T` is a type. We chose this syntax because it draws
parallels to the cast keyword syntax, without requiring special client handling.
We could have opted for an `ANNOTATE_TYPE(E AS T)` syntax, which would have been
more similar to the cast keyword syntax, but would not be usable by ORMs or other
SQL client abstraction layers because it would not look like a function call.

For example: `ANNOTATE_TYPE(1, int)`.

### Type Annotations vs. Type Casts

As expressed above, type annotations and type casts play different roles.
The primary purpose of a type cast is to take a given expression of type
`T1` and map it during evaluation to a type `T2`. This cast can be used to
work around disjoint types between a sub-expression and its parent context,
but it has no effect on typing of the sub-expression itself. In practice 
with the Summer type system, this means that during type checking, a 
`CastExpr` will throw away any recursively passed `desiredType`, and will 
propagate down `NoPreference` to its sub-expressions.

On the other hand, type annotations have no runtime effect (as implied by
the term "annotation"). Instead, the annotation only has an effect during
semantic analysis. The annotation will do what it can to type its sub-expression
as its annotation type, and then assert that the sub-expression does get typed
as this type, throwing an error if not. In a "bottom-up" type system, the 
only thing an annotation like this could do is assert after its sub-expression's
type has been resolved that it matches its annotation type. However, in a
bi-directional typing system like Summer, the annotation can be more effective
by also "desiring" its annotation type from its children. In effect, this means
that the sub-expression will become the desired type if possible.

Examples

```sql
    SELECT 1.2
        -> no preference for the return type of `1.2`
        -> numeric constant without a preference defaults to DECIMAL
        -> the expression returns a DECIMAL
    SELECT 1.2::float
        -> no preference for the return type of CAST(... as FLOAT)
        -> no preference for the return type of `1.2`
        -> numeric constant without a preference defaults to DECIMAL
        -> CastExpr returns a FLOAT instead, which it will cast during evaluation
        -> the expression returns a FLOAT
    SELECT 1.2::string
        -> no preference for the return type of CAST(... as STRING)
        -> no preference for the return type of `1.2`
        -> numeric constant without a preference defaults to DECIMAL
        -> CastExpr returns a STRING instead, which it will cast (format) during evaluation
        -> the expression returns a STRING
    SELECT 1.2:::float
        -> no preference for the return type of `... ::: FLOAT`
        -> FLOAT preference passed for the return type of `1.2`
        -> numeric constant contains FLOAT in its "resolvable type set", so it resolves
           itself as a FLOAT
        -> the type annotation correctly asserts that its sub-expression returns a FLOAT
        -> the type assertion itself returns a float, and does nothing during evaluation
        -> the expression returns a FLOAT
    SELECT 1.2:::string
        -> no preference for the return type of `... ::: STRING`
        -> STRING preference passed for the return type of `1.2`
        -> numeric constant does not contains STRING in its "resolvable type set", so it 
           resolves itself to the default type of DECIMAL instead
        -> the type annotation throws an error when asserting that its sub-expression 
           returns a STRING, type checking fails

    SELECT sign(1.2)
        -> no preference for the return type of `sign`
        -> no preference for the return type of `1.2`
        -> `1.2` defaults to a DECIMAL
        -> overload resolution chooses the DECIMAL implementation
        -> the expression returns a DECIMAL
    SELECT sign(1.2)::float
        -> no preference for the return type of CAST(... as FLOAT)
        -> no preference for the return type of `sign`
        -> no preference for the return type of `1.2`
        -> `1.2` defaults to a DECIMAL
        -> overload resolution chooses the DECIMAL implementation
        -> CastExpr returns a FLOAT instead, which it will cast during evaluation
        -> the expression returns a FLOAT
    SELECT sign(1.2)::string
        -> no preference for the return type of CAST(... as STRING)
        -> no preference for the return type of `sign`
        -> no preference for the return type of `1.2`
        -> `1.2` defaults to a DECIMAL
        -> overload resolution chooses the DECIMAL implementation
        -> CastExpr returns a STRING instead, which it will cast (format) during evaluation
        -> the expression returns a STRING
    SELECT sign(1.2):::float
        -> no preference for the return type of `... ::: FLOAT`
        -> FLOAT preference passed for the return type of `sign`
        -> FLOAT used in overload resolution to determine float overload and
           to resolve the numeric constant as a float
        -> the type annotation correctly asserts that its sub-expression returns a FLOAT
        -> the type assertion itself returns a float, and does nothing during evaluation
        -> the expression returns a FLOAT
    SELECT sign(1.2):::string
        -> no preference for the return type of `... ::: STRING`
        -> STRING preference passed for the return type of `sign`
        -> STRING ignore in overload resolution because no overloads return a STRING,
           so DECIMAL implementation is defaulted to
        -> the type annotation throws an error when asserting that its sub-expression 
           returns a STRING, type checking fails
```

Note that in the above examples, adding a type annotation onto the top
of a SELECT clause has an almost identical effect on type checking as 
INSERTing into a column with the annotation type.

### Interaction with Placeholders

The initial Summer RFC defined a set of rules for determining the type of
placeholders based on type casts and type annotations. The proposed rules
are:

- if any given placeholder appears as immediate argument of an
  explicit annotation, then assign that type to the placeholder (and
  reject conflicting annotations after the 1st).

- otherwise (no direct annotations on a placeholder), if all
  occurrences of a placeholder appear as immediate argument
  to a cast expression then:

  - if all the cast(s) are homogeneous,
    then assign the placeholder the type indicated by the cast.

  - otherwise, assign the type "string" to the placeholder.

- for anything else, defer to type checking to determine the placeholder type.

Examples:

```sql
   SELECT $1:::float, $1::string
       -> $1 ::: float, execution will perform explicit cast float->string
   SELECT $1:::float, $1:::string
       -> error: conflicting types
   SELECT $1::float, $1::float
       -> $1 ::: float
   SELECT $1::float, $1::string
       -> $1 ::: string, execution will perform explicit cast $1 -> float
   SELECT $1:::float, $1
       -> $1 ::: float
   SELECT $1::float, $1
       -> nothing done during 1st pass, type checking will resolve
```

(Note that this rule does not interfere with the separate rule,
customary in SQL interpreters, that the client may choose to disregard
the stated type of a placeholder during execute and instead pass the
value as a string. The query executor must then convert the string to
the type for the placeholder that was determined during type checking.
For example if a client prepares `select $1:::int + 2` and passes "123" (a string),
the executor must convert "123" to 123 (an int) before running the query. The
annotation expression is a type assertion, not conversion, at run-time.)

This set of rules has already been partially implemented in the 
`parser.placeholderAnnotationVisitor`, which is used through
`parser.ProcessPlaceholderAnnotations`.

### Implementation Plan

A `TypeAnnotationExpr` or `AnnotationExpr` will be created in the `parser`
package. It's implementation will mirror that of the `ParenExpr`, except
in its `TypeCheck` method, where it will call `typeCheckAndRequire` on
its sub-expression with its annotation type. The expression could also
make a similar type assertion in its `Eval` method, but this shouldn't
be necessary.

The `ProcessPlaceholderAnnotations` will need to be adjusted to adopt the
rules listed above for interactions between casts and annotions. This is
already stubbed out.

# Drawbacks

Type annotations will be a Cockroach specific language extension to SQL.
In general, we have tried to avoid language extensions, as they limit
portability of SQL statements and create a higher learning curve for
moving to Cockroach. However, we concluded that this was ok, because 
these annotations are fully opt-in, and are not a requirement of the 
dialect.

# Alternatives

None. This is strictly an isolated extension.

# Unresolved questions

### EXPLAIN(TYPES)

An initial proof of concept implementation of type annotations made it
clear that `EXPLAIN(TYPES)` in conjunction with type annotations results
in a fairly verbose output. It might make sense to make type annotations 
"invisible" in the output of `EXPLAIN(TYPES)`, as they are annotations 
for the type system, as opposed to expressions which affect evaluation.
Furthermore, if the annotation passes type checking, its child will already
have the same type, so including it in the output is unnecessary.
