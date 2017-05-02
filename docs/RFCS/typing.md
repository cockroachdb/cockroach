- Feature Name: SQL typing
- Status: completed
- Authors: Andrei, knz, Nathan
- Start date: 2016-01-29
- RFC PR: [#4121](https://github.com/cockroachdb/cockroach/pull/4121),
          [#6189](https://github.com/cockroachdb/cockroach/pull/6189)
- Cockroach Issue: [#4024](https://github.com/cockroachdb/cockroach/issues/4024),
                   [#4026](https://github.com/cockroachdb/cockroach/issues/4026),
                   [#3633](https://github.com/cockroachdb/cockroach/issues/3633),
                   [#4073](https://github.com/cockroachdb/cockroach/issues/4073),
                   [#4088](https://github.com/cockroachdb/cockroach/issues/4088),
                   [#327](https://github.com/cockroachdb/cockroach/pull/327),
                   [#1795](https://github.com/cockroachdb/cockroach/issues/1795)

# Summary

This RFC proposes to revamp the SQL semantic analysis (what happens
after the parser and before the query is compiled or executed) with a
few goals in mind:

- address some limitations of the current type-checking implementation
- improve support for fancy (and not-so-fancy) uses of SQL and typing
  of placeholders for prepared queries
- improve the quality of the code internally
- pave the way for implementing sub-selects

To reach these goals the RFC proposes to:

- implement a new type system that is able to type more code than the
  one currently implemented.
- separate semantic analysis in separate phases after parsing
- unify all typed SQL statements (including `SELECT`/`UPDATE`/`INSERT`) as
  expressions (`SELECT` as an expression is already a prerequisite for
  sub-selects)
- structure typing as a visitor that annotates types as attributes in AST nodes
- extend `EXPLAIN` to pretty-print the inferred types. This will be approached
  by adding a new `EXPLAIN (TYPES)` command.

As with all in software engineering, more intelligence requires more
work, and has the potential to make software less predictable.
Among the spectrum of possible design points, this RFC settles
on a typing system we call *Summer*, which can be implemented
as a rule-based depth-first traversal of the query AST.

Alternate earlier proposals called *Rick* and *Morty* are also recorded for posterity.

# Motivation

## Overview

We need a better typing system.

Why: some things currently do not work that should really work. Some
other things behave incongruously and are difficult to understand, and
this runs counter to our design ideal to "make data easy".

How: let's look at a few examples, understand what goes Really Wrong,
propose some reasonable expected behavior(s), and see how to get there.

## Problems considered


This RFC considers specifically the following issues:

- overall architecture of semantic analysis in the SQL engine
- typing expressions involving only untyped literals
- typing expressions involving only untyped literals and placeholders
- overloaded function resolution in calls with untyped literals or
  placeholders as arguments

The following issues are related to typing but fall outside of the
scope of this RFC:

- "prepare" reports type X to client, client does not *know* X (and
  thus unable to send the proper format byte in subsequent "execute")

  This issue can be addressed by extending/completing the client
  Postgres driver.

- program/client sends a string literal in a position of another type,
  expects a coercion like in pg.

  For this issue one can argue the client is wrong; this issue may be
  addressed at a later stage if real-world use shows that demand for
  legacy compatibility here is real.

- prepare reports type "int" to client, client feeds "string" during
  execute

  Same as previous point.

## What typing is about


There are 4 different roles for typing in SQL:

1. **soundness analysis**, the most important is shared with other
   languages: check that the code is semantically sound -- that the
   operations given are actually computable. Typing soundness analysis
   tells you e.g. that ``3 + "foo"`` does not make sense and should be
   rejected.

2. **overload resolution** deciding what meaning to give
   to type-overloaded constructs in the language. For example some
   operators behave differently when given ``int`` or ``float``
   arguments (+, - etc). Additionally, there are overloaded functions
   (``length`` is different for ``string`` and ``bytes``) that behave
   differently depending on provided arguments. These are both
   features shared with other languages, when overloading exists.

3. **inferring implicit conversions**, ie. determine where to insert
   implicit casts in contexts with disjoint types, when your flavor of
   SQL supports this (this is like in a few other languages, like C).

4. **typing placeholders** inferring the type of
   placeholders (``$1``..., sometimes also noted ``?``), because the
   client needs to know this after a ``prepare`` and before an
   ``execute``.

What we see in CockroachDB at this time, as well as in some other SQL
products, is that SQL engines have issues in all 4 aspects.

There are often applicable reasons why this is so, for example
1) lack of specification of the SQL language itself 2) lack of
interest for this issue 3) organic growth of the machinery and 4)
general developer ignorance about typing.


## Examples that go wrong (arguably)

It's rather difficult to find examples where soundness goes wrong
because people tend to care about this most. That said, it is
reasonably easy to find example SQL code that seems to make logical
sense, but which engines reject as being unsound. For example:

```sql
    prepare a as select 3 + case (4) when 4 then $1 end
```

this fails in Postgres because ``$1`` is typed as ``string`` always and
you can't add string to int (this is a soundness error). What we'd
rather want is to infer ``$1`` either as ``int`` (or decimal) and let
the operation succeed, or fail with a type inference error ("can't
decide the type"). In CockroachDB this does not even compile, there is
no inference available within ``CASE``.

Next to this, there are a number of situations where existing engines
have chosen a behavior that makes the implementation of the engine
easy, but may irk / surprise the SQL user. And Surprise is Bad.

For example:


1. pessimistic typing for numeric literals.

   For example:

   ```sql

      create table t (x float);
      insert into t(x) values (1e10000 * 1e-9999);
   ```

   This fails on both Postgres and CockroachDB with a complaint that
   the numbers do not fit in either int or float, despite the fact the
   result would.

2. incorrect typing for literals.

   For example::

   ```sql
      select length(E'\\000a'::bytea || 'b'::text)
   ```

   Succeeds (wrongly!) in Postgres and reports 7 as result.  This
   should have failed with either "cannot concatenate bytes and string",
   or created a byte array of 3 bytes (\x00ab), or a string with a
   single character (b), or a 0-sized string.

3. engine throws hands up in the air and abandons something that could
   otherwise look perfectly fine::

   ```sql
       select floor($1 + $2)
   ```

   This fails in Postgres with "can't infer the types" whereas the
   context suggests that inferring ``decimal`` would be perfectly
   fine.

4. failure to use context information to infer types where this
   information is available.

   To simplify the explanation let's construct a simple example by
   hand. Consider a library containing the following functions::

        f(int) -> int
        f(float) -> float
        g(int) -> int

   Then consider the following statement::

   ```sql
        prepare a as select g(f($1))
   ```

   This fails with ambiguous/untypable $1, whereas one could argue (as
   is implemented in other languages) that ``g`` asking for ``int`` is
   sufficient to select the 1st overload for ``f`` and thus fully
   determine the type of $1.

5. Lack of clarity about the expected behavior of the division sign.

   Consider the following:

   ```sql
       create table w (x int, y float);
	   insert into w values (3/2, 3/2);
   ```

   In PostgreSQL this inserts (1, 1.0), with perhaps a surprise on the
   2nd value.  In CockroachDB this fails (arguably surprisingly) on
   the 1st expression (can't insert float into int), although the
   expression seems well-formed for the receiving column type.

6. Uncertainty on the typing of placeholders due to conflicting contexts:

   ```sql
      prepare a as select (3 + $1) + ($1 + 3.5)
   ```

   PostgreSQL resolves $1 as `decimal`. CockroachDB can't infer.
   Arguably both "int" and "float" may come to mind as well.



## Things that look wrong but really aren't


1. loss of equivalence between prepared and direct statements::

   ```sql
     prepare a as select ($1 + 2)
     execute a(1.5)

     -- reports 3 (in Postgres)
   ```

   The issue here is that the + operator is overloaded, and the
   engine performs typing on $1 only considering the 2nd operand to
   the +, and not the fact that $1 may have a richer type.

   One may argue that a typing algorithm that only performs "locally"
   is sufficient, and that this statement can be reliably understood
   to perform an integer operation in all cases, with a forced cast of
   the value filled in the placeholder. The problem with this argument
   is that this interpretation loses the equivalence between a direct
   statement and a prepared statement, that is, the substitution of:

   ```sql
      select 1.5 + 2
   ```

   is not equivalent to:

   ```sql
      prepare a as select $1 + 2; execute a(1.5)
   ```

   The real issue however is that SQL's typing is essentially
   monomorphic and that prepare statements are evaluated independently
   of subsequent queries: there is simply no SQL type that can be
   inferred for the placeholder in a way that provides sensible
   behavior for all subsequent queries. And introducing polymorphic
   types (or type families) just for this purpose doesn't seem
   sufficiently justified, since an easy workaround is available::

   ```sql
     prepare a as select $1::float + 2;
     execute a(1.5)
   ```

2. Casts as type hints.

   Postgres uses casts as a way to indicate type hints on
   placeholders. One could argue that this is not intuitive, because a
   user may legitimately want to use a value of a given type in a
   context where another type is needed, without restricting the type
   of the placeholder. For example:

   ```sql
     create table t (x int, s string);
     insert into t (x, s)  values ($1, "hello " + $1::string)
   ```

   Here intuition says we want this to infer "int" for $1, not get a
   type error due to conflicting types.

   However in any such case it is always possible to rewrite the
   query to both take advantage of type hints and also demand
   the required cast, for example:

   ```sql
     create table t (x int, s string);
     insert into t (x, s)  values ($1::int, "hello " + ($1::int)::string
   ```

   Therefore the use of casts as type hints should not be seem as a
   hurdle, and simply requires the documentation to properly mention
   to the user "if you intend to cast placeholders, explain the intended source
   type of your placeholder inside your cast first".

# Detailed design

Summary: Nathan spent some time trying to implement the first version of
this RFC. While doing so, he discovered it was more comfortable, performant,
and desirable to implement something in-between the current proposals
for Rick and Morty.

Since this new code is already largely written and seems to behave in
as expected in almost all scenarios (all tests pass, examples from the
previous RFC are handled at least as well as Morty), we figured it warrants
a specification *a posteriori*. This will allow us to consider the new system
orthogonally from the code, and directly compare it to Rick and Morty.

The resulting type system is called **Summer**, after the name of
Morty's sister in the show. Summer is more mature and more predictable
than Morty, and gives the same or more desirable results in almost all
scenarios while being more easy to understand
externally.

## Overview of Summer

- Summer is also based on a set of rules that can be applied using
  using a regular tree recursion.
- Summer does slightly more work than Morty (more conditions checked
  at every level) but is not iterative like Rick.
- Summer requires constant folding early in the type resolution.
- Summer does not require or allow implicit type conversions, as opposed
  to Morty. In a similar approach to Go, it uses untyped literals
  to cover 90% of the use cases for implicit type conversions, and deems
  that it's preferable to require explicit programmer clarification for
  the other 10%.
- Summer only uses exact arithmetic during initial constant folding,
  and performs all further operations using SQL types, whereas Morty
  sometimes uses exact arithmetic during evaluation.

Criticism of Morty where Summer is better: EXPLAIN on Morty will
basically say to the user "I don't really know what the type of these
expressions is" (eval-time type assertions with an exact argument).
Where Summer will always pick a type and be able to explain it.

## Proposed typing strategy

### High-level overview

To explain Summer to a newcomer it would be mostly correct to say
"Summer first determines the types of the operands of a complex
expression, then based on the operand types decides the type of the
complex expression", ie. the intuitive description of a bottom-up type
inference.

The reason why Summer is more complex than this in reality (and the principle
underlying its design) is threefold:

- Expressions containing placeholders often contain insufficient
  information to determine a proper type in a bottom-up fashion. For
  example in the expression `floor($1 * $2)` we cannot type the
  placeholders unless we take into account the accepted argument types
  of `floor`.

- SQL number literals are usually valid values in multiple types
  (`int`, `float`, `decimal`). Not only do users expect a minimum
  amount of automatic type coercion, so that expressions like `1.5 +
  123` are not rejected.  Also there is a conflict of interest between
  flexibility for the SQL user (which suggests picking the largest
  type) and performance (which suggests picking the smallest type).
  Summer does extra work to reach a balance in there. For example
  `greatest(1, 1.2)` will pick `float` whereas `greatest(1,
  1.2e10000)` will pick `decimal`.

- SQL has overloaded functions. If there are multiple candidates and
  the operand types do not match the candidates' expected types
  "exactly" Summer does extra work to find an acceptable candidate.

So another way to explain Summer that is somewhat less incorrect
than the naive explanation above would be:

1. the type of constant literals (numbers, strings, null) and
   placeholders are mostly determined by their parent expression
   depending on other rules (especially the expected type at that
   position), not themselves. For example Summer does not "know"
   (determines) the constant "123" to be an `int` until it looks at
   its parent in the syntax tree. For complex expressions involving
   number constants, this requires Summer to first perform constant
   folding so that the immediate parent of a constant, often an
   overloaded operator, has enough information from its other
   operand(s) to decide a type for the constant. This constant folding
   is performed using exact arithmetic.

2. for functions that require homogenous types (e.g. `GREATEST`, `CASE
   .. THEN` etc), the type expected by the context, if any, is used to
   restrict the operand types (rule 6.2) otherwise the first operand
   with a "possibly useful" type is used to restrict the type of the
   other operands (rules 6.3 and 6.4).

3. during overload resolution, the candidate list is first restricted
   to the candidates that are *compatible* with the arguments (rules
   7.1 to 7.3), then filtered down by compatibility between the
   candidate return types and the context (7.4), then by minimizing
   the amount of type conversions for literals (7.5), then by
   preferring homogenous argument lists (7.6).

### Language extension

In order to clarify the typing rules below and to exercise
the proposed system, we found it was useful to "force" certain
expressions to be of a certain type.

Unfortunately the SQL cast expression (`CAST(... AS ...)` or
`...::...`) is not appropriate for this, because although it
guarantees a type to the surrounding expression it does not constrain
its argument. For example `sign(1.2)::int` does not disambiguate which
overload of `sign` to use.

Therefore we propose the following SQL extension, which is not
required to implement the typing system but offers opportunities to
better exercise it in tests.  The explanatory examples below also use
this extension for explanatory purposes.

The extension is a new expression node "type annotation".

We also propose the following SQL syntax for this: "E : T".

For example: `1:int` of `1 : int`.

The meaning of this at a first order approximation is "interpret the
expression on the left using the type on the right".

This is different from casts, as explain below.

The need for this type of extension is also implicitly
present/expressed in the alternate proposals Rick and Morty.

### First pass: placeholder annotations

In the first pass we check the following:

- if any given placeholder appears as immediate argument of an
  explicit annotation, then assign that type to the placeholder (and
  reject conflicting annotations after the 1st).

- otherwise (no direct annotations on a placeholder), if all
  occurrences of a placeholder appear as immediate argument
  to a cast expression then:

  - if all the cast(s) are homogenous,
    then assign the placeholder the type indicated by the cast.

  - otherwise, assign the type "string" to the placeholder.

Examples:

```
   select $1:float, $1::string
       -> $1 : float, execution will perform explicit cast float->string
   select $1:float, $1:string
       -> error: conflicting types
   select $1::float, $1::float
       -> $1 : float
   select $1::float, $1::string
       -> $1 : string, execution will perform explicit cast $1 -> float
   select $1:float, $1
       -> $1 : float
   select $1::float, $1
       -> nothing done during 1st pass, typing below will resolve
```

(Note that this rule does not interfere with the separate rule,
customary in SQL interpreters, that the client may choose to disregard
the stated type of a placeholder during execute and instead pass the
value as a string. The query executor must then convert the string to
the type for the placeholder that was determined during type checking.
For example if a client prepares `select $1:int + 2` and passes "123" (a string),
the executor must convert "123" to 123 (an int) before running the query. The
annotation expression is a type assertion, not conversion, at run-time.)

### Second pass: constant folding

The second pass performs constant folding and annotates constant literals with
their possible types. Note that in practice, the first two passes could actually be implemented in a
single pass, but for the sake of understanding, it is easier to separate them
logically.

Constant expressions are folded using exact arithmetic. This is accomplished using a
depth-first, post-order traversal of the syntax tree. At the end of this phase,
the parents of constant values are either statements, or expression nodes where
one of the children is not a constant (either a column reference, a placeholder, or a
more complex non-constant expression).

Constant values are broken into two categories: **Numeric** and **String-like** constants,
which will be represented as the types `NumVal` and `StrVal` in the implemented typing system.
Numeric constants are stored internally as exact numeric values using the
[`go/constant`](https://golang.org/pkg/go/constant/) package. String-like constants are
stored internally using a `string` value.

After constant folding has occurred the remaining constants are represented as
literal constants in the syntax tree and annotated by an ordered list of SQL types that can
represent them with the least loss of information. We call this list the *resolvable type
ordered set*, or *resolvable type set* for short, and the head of this list the *natural type* of the
constant.

#### Numeric Constant Examples

| value                  | resolvable type set
|:----------------------:|:--------------------------
| 1                      | [int, float, decimal]
| 1.0                    | [float, int, decimal]
| 1.1                    | [float, decimal]
| null                   | [null, int, float, decimal, string, bytes, timestamp, ...]
| 123..overflowInt..4567 | [decimal, float]
| 12..overflowFloat..567 | [decimal]
| 1/2                    | [float, decimal]
| 1/3                    | [float, decimal] // perhaps future feature: [fractional, float, decimal]

Notice: we use the lowercase SQL types in the RFC, these are reified in the code using either zero
values of `Datum` (original implementation) or optionally the enum values of a new `Type` type.

#### String-like Constant Examples

| value                  | resolvable type set
|:----------------------:|:--------------------------
| 'abc'                  | [string, bytes]
| b'abc'                 | [bytes, string]
| b'a\00bc'              | [bytes]

These traits will be used later during the type resolution phase of constants.

### Third pass: expression typing, a recursive traversal of the syntax tree

The recursive typing function T takes two input parameters: the node to work
on, and a specification of **desired types**.

Desired types are simple for expressions: that's the desired type of the
result of evaluating the expression. For a sub-select or other syntax nodes
that return tables, the specification is a map from column name to requested
type for that column.

A desired type is merely a hint to the sub-expression
being type checked, it is up to the caller to assert a specific type
is returned from the expression typing and throw a type checking error
if necessary.

Desired type classes can be:

- fully unspecified (the top level specification is a wildcard)
- structurally specified (the top level specification is not a wildcard, but the desired
  type may contain wildcards)

We also say that a desired type is "fully specified" if it doesn't contain any wildcard.

We say that two desired type patterns are "equivalent" when they are structurally
equivalent given that a wildcard is equivalent to any type.

While a specified (fully or partially) desired type indicates a preference to
the sub-expression being type checked, an unspecified desired type indicates
no preference. This means that the sub-expression should type itself naturally
(see *natural type* discussion above). Generally speaking, wildcard types can be
thought of as accepting any type in their place. In certain cases, such as with
placeholders, a desired type must be fully-specified or an ambiguity error will
be thrown.

The alternative would be to propagate the desired type down as a constraint,
and fail as soon as this constraint is violated. However by doing so we would
dilute the clarity of the origin of the error. Consider for example `insert into (text_column) values (1+floor(1.5))`;
if we had desired types as constraints the error would be `1 is not a string` whereas by making the caller
that demands a type the checker, the error becomes `1+floor(1.5) is not a string`, which is arguably more desirable.
Meanwhile, the type checking of a node retains the option to accept
the type found for a sub-tree even if it's different from the desired type.

As an important optimization, we annotate the results of typing in the syntax
node. This way during normalization when the syntax structure is changed,
the new nodes created by normalization can reuse the types of their sub-trees
without having to recompute them (since normalization does not change
the type of the sub-trees). A new method on syntax node provides read access to
the type annotation.

The output of T is a new "typed expression" which is capable of returning
(without checking again) the type it will return when evaluated. T also stores
the inferred type in the input node itself (at every level) before returning.
In effect, this means that type checking will transform an untyped expression
tree where each node is unable to be properly introspect about its own return
type into a typed tree which can provide its inferred result type, and as such
can be evaluated later.

#### Implementation Notes

_In an effort to make this distinction clearer in code, a `TypedExpr` interface
will be created, which is a superset of the `Expr` interface, but also has the
ability to return its annotation and evaluate itself. This means that the `Eval`
method in `Expr` will be moved to `TypedExpr`, and that the `TypeCheck` method on
`Expr`s will return a `TypedExpr`._

The function then works as follows:

1. if the node is a constant literal: if the desired type is within the constant's
   **resolvable type set**, convert the literal to the desired type. Otherwise, resolve
   the literal as its **natural type**. Note that only fully-specified types will ever
   be in a constant's **resolvable type set**.

2. if the node is a column reference or a datum: use the type determined by the node regardless
   of the desired type.

3. if the node is a placeholder: if the desired type is not fully-specified, report an error.
   if there is a fully-specified desired type, and the placeholder was not yet assigned a type,
   assign the desired type to the placeholder. If the placeholder was already assigned a
   different type, report an error.

4. if the node is NULL: if there is a fully-specified desired type, annotate the node with
   the equivalent type and return that, otherwise return the NULL type as the expression's resolved
   type.

5. if the node is a simple statement (or sub-select, not CASE!). Propagate the desired
   types down, then look at what comes up when the recursion returns, then check the
   inferred type are compatible with the statement semantics.

6. for statements or variadic function calls with a homogeneity requirement, we use the rules in the section
   [below](#required-homogeneity) for typing.

7. if the node is a function call not otherwise handled in step #6 [incl a binary or unary operation, or a comparison
   operation], perform overload resolution on the set of possible overloaded
   functions that could be used. See [below](#overload-resolution) for how
   this is performed.

8. if the node is a type annotation, then the desired type provided from the parent is ignored and the annotated
   type required instead (sent down as desired type, checked upon type resolution, if they don't match an error is reported).
   The annotated type is resolved.

### Overload resolution

In the case of a function call (and all call-like expressions) there
are a set of overloads that must be chosen from to resolve the correct operation implementation
for to dispatch to during evaluation. This resolution can be broken down into a series of filtering
steps, whereby candidates which do not pass a filter are eliminated from the resolution process.

The resolution is based on an initial classification of all the argument expressions to the call into
3 categories (implemented as 3 vectors of (position, expression) in the implementation):

- constant numeric literals
- unresolved arguments: placeholder nodes that do not have an assigned type yet
- "pre-typable nodes" (which happens to be either unambiguously resolvable expressions or previously resolved placeholders or constant
string literals)

The **first three** steps below are run unconditionally. After the 3rd step and after each
subsequent step, we check the remaining overload set:

- If there are no candidates left, type checking fails ("no matching overload").
- if there is only one candidate left, this is used as the implementation function to use for the call, any
  yet untyped placeholder or constant literal is typed recursively using the type defined by its argument position as desired type,
  (it is possible to prove, and we could assert here, that the inferred type here is always
  equivalent to the desired type) then subsequent steps are skipped.
- if there is more than one candidate left, the next filter is applied and the resolution
  continues.


1. (7.1) candidates are filtered based on the number of arguments

2. (7.2) the pre-typable sub-nodes (and only those) are typed, starting with an unspecified desired type.
   At every sub-node, the candidate list is filtered using the types found so far. If at
   any point there is only one candidate remaining, further pre-typable sub-nodes are typed using
   the remaining candidate's argument type at that position as desired type.

   Possible extension: if at any point the remaining candidates all accept the same type at the
   current argument position, that type is also used as desired type.

   Then the overload candidates are filtered based on the resulting types. If any argument of the call
   receives type null, then it is not used for filtering.

   For example: `select mod(extract(seconds from now()), $1*20)`. There
   are 3 candidates for `mod`, on `int`, `float` and `decimal`. The
   first argument `extract` is typed with an unspecified desired type and
   resolves to `int`. This selects the candidate `mod(int, int)`. From then on only one candidate
   remains so `$1*20` gets typed using desired type `int` and `$1` gets typed as `int`.

3. (7.3) candidates are filtered based on the resolvable type set types of constant number literals.
   Remember at this point all constant literals already have a resolvable type set since constant folding.

   The filtering is done left to right, eliminating at each argument all candidates that do not accept
   one of the types in the resolvable set at that position.

   Example: `select sign(1.2)`. `sign` has 3 candidates for `int`, `float` and `decimal`. Step 7.3 eliminates
   the candidate for `int`.

   After this point,
   the number of candidates left will be checked now and after each following step.

4. (7.4) candidates are filtered based on the desired return type, if one is provided

   Example: `insert into (str_col) values (left($1, 1))`
   With only rules 7.2 and 7.3 above we still have 2 candidates: `left(string, int)` and `left(bytes, int)`.
   With rule 7.4 `left(string, int)` is selected.

5. (7.5) If there are constant number literals in the argument list, then try to filter the candidate list
   down to 1 candidate using the natural type of the constants. If that fails (either 0 candidates left or >1), try again
   this time trying to find a candidate that accepts the "best" mutual type in the resolvable type set of all constants.
   (in the order defined in the resolvable type set)

   Example: `select sign(1.2)`
   With only rules 7.2 to 7.4 above we still have 2 candidates:
   With candidates `sign(float)` and `sign(decimal)`.
   Rule 7.5 with the natural type selects `div(float)`.

   Example: `select div(1e10000,2.5)`
   With only rules 7.2 to 7.4 above we still have 2 candidates:
   `div(float,float)` and `div(decimal,decimal)` however the natural types are `decimal` and `float`, respectively.
   The 2nd part of rule 7.5 picks `div(decimal,decimal)`.

6. (7.6) *for the final step, we look to prefer homogeneous argument types across candidates.
   For instance, overloads
   for `int + int` and `int + date` exist, so without preferring homogeneous overloads, `1 + $1` would
   be resolved as ambiguous. Therefore, we check if all previously resolved types are the same, and if
   so, follow the filtering step.*

   if there is at least one argument with a resolved type, and all resolved types for arguments so far are homogeneous in type
   and all remaining constants have this type in their resolvable type set, and there is at least one candidate.
   that accepts this type in the yet untyped positions,
   choose that candidate.

   Example: `select div(1, $1)` still has candidates for `int`, `float` and `decimal`.

Another approach would be to go through each overload and attempt to type check each
argument expression with the parameter's type. If any of these expressions type checked to a
different type then we could discard the overload. This would avoid some of the issues noted in
step 2, but would create a few other issues
- it would ignore constant's **natural** types. This could be special cased, but only one level deep
- it could be expensive if the function was high up in an expression tree
- it would ignore preferred homogeneity. Again though, this could be special cased
Because of these issues, this approach is not being considered

### Required homogeneity

There are a number of cases where it is required that the type of all expressions are the
same. For example: COALESCE, CASE (conditions and values), IF, NULLIF, RANGE, CONCAT, LEAST/GREATEST (MIN/MAX variadic)....

These situations may or may not also desire a given type for all subexpressions. Two
examples of this type of situation are in CASE statements (both for the condition set and the
value set) and in COALESCE statements. Because this is a common need for a number of statement
types, the typing resolution of this situation should be specified. Here we present a list of
rules to be applied to a given list of untyped expressions and a desired type.

1. (6.1) as we did with overload resolution, split the provided expressions into three groups:
  - pre-typable nodes unambiguously resolvable expressions, previously resolved arguments, and constant string literals
  - constant numeric literals
  - unresolved placeholders

2. (6.2) if there is a specified (partially or fully) desired type, type all the sub-nodes using
   this type as desired type. If any of the sub-nodes resolves to a different type, report an
   error (expecting X, got Y).

3. (6.3) otherwise (wildcard desired type), if there is any pre-typable node, then
   type this node with an unspecified desired type.
   Call the resulting type T.
   Then for all remaining sub-nodes, type it desiring T. If the resulting type is different from T, report an error.
   The result of typing is T.

4. (6.4) (wildcard desired type, no pre-typable node, all remaining nodes are either constant number literals or untyped placeholders)

   If there is at least one constant literal, then pick the best mutual type of all constant literals, if any, call that T,
   type all sub-nodes using T as desired type, and return T as resolved type.

5. (6.5) Fail with ambiguous typing.

## Examples with Summer

### Example 1

```sql
    prepare a as select 3 + case (4) when 4 then $1 end
  Tree:
       select
         |
         +
       /   \
      3    case
        /  |  \
       4   4   $1
```

Constant folding happens, nothing changes.

Typing of the select begins. Since this is not a sub-select there is wildcard desired type.
Typing of "+" begins. Again wildcard desired type.

Rule 7.1 then 7.2 applies.

Typing of "case" begins without a specified desired type.

Then "case" recursively types its condition variable with a wildcard desired type.
Typing of "4" begins. unspecified desired type here, resolves to int as natural type, [int, float, dec] as resolvable type set.
Typing of "case" continues. Now it knows the condition is an "int" it will demand "int" for the WHEN branches.
Typing of "4" (the 2nd one) begins. Type "int" is desired so the 2nd "4" is typed to that.
Typing of "case" continues. Here rule 6.4 applies, and a failure occurs.

```sql
 prepare a as select 3 + case (4) when 4 then $1 else 42 end
```

Typing of the select begins. Since this is not a sub-select there is wildcard desired type.
Typing of "+" begins. Again wildcard desired type.

Rule 7.1 then 7.2 applies.

Typing of "case" begins without a specified desired type.

Then "case" recursively types its condition variable with a wildcard desired type.
Typing of "4" begins. wildcard desired type, resolves to int as natural type, [int, float, dec] as resolvable type set.
Typing of "case" continues. Now it knows the condition is an "int" it will demand "int" for the WHEN branches.
Typing of "4" (the 2nd one) begins. Type "int" is desired so the 2nd "4" is typed to that.

Here rule 6.4 applies. "42" decides int, so $1 gets assigned "int" and
case resolves as "int".

Then typing of "+" resumes.

Based on the resolved type for case, "+" reduces the overload set to
(int, int), (date, int), (timestamp, int).

Rule 7.3 applies. This eliminates the candidates that take non-number as 1st argument. Only (int, int) remains.
This decides the overload, "$1" gets typed as "int", and the
typing completes for "+" with "int".
Typing completes.


### Example 2

```sql
    create table t (x float);
  insert into t(x) values (1e10000 * 1e-9999);
```

First pass: constant folding using exact arithmetic. The expression 1e10000 * 1e-9999 gets simplified to 10.

Typing insert. The target of the insert is looked at first. This determines desired type "float" for the 1st column in the values clause.
Typing of the values clause begins, with desired type "float".
Typing of "10" begins with desired type "float". "10" gets converted to float.
Typing of insert ends. All is well. Result:

```
       insert
       |
     10:float
```


### Example 3

```
   select floor($1 + $2)
```

Assuming `floor` is only defined for floats.
Typing of "floor" begins with an unspecified desired type.

Rule 7.2 applies.
There is only one candidate, so there is a desired type for the remaining arguments (here the only one of them) based on the arguments taken by floor.

Typing of "+" begins with desired type "float".

Rule 7.2 applies: nothing to do.
Rule 7.3 applies: nothing to do.
Rule 7.4 applies: +(float, float) is selected.
Then $1 and $2 are assigned the types desired by the remaining candidate.

Typing of "floor" resumes, finds an "float" argument.
rules 7.2 completes with 1 candidate, and
typing of "floor" completes with type "float".

Typing completes.

```
   select
    |
   floor:float
  /          \
$1:float    $2:float
```


### Example 4

```sql
   select ($1+$1)+current_date()
     select
         +(a)
     +(b)     current_date()
   $1 $1
```

Typing of "+(a)" begins with a wildcard desired type.
Rule 7.2 applies.
All candidates for "+" take different types,
so we don't find any desired type

Typing of "+(b)" begins without a specified desired type.
Rules 7.1 to 7.6 fail to reduce the overload set, so typing fails with ambiguous types.

Possible fix:
- annotate during constant folding all the nodes known to not contain placeholders or constants underneath
- in rule 7.2 order the typing of pre-typable sub-nodes by starting
with those nodes.


### Example 5

Consider a library containing the following functions::

   f(int) -> int
   f(float) -> float
   g(int) -> int

Then consider the following statement::

```sql
  prepare a as select g(f($1))
```

Typing starts for "select".
Typing starts for the call to "g" without a specified desired type.
Rule 7.2 applies. Only 1 candidate so the sub-nodes are typed
with its argument type as desired type.

Typing starts for the call to "f" with desired type "int".
Rule 7.4 applies, select only 1 candidate.
then typing of "f" completes, "$1" gets assigned "int",
"f" resolves to "int".

"g" sees only 1 candidate, resolves to that candidate's
return type "int"
Typing completes.

```sql
  INSERT INTO t(int_col) VALUES (4.5)
```

Insert demands "int", "4.5" has natural type float and doesn't have
"int" in its resolvable type set.
Typing fails (like in Rick and Morty).

```sql
    INSERT INTO t(int_col) VALUES ($1 + 1)
```

Insert demands "int",
Typing of "+" begins with desired type "int"
Rule 7.4 applies, choses +(int, int).
Only 1 candidate, $1 and 1 gets assigned 'int"
Typing completes.

```sql
    insert into (int_col) values ($1 - $2)
```

  do not forget:
  -(int, int) -> int
  -(date, date) -> int

Ambiguous on overload resolution of "-"


### Example 6

```sql
  insert into (str_col) values (coalesce(1, "foo"))
  -- must say "1" is not string
  select coalesce(1, "foo")
  -- must say "foo" is not int
```

(to check in testing: Rules 6.1-6.5 do this)

```sql
    SELECT ($1 + 2) + ($1 + 2.5)
```

($1 + 2) types as int, $1 gets assigned int
then ($2 + 2.5) doesn't type.

(Morty would have done $1 = exact)


### Example 7

```sql
      create table t (x float);
      insert into t(x) values (3 / 2)
```
Constant folding reduces 3/2 into 1.5.

Typing "1.5" stars with desired type "float", succeeds, 1.5 gets inserted.

```sql
      create table u (x int);
      insert into u(x) values (((9 / 3) * (1 / 3))::int)
```
Constant folding folds this down to ... values("1") with "1"
annotated with natural type "int" and resolvable type set [int].

Then typing succeeds.


### Example 8

```sql
     create table t (x int, s text);
     insert into t (x, s)  values ($1, "hello " + $1::text)
```

First "$1" gets typed with desired type "int", gets assigned "int".
Then "+"  is typed.
Rule 7.2 applies.
The cast "cast ($1 as text)" is typed with a wildcard desired type.
This succeeds, leaves the $1 unchanged (it is agnostic of its argument)
and resolves to type "text".
"+" resolves to 1 candidate, is typed as "string"
Typing ends. $1 is int.
(better than Morty!)


### Example 9

```sql
    select $1::int
```

First pass annotates $1 as int (all occurrences are argument of
cast). Typing completes with int.


### Example 10

```sql
    f:int,int->int
    f:float,float->int
    PREPARE a AS SELECT f($1, $2), $2::float
```

Typing of "f" starts,
Multiple candidate remain after overload resolution.
Typing fails with ambiguous types.


### Example 11

#### Part a

```sql
    f:int,int->int
    f:float,float->int
    PREPARE a AS SELECT f($1, $2), $2:float
```

$2 is assigned to "float" during the first phase.
then typing of "f" starts,
the argument have reduced the candidate set to just one.
Typing completes
$1 is assigned "float"

#### Part b

```sql
    PREPARE a AS SELECT ($1 + 4) + $1::int
```

Typin of top "+" starts.
Typing of inner "+" starts.
Candidates filtered to +(date,int) and +(int, int).
Rule 7.6 applies, $1 gets assigned "int".
"+" resolves 1 candidate.
Top level plus is +(int,int)->int
Typing end with int.

#### Part c

```sql
    PREPARE a AS SELECT ($1 + 4) + $1:int
```

"$1" gets assigned "int" during the first phase.
"+" resolves 1 candidate
[...]
Typing ends.

#### Part d

```sql
    PREPARE a AS SELECT ($2 - $2) * $1:int, $2:int
```

$2 is assigned int during the first pass.
Typing of "*" begins.
It sees that its 2nd argument already has type.
So the candidate list is reduced to *(int,int)
so Typing of "-" starts with desired type "int".
There are 2 candidates:
-(int, int) -> int
-(date, date) -> int
$2 already has type int, so one candidate remains.
[...]
Typing ends successfully.


### Example 12

```sql
    f : int -> int
    INSERT INTO t (int_a, int_b) VALUES (f($1), $1 - $2)
    -- succeeds (f types $1::int first, then $2 gets typed int),
    -- however:
    INSERT INTO t (int_b, int_a) VALUES ($1 - $2, f($1))
    -- fails with ambiguous typing for $1-$2, f not visited yet.
```

Same as Morty.

```sql
   SELECT CASE a_int
      WHEN 1 THEN 'one'
      WHEN 2 THEN
         CASE language_str
           WHEN 'en' THEN $1
         END
   END
```

Rule 6.3 applies for the outer case, "one" gets typed as "string"
Then "string" is desired for the inner case.
Then typing of "$1" assigns "string" (desired).
Then typing completes.


### Example 13

```sql
  select max($1, $1):int
```

Annotation demands "int" so rule 6 demands "int" from max, resolves "int" for $1 and max.


### Example 14

```sql
  select array_length(ARRAY[1, 2, 3])
```

Typing starts for "select".
Typing starts for the call to "array_length" without a specified desired type.
Rule 7.2 applies. Only 1 candidate is available so the sub-nodes are typed with
its argument type as a desired type, which is "array<*>".

Typing starts for the ARRAY constructor with desired type "array<*>".
The ARRAY expression checks that the desired type is present and has a
base type of "array". Because it does, it unwraps the desired type, pulls
out the parameterized type "*", and passes this as the desired type when
requiring homogeneous types for all elements. 

Typing starts for the array's expressions. These elements, in the presence
of an unspecified desired type, naturally type themselves as "int"s using
rule 6.4.

The ARRAY expression types itself as "array\<int\>".

The overload resolution for "array_length" finds that this resolved type is
equivalent to its single candidate's parameter (`array<*> ≡ array<int>`), so
it picks that candidate and resolves to that candidate's return type of "int".

Typing completes.


# Alternatives

## Overview of Morty

- Morty is a simple set of rules; they're applied locally (single
  depth-first, post-order traversal) to AST nodes for making typing
  decisions.

  One thing that conveniently makes a bunch of simple examples just
  work is that we keep numerical constants untyped as much as possible
  and introduce the one and only implicit cast from an untyped number
  constant to any other numeric type;

- Morty has only two implicit conversions, one for arithmetic on
  untyped constants and placeholders, and one for string literals.

- Morty does not require but can benefit from constant folding.

We use the following notations below:


    E :: T  => the regular SQL cast, equivalent to `CAST(E as T)`
    E [T]   => an AST node representing `E`
               with an annotation that indicates it has type T

## AST changes and new types

These are common to both Rick and Morty.

`SELECT`, `INSERT` and `UPDATE` should really be **EXPR** s.

The type of a `SELECT` expression should be an **aggregate**.

Table names should type as the **aggregate type** derived from their
schema.

An insert/update should really be seen as an expression like
a **function call** where the type of the arguments
is determined by the column names targeted by the insert.


## Proposed typing strategy for Morty

First pass: populating initial types for literals and placeholders.

- for each numeric literal, annotate with an internal type
  `exact`. Just like for Rick, we can do arithmetic in this type for
  constant folding.

- for each placeholder, process immediate casts if any by annotating
  the placeholder by the type indicated by the cast *when there is no
  other type discovered earlier for this placeholder* during this
  phase. If the same placeholder is encountered a 2nd time with a
  conflicting cast, report a typing error ("conflicting types for $n
  ...")

Second pass (optional, not part of type checking): constant folding.

Third pass, type inference and soundness analysis:

1. Overload resolution is done using only already typed arguments. This
   includes non-placeholder arguments, and placeholders with a type discovered
   earlier (either from the first pass, or earlier in this pass in traversal order).
2. If, during overload resolution, an expression E of type `exact` is
   found at some argument position and no candidate accepts `exact` at
   that position, and *also* there is only one candidate that accepts
   a numeric type T at that position, then the expression E is
   automatically substituted by `TYPEASSERT_NUMERIC(E,T)[T]` and
   typing continues assuming `E[T]` (see rule 11 below for a definition of `TYPEASSERT_NUMERIC`).
3. If, during overload resolution, a *literal* `string` E is
   found at some argument position and no candidate accepts `string`
   at that position, and *also* there is only one candidate left based
   on other arguments that accept type T at that position *which does
   not have a native literal syntax*, then the expression E is
   automatically substituted by `TYPEASSERT_STRING(E,T)[T]` and typing
   continues assuming E[T]. See rule 12 below.
4. If no candidate overload can be found after steps #2 and #3, typing
   fails with "no known function with these argument types".
5. If an overload has only one candidate based on rules #2 and #3,
   then any placeholder it has as immediate arguments that are not yet
   typed receive the type indicated by their argument position.
6. If overload resolution finds more than 1 candidate, typing fails
   with "ambiguous overload".
7. `INSERT`s and `UPDATE`s come with the same inference rules
   as function calls.
8. If no type can be inferred for a placeholder (e.g. it's used only
   in overloaded function calls with multiple remaining candidates or
   only comes in contact with other untyped placeholders), then again
   fail with "ambiguous typing for the placeholder".
9. literal NULL is typed "unknown" unless there's an immediate cast just
   afterwards, and the _type_ "unknown" propagates up expressions until
   either the top level (that's an error) or a function that explicitly
   takes unknown as input type to do something with it (e.g. is_null,
   comparison, or INSERT with nullable columns);
10. "then" clauses (And the entire surrounding case expression) get
    typed by first attempting to type all the expressions after
    "then"; then once this done, take the 1st expression that has a
    type (if any) and type check the other expressions against that
    type (possibly assigning types to untyped placeholders/exact
    expressions in that process, as per rule 2/3). If there are "then"
    clauses with no types after this, a typing error is reported.
11. `TYPEASSERT_NUMERIC(<expression>, <type>)` accepts an expression of type
    `exact` as first argument and a numeric type name as 2nd
    argument. If at run-time the value of the expression fits into the
    specified type (at least preserving the amplitude for float, and
    without any information loss for integer and decimal), the value
    of the expression is returned, casted to the type. Otherwise, a
    SQL error is generated.
12. `TYPEASSERT_STRING(<expression>, <type>)` accepts an expression of
    type `string` as first argument and a type with a possible
    conversion from string as 2nd argument. If at run-time the
    converted value of the expression fits into the specified type
    (the format is correct, and the conversion is at least preserving
    the amplitude for float, and without any information loss for
    integer and decimal), the value of the expression is returned,
    converted to the type. Otherwise, a SQL error is generated.

You can see that Morty is simpler than Rick: there's no sets of type candidates for any expressions.
Other differences is that Morty relies on the introduction of an
guarded implicit cast. This is because of the following cases:

```sql
    (1)   INSERT INTO t(int_col) VALUES (4.5)
```

This is a type error in Rick. Without Morty's rule 2 and a "blind"
implicit cast, this would insert `4` which would be undesirable. With
rule 2, the semantics become:

```sql
    (1)   INSERT INTO t(int_col) VALUES (TYPEASSERT_NUMERIC(4.5, int)[int])
```

And this would fail, as desired.

`Exact` is obviously not supported by the pgwire protocol, or by
clients, so we'd report `numeric` when `exact` has been inferred for a
placeholder.

Similarly, and in a fashion compatible with many SQL engines, string
values are autocasted when there is no ambiguity (rule 3); for
example:

```sql
    (1b)   INSERT INTO t(timestamp_col) VALUES ('2012-02-01 01:02:03')

	Gets replaced by:

    (1b)   INSERT INTO t(timestamp_col) VALUES (TYPEASSERT_STRING('2012-02-01 01:02:03', timestamp)[timestamp])

	which succeeds, and

    (1c)   INSERT INTO t(timestamp_col) VALUES ('4.5')

	gets replaced by:

    (1c)   INSERT INTO t(timestamp_col) VALUES (TYPEASSERT_STRING('4.5', timestamp)[timestamp])

	which fails at run-time.
```

Morty's rule 3 is proposed for convenience, observing that
once the SQL implementation starts to provide custom / extended types,
clients may not support a native wire representation for them. It can
be observed in many SQL implementations that clients will pass values
of "exotic" types (interval, timestamps, ranges, etc) as strings,
expecting the Right Thing to happen automatically. Rule 3 is
our proposal to go in this direction.

Rule 3 is restricted to literals however, because we probably don't
want to support things like `insert into x(timestamp_column) values
(substring(...) || 'foo')` without an explicit cast to make the
intention clear.


Regarding typing of placeholders:

```sql
    (2)   INSERT INTO t(int_col) VALUES ($1)
    (3)   INSERT INTO t(int_col) VALUES ($1 + 1)
```

In `(2)`, `$1` is inferred to be `int`.  Passing the value `"4.5"` for
`$1` in `(2)` would be a type error during execute.

In `(3)`, `$1` is inferred to be `exact` and reported as `numeric`; the
client can then send numbers as either int, floats or decimal down the
wire during execute. (We propose to change the parser to accept any
client-provided numeric type for a placeholder when the AST expects
exact.)

However meanwhile because the expression `$1 + 1` is also
`exact`, the semantics are automatically changed to become:

```sql
    (3)   INSERT INTO t(int_col) VALUES (TYPEASSERT($1 + 1, int)[int])
```

This way the statement only effectively succeeds when the client
passes integers for the placeholder.

Although another type system could have chosen to infer `int` for `$1`
based on the appearance of the constant 1 in the expression, the true
strength of Morty comes with statements of the following form:

```sql
    (4)   INSERT INTO t(int_col) VALUES ($1 + 1.5)
```

Here `$1` is typed `exact`, clients see `numeric`, and thanks to the
type assertion, using `$1 = 3.5` for example will actually succeed
because the result fits into an int.

Typing of constants as `exact` seems to come in handy in some
situations that Rick didn't handle very well:

```sql
    SELECT ($1 + 2) + ($1 + 2.5)
```

Here Rick would throw a type error for `$1`, whereas Morty infers `exact`.

## Examples of Morty's behavior

```sql
      create table t (x float);
      insert into t(x) values (3 / 2)
```

`3/2` gets typed as `3::exact / 2::exact`, division gets exact 1.5,
then exact gets autocasted to float for insert (because float
preserves the amplitude of 1.5).

```sql
      create table u (x int);
      insert into u(x) values (((9 / 3) * (1 / 3))::int)
```

`(9/3)*(1/3)` gets typed and computes down to exact 1, then exact
gets casted to int as requested.

Note that in this specific case the cast is not required any more
because the implicit conversion from exact to int would take place
anyways.

```sql
      create table t (x float);
      insert into t(x) values (1e10000 * 1e-9999);
```

Numbers gets typed and casted as exact, multiplication
evaluates to exact 10, this gets autocasted back to float for insert.

```sql
      select length(E'\\000a'::bytea || 'b'::text)
```

Type error, concat only works for homogeneous types.

```sql
      select floor($1 + $2)
```

Type error, ambiguous resolve for `+`.
This can be fixed by `floor($1::float + $2)`, then there's only
one type remaining for `$2` and all is well.

```sql
      f(int) -> int
      f(float) -> float
      g(int) -> int
      prepare a as select g(f($1))
```

Ambiguous, tough luck. Try with `g(f($1::int))` then all is well.

```sql
      prepare a as select ($1 + 2)
      execute a(1.5)
```

`2` typed as exact, so `$1` too. `numeric` reported to client, then
`a(1.5)` sends `1.5` down the wire, all is well.

```sql
     create table t (x int, s text);
     insert into t (x, s)  values ($1, "hello " + $1::text)
```

`$1` typed during first phase by collecting the hint `::text`:

```sql
     insert into t (x, s) values ($1[text], "hello "[text] + $1::text)
```

Then during type checking, text is found where int is expected in the
1st position of `values`, and typing fails. The user can force the
typing for `int` by using explicit hints:

```sql
     create table t (x int, s text);
     insert into t (x, s)  values ($1::int, "hello " + $1::int::text)
```

Regarding case statements:

```sql
     prepare a as select 3 + case (4) when 4 then $1 end
```

Because there is only one `then` clause without a type, typing fails.
The user can fix by suggesting a type hint. However, with:


```sql
     prepare a as select 3 + case (4) when 4 then $1 else 42 end
```

`42` gets typed as `exact`, so `exact` is assumed for the other `then` branches
including `$1` which gets typed as `exact` too.

Indirect overload resolution:

```sql
    f:int,int->int
    f:float,float->int
    PREPARE a AS SELECT f($1, $2), $2::float
```

Morty sees `$2::float` first, thus types `$2` as float then `$1` as
float too by rule 5. Likewise:


```sql
    PREPARE a AS SELECT $1 + 4 + $1::int
```

Morty sees `$1::int` first, then autocasts 4 to `int` and the
operation is performed on int arguments.

## Alternatives around Morty

Morty is an asymmetric algorithm: how much an how well
the type of a placeholder is typed depends on the order of syntax
elements. HFor example:

```sql
    f : int -> int
    INSERT INTO t (a, b) VALUES (f($1), $1 + $2)
    -- succeeds (f types $1::int first, then $2 gets typed int),
    -- however:
    INSERT INTO t (b, a) VALUES ($1 + $2, f($1))
    -- fails with ambiguous typing for $1+$2, f not visited yet.
```

Of course we could explain this in documentation and suggest the use
of explicit casts in ambiguous contexts. However, if this property is
deemed too uncomfortable to expose to users, we could make the
algorithm iterative and repeat applying Morty's rule 5 to all
expressions as long as it manages to type new placeholders. This way:

```sql
    INSERT INTO t (b, a) VALUES ($1 + $2, f($1))
    --                              ^  fail, but continue
	--  						 $1 + $2, f($1)      continue
	--  							        ^
	--  						  ....  , f($1:int)  now retry
	--
    --                           $1::int + $2, ...
	--  						         ^ aha! new information
	--  						 $1::int + $2::int, f($1::int)

    -- all is well!
```

## Implementation notes

(these may evolve as the RFC gets implemented. This section
is likely to become outdated a few months after the RFC gets accepted.)

1. All AST nodes (produced by the parser) implement `Expr`.

   `INSERT`, `SELECT`, `UPDATE` nodes become visitable by
   visitors. This will unify the way we do processing on the AST.
2. The ``TypeCheck`` method from ``Expr`` becomes a separate
   visitor. Expr gets a ``type`` field populated by this visitor. This
   will make it clear when type inference and type checking have run
   (and that they run only once). This is in contrast with
   ``TypeCheck`` being called at random times by random code.
3. During typing there will be a need for a data structure to collect
   the type candidate sets per AST node (``Expr``) and
   placeholder. This should be done using a separate map, where either
   AST nodes or placeholder names are keys.
4. Semantic analysis will be done as a new step doing constant
   folding, type inference, type checking.

The semantic analysis will thus look like::

    ```go
    type placeholderTypes = map[ValArg]Type

    // Mutates the tree and populates .type
    func semanticAnalysis(root Expr) (assignments placeholderTypes,  error) {
      var untypedFolder UntypedConstantFoldingVisitor = UntypedConstantFoldingVisitor{}
      untypedFolder.Visit(root)

      // Type checking and type inference combined.
      var typeChecker TypeCheckVisitor = TypeCheckVisitor{}
      if err := typeChecker.Visit(root); err != nil {
        report ambiguity or typing error
      }
      assignments = typeChecker.GetPlaceholderTypes()

      // Optional in Morty
      var constantFolder ConstantFoldingVisitor = ConstantFoldingVisitor{}
      constantFolder.Visit(root)
    }
    ```

When sending values over pgwire during bind, the client sends the
arguments positionally. For each argument, it specifies a "format"
(different that a type). The format can be binary or text, and
specifies the encoding of that argument. Every type has a text
encoding, only some also have binary encodings. The client does not
send an oID back, or anything to identify the type. So the server just
needs to parse whatever it got assuming the type it previously
inferred.

The issue of parsing these arguments is not really a typing
issue. Formally Morty (and Rick, its alternative) just assumes that it gets whatever
type it asked for. Whomever implements the parsing of these arguments
(our pgwire implementation) uses the same code/principles as a
`TYPEASSERT_STRING` (but this has nothing to do with the AST of our
query (which ideally should have been already saved from the prepare
phase)).


## Overview of Rick

The precursor of, and an alternative to, Morty was called *Rick*.  We
present it here to keep historical records and possibly serve as other
point of reference if the topic is revisited in the future.

- Rick is an iterative (multiple traversals) algorithm that tries
  harder to find a type for placeholders that accommodates all their
  occurrences;

- Rick allows from flexible implicit conversions;

- Rick really can't work without constant folding to simplify complex
  expressions involving only constants;

- Rick tries to "optimize" the type given to a literal constant
  depending on context;


## Proposed typing strategy for Rick

We use the following notations below::

    E :: T  => the regular SQL cast, equivalent to `CAST(E as T)`
    E [T]   => an AST node representing `E`
               with an annotation that indicates it has type T

For conciseness, we also introduce the notation E[\*N] to mean that
`E` has an unknown number type (`int`, `float` or `decimal`).

We assume that an initial/earlier phase has performed the reduction of
casted placeholders (but only placeholders!), that is, folding:

     $1::T      => $1[T]
     x::T       => x :: T  (for any x that is not a placeholder)
     $1::T :: U => $1[T] :: U

Then we type using the following phases, detailed below:

- 1. Constant folding for untyped constants, mandatory
- 2-6. Type assignment and checking
- 7. Constant folding for remaining typed constants, optional

The details:

1. Constant folding.

   This reduces complex expressions without losing information (like
   in [Go](https://blog.golang.org/constants)!) Literal constants are
   evaluated using either their type, if intrinsically known (for
   unambiguous literals like true/false, strings, byte arrays), or an
   internal exact implementation type for ambiguous literals
   (numbers). This is performed for all expressions involving only
   untyped literals and functions applications applied only to such
   expressions.  For number literals, the imlementation type from the
   [exact](<https://godoc.org/golang.org/x/tools/go/exact>)
   arithmetic library can be used.

   While the constant expressions are folded, the results must be typed
   using either the known type if any operands had one; or the unknown
   numeric type when the none of the operands had a known type.

   For example:

        true and false               => false[bool]
        'a' + 'b'                    => "ab"[string]
        12 + 3.5                     => 15.5[*N]
        case 1 when 1 then x         => x[?]
        case 1 when 1 then 2         => 2[*N]
        3 + case 1 when 1 then 2     => 5[*N]
        abs(-2)                      => 2[*N]
        abs(-2e10000)                => 2e10000[*N]

   Note that folding does not take place for functions/operators that
   are overloaded and when the operands have different types (we
   might resolve type coercions at a later phase):

        23 + 'abc'                   => 23[*N] + 'abc'[string]
        23 + sin(23)                 => 23[*N] + -0.8462204041751706[float]

   Folding does "as much work as possible", for example:

        case x when 1 + 2 then 3 - 4 => (case x[?] when 3[*N] then -1[*N])

   Note that casts select a specific type, but may stop the fold
   because the surrounding operation becomes applied to different
   types:

        true::bool and false         => false[bool] (both operands of "and" are bool)
        1::int + 23                  => 1[int] + 23[*N]
        (2 + 3)::int + 23            => 5[int] + 23[*N]

   Constant function evaluation only takes place for a limited
   subset of supported functions, they need to be pure and have an
   implementation for the exact type.

2. Culling and candidate type collection.

   This phase collects candidate types for AST nodes, does a
   pre-selection of candidates for overloaded calls and computes
   intersections.

   This is a depth-first, post-order traversal. At every node:

   1. the candidate types of the children are computed first

   2. the current node is looked at, some candidate overloads may be
      filtered out

   3. in case of call to an overloaded op/fun, the argument types
      are used to restrict the candidate set of the direct child
      nodes (set intersection)

   4. if the steps above determine there are no
      possible types for a node, fail as a typing error.

      (Note: this is probably a point where we can look at implicit
      coercions)

   Simple example:

        5[int] + 23[*N]

   This filters the candidates for + to only the one taking `int` and
   `int` (rule 2).  Then by rule 2.3 the annotation on 23 is changed,
   and we obtain:

        ( 5[int] + 23[int] )[int]

   Another example::

        f:int->int
        f:float->float
        f:string->string
        (12 + $1) + f($1)

   We type as follows::

        (12[*N] + $1) + f($1)
           ^

        (12[*N] + $1[*N]) + f($1[*N])
                    ^
        -- Note that the placeholders in the AST share
        their type annotation between all their occurrences
        (this is unique to them, e.g. literals have
        separate type annotations)

        (12[*N] + $1[*N])[*N] + f($1[*N])
                         ^

        (12[*N] + $1[*N])[*N] + f($1[*N])
                                  ^
          (nothing to do anymore)

        (12[*N] + $1[*N])[*N] + f($1[*N])
                               ^

   At this point, we are looking at `f($1[int,float,decimal,...])`.
   Yet f is only overloaded for int and float, therefore, we restrict
   the set of candidates to those allowed by the type of $1 at that
   point, and that reduces us to:

        f:int->int
        f:float->float

   And the typing continues, restricting the type of $1:

         (12[*N] + $1[int,float])[*N] + f($1[int,float])
                      ^^                ^       ^^

         (12[*N] + $1[int,float])[*N] + f($1[int,float])[int,float]
                                        ^                  ^^

         (12[*N] + $1[int,float])[*N] + f($1[int,float])[int,float]
                                      ^

   Aha! Now the plus sees an operand on the right more restricted
   than the one on the left, so it filters out all the unapplicable
   candidates, and only the following are left over::

         +: int,int->int
         +: float,float->float

   And thus this phase completes with::

         ((12[*N] + $1[int,float])[int,float] + f($1[int,float])[int,float])[int,float]
                                    ^^      ^
   Notice how the restrictions only apply to the direct children
   nodes when there is a call and not pushed further down (e.g. to
   `12[*N]` in this example).

3. Repeat step 2 as long as there is at least one candidate set with more
   than one type, and until the candidate sets do not evolve any more.

   This simplifies the example above to:

         ((12[int,float] + $1[int,float])[int,float] + f($1[int,float])[int,float])[int,float]

4. Refine the type of numeric constants.

   This is a depth-first, post-order traversal.

   For every constant with more than one type in its candidate type
   set, pick the best type that can represent the constant: we use
   the preference order `int`, `float`, `decimal`
   and pick the first that can represent the value we've computed.

   For example:

         12[int,float] + $1[int,float] => 12[int] + $1[int, float]

   The reason why we consider constants here (and not placeholders) is
   that the programmers express an intent about typing in the form of
   their literals. That is, there is a special meaning expressed by
   writing "2.0" instead of "2". (Weak argument?)

   Also see section
   [Implementing Rick](#implementing-rick-untyped-numeric-literals).

5. Run steps 2 and 3 again. This will refine the type of placeholders
   automatically.

6. If there is any remaining candidate type set with more than one
   candidate, fail with ambiguous.

7. Perform further constant folding on the remaining constants that now have a specific type.

## Revisiting the examples from earlier with Rick

From section [Examples that go wrong (arguably)](#examples-that-go-wrong-arguably):

```sql
    prepare a as select 3 + case (4) when 4 then $1 end
    --                  3[*N] + $1[?]       (rule 1)
    --                  3[*N] + $1[*N]      (rule 2)
    --                  3[int] + $1[*N]     (rule 4)
    --                  3[int] + $1[int]    (rule 2)
    --OK

    create table t (x decimal);
    insert into t(x) values (3/2)
    --                      (3/2)[*N]        (rule 1)
    --                      (3/2)[decimal]   (rule 2)
    --OK

    create table u (x int);
    insert into u(x) values (((9 / 3) * (1 / 3))::int)
    --                         3 * (1/3)::int   (rule 1)
    --                         1::int           (rule 1)
    --                         1[int]           (rule 1)
    --OK

    create table t (x float);
    insert into t(x) values (1e10000 * 1e-9999)
    --                       10[*N]      (rule 1)
    --                       10[float]   (rule 2)
    --OK

    select length(E'\\000' + 'a'::bytes)
    --            E'\\000'[string] + 'a'[bytes]     (input, pretype)
    --            then failure, no overload for + found
    --OK

    select length(E'\\000a'::bytes || 'b'::string)
    --            E'\\000a'[bytes] || 'b'[string]
    --            then failure, no overload for || found
    --OK
```

Fancier example that shows the power of the proposed
type system, with an example where Postgres would
give up:

```sql
    f:int,float->int
    f:string,string->int
    g:float,decimal->int
    g:string,string->int
    h:decimal,float->int
    h:string,string->int
    prepare a as select  f($1,$2) + g($2,$3) + h($3,$1)
    --                   ^
    --                   f($1[int,string],$2[float,string]) + ....
    --                            ^
    --                   f(...)+g($2[float,string],$3[decimal,string]) + ...
    --                            ^
    --                   f(...)+g(...)+h($3[decimal,string],$1[string])
    --                                  ^
    -- (2 re-iterates)
    --        f($1[string],$2[string]) + ...
    --               ^
    --        f(...)+g($2[string],$3[string]) + ...
    --                      ^
    --        f(...)+g(...)+h($3[string],$1[string])
    --                             ^

    --        (B stops, all types have been resolved)

    -- => $1, $2, $3 must be strings
```

## Drawbacks of Rick

The following example types differently from PostgreSQL::

```sql
     select (3 + $1) + ($1 + 3.5)
     --      (3[*N] + $1[*N]) + ($1[*N] + 3.5[*N])       rule 2
     --      (3[int] + $1[*N]) + ($1[*N] + 3.5[float])   rule 4
     --      (3[int] + $1[int]) + ...
     --                ^                                 rule 2
     --      (3[int] + $1[int] + ($1[int] + 3.5[float])
     --                                   ^  failure, unknown overload
```

Here Postgres would infer "decimal" for `$1` whereas our proposed
algorithm fails.

The following situations are not handled, although they were mentioned
in section
[Examples that go wrong (arguably)](#examples-that-go-wrong-arguably)
as possible candidates for an improvement:

```sql
    select floor($1 + $2)
    --           $1[*N] + $2[*N]     (rule 2)
    -- => failure, ambiguous types for $1 and $2

    f(int) -> int
    f(float) -> float
    g(int) -> int
    prepare a as select g(f($1))
    --                      $1[int,float]     (rule 2)
    -- => failure, ambiguous types for $1 and $2
```

## Alternatives around Rick (other than Morty)

There's cases where the type inference doesn't quite work, like

    floor($1 + $2)
    g(f($1))
    CASE a
      WHEN 1 THEN 'one'
      WHEN 2 THEN
         CASE language
           WHEN 'en' THEN $1
         END
    END

Another category of failures involves dependencies between choices of
types. E.g.:

    f: int,int->int
    f: float,float->int
    f: char, string->int
    g: int->int
    g: float->int
    h: int->int
    h: string->int

    f($1, $2) + g($1) + h($2)

Here the only possibility is `$1[int], $2[int]` but the algorithm is not
smart enough to figure that out.

To support these, one might
suggest to make Rick super-smart via
the application of a "bidirectional" typing algorithm, where
the allowable types in a given context guide the typing of
sub-expressions. These are akin to constraint-driven typing and a number
of established algorithms exist, such as Hindley-Milner.

The introduction of a more powerful typing system would certainly
attract attention to CockroachDB and probably attract a crowd of
language enthousiasts, with possible benefits in terms of external
contributions.

However, from a practical perspective, more complex type systems are
also more complex to implement and troubleshoot (they are usually
implemented functionally and need to be first translated to
non-functional Go code) and may have non-trivial run-time costs
(e.g. extensions to Hindley-Milner to support overloading resolve in
quadratic time).

## Implementing Rick: untyped numeric literals

To implement untyped numeric literals which will enable exact
arithmetic, we will use
https://godoc.org/golang.org/x/tools/go/exact. This will require a
change to our Yacc parser and lexical scanner, which will parser all
numeric looking values (`ICONST` and `FCONST`) as `NumVal`.

We will then introduce a constant folding pass before type checking is
initially performed (ideally using a folding visitor instead of the
current interface approach).  While constant folding these untyped
literals, we can use
[BinaryOp](https://godoc.org/golang.org/x/tools/go/exact#BinaryOp) and
[UnaryOp](https://godoc.org/golang.org/x/tools/go/exact#UnaryOp) to
retain exact precision.

Next, during type checking, ``NumVals`` will be evalutated as their
logical `Datum` types. Here, they will be converted `int`, `float` or
`decimal`, based on their `Value.Kind()` (e.g.  using
[Int64Val](https://godoc.org/golang.org/x/tools/go/exact#Int64Val>) or
`decimal.SetString(Value.String())`.  Some Kinds will result in a
panic because they should not be possible based on our
parser. However, we could eventually introduce Complex literals using
this approach.

Finally, once type checking has occurred, we can proceed with folding
for all typed values and expressions.

Untyped numeric literals become typed when they interact with other
types. E.g.: `(2 + 10) / strpos(“hello”, “o”)`: 2 and 10 would be
added using exact arithmatic in the first folding phase to
get 12. However, because the constant function `strpos` returns a
typed value, we would not fold its result further in the first phase.
Instead, we would type the 12 to a `DInt` in the type check phase, and
then perform the rest of the constant folding on the `DInt` and the
return value of `strpos` in the second constant folding phase.  **Once
an untyped constant literal needs to be typed, it can never become
untyped again.**

## Comments on Rick, leading to Morty

Rick seems both imperfect (it can fail to find the unique type
assignment that makes the expression sound) and complicated. Moreover
one can easily argue that it can infer too much and appear magic.
E.g. the `f($1,$2) + g($2,$3) + h($3,$1)` example where it might be
better to just ask the user to give type hints.

It also makes some pretty arbitrary decisions about programmer intent,
e.g. for `f` overloaded on `int` and `float`, `f((1.5 - 0.5) + $1)`,
the constant expression `1.5 - 0.5` evaluates to an `int` an forces
`$1` to be an `int` too.

The complexity and perhaps excessive intelligence of Rick stimulated a
discussion about the simplest alternative that's still useful for
enough common cases. Morty was born from this discussion: a simple set
of rules operating in two simple passes on the AST; there's no recursion
and no iteration.

## Examples where Morty differs from Rick

```sql
    f: int -> int
    f: float -> float
    SELECT f(1)
```

*M* says it can't choose an overload. *R* would type `1` as `int`.

```sql
    f:int->int
    f:float->float
    f:string->string
    PREPARE a AS (12 + $1) + f($1)
```

*M* infers `exact` and says that `f` is ambiguous for an `exact` argument, *R* infers `int`.

```sql
    f:int->int
    f:float->float
    g:float->int
    g:numeric->int
    PREPARE a AS SELECT f($1) + g($1)
```

*M* can't infer anything, *R* intersects candidate sets and figures
out `float` for `$1`.

## Implementation notes for Rick

Constant folding for Rick will actually be split in two parts: one
running before type checking and doing folding of untyped
numerical computations, the other running after type checking and
doing folding of any constant expression (typed literals, function
calls, etc.). This is because we want to do untyped computations
before having to figure out types, so we can possibly use the
resulting value when deciding the type (e.g. 3.5 - 0.5 could b
inferred as ``int``).

## Unresolved questions for Rick

Note that some of the reasons why implicit casts would be otherwise
needed go away with the untyped constant arithmetic that we're suggesting,
and also because we'd now have type inference for values used in `INSERT`
and `UPDATE` statements (`INSERT INTO tab (float_col) VALUES 42` works as
expected). If we choose to have some implicit casts in the language, then the
type inference algorithm probably needs to be extended to rank overload options based on
the number of casts required.

What's the story for `NULL` constants (literals or the result of a
pure function) in Rick? Do they need to be typed?

Generally do we need to have null-able and non-nullable types?

# Unresolved questions

How much Postgres compatibility is really required?

