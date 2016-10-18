- Feature Name: Intermediate Representation (IR)
- Status: draft
- Start Date: 2016-10-18
- Authors: knz, andrei, radu, cuong, irfan, arjun, davidE
- RFC PR: #10055
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC introduces an extended intermediate representation (IR) for
the SQL used by CockroachDB.

Summarized implementation strategy:

1. identify transforms (we're not doing great on this yet)
2. use a common set of IR nodes through transforms, our Expr nodes are
   already IR nodes
3. unzip the planNode construction hierarchy
4. start working on new IR-based transforms

Summarized suggested architecture post-implementation:

1. IR mostly consists of AST from parse to/including semantic analysis
2. after semantic analyzed IR progressively transformed to non-SQL; transforms
   incrementally optimize the tree/graph and create annotations
3. as late a possible a final transform produces the nodes suitable for execution:
   - that's where distSQL physical plan creation kicks in
   - we want to avoid IR transforms after that point; if there are transforms
     that add distSQL-specific annotations we can always
     annotate the nodes in a way that keeps the IR valid as input
     for the other transforms.
4. transforms are implememented to ignore+preserve annotations
   they don't recognize.

# Motivation

Primary motivations:

- to enable sharing query analysis and optimization between the planNode
  and distSQL execution back-ends.
- to enable more straightforward definitions for query optimization algorithms.
- to encode the query of non-materialized views.
- to provide robustness against db/table/column/view renames in a lightweight fashion.
- (hopefully) to version built-in function and virtual table references.
- (optionally) to transfer filter expressions more efficiently across the wire when spawning distSQL processors.

Non-constraints:

- The IR needs not be pretty-printable back to valid SQL. In the few
  cases where something needs to produce SQL syntax back to a user
  (e.g. SHOW CREATE), the IR node can keep a copy of the string in
  original syntax before encoding in the IR.

# General principles behind the approach

To talk about IRs we must first define what is being represented and why.

For example, the reason why we work using an AST instead of the raw
characters of a SQL string is twofold.

One of the two reasons, the one that most people can name from the top
of their head, is that the SQL query needs to be analyzed two or more
times; at least once to check it is well-formed, and another to run
it. Figuring out the boundary of the semantic components of a query in
the raw text every time it needs to be analyzed is wasteful. The tree
data structure accelerates that.

The other reason is that *each analysis may need annotations on the
SQL code that have been produced by a previous analysis*. For example,
after type checking each expression node is annotated with its type,
so that the execution-related pass(es) afterwards know which SQL data
type they are dealing with.

This second reason is really the most important, and also why this RFC
exists in the first place. To talk about IRs, we should not really
start by talking about IRs, and talk instead about **program
transformations**.

(Disclaimer: if you've ever studied program transformation /
compilation, even casually, all this may sound too simple, so feel
free to skip ahead. However I surmise that a refresher doesn't hurt.)

In other words, instead of thinking about and describing what your
code does like this:

```
,-----.                ,-----.               ,---------------------.
| SQL +--(magic???)--->| IR  +--(magic???)-->| planNode, or whatev |
`-----'                `-----'               `---------------------'
```

Think and explain like this:

```
,--------------.     ,-------.    ,---------------.
| Input reader +---->| Parse +--->| Resolve names +----.
`--------------'     `-------'    `---------------'    |
                                              ,------------------.
                                              | Constant folding |
                                              `--------+---------'
       ,-----------------------.    ,------------.     |
 ,-----+ Normalize expressions |<---+ Type check |<----'
 |     `-----------------------'    `------------'
 |
 `--> ...
```

In this visual phrasing, the emphasis is on the transformations: each
block takes representations of some language as input and produce
representations of some language as output.

## Some fun stuff about transforms and languages

(This is theory trivia - not strictly necessary for this RFC, feel
free to skip ahead to the next section.)

In some transformations (blocks) the input and output language are the
same -- the output of one block can be passed back to it as
input. When this happens, we can classify the blocks either as
*idempotent* (you get the same result if you apply it two or more
times in a row as if you run it just once) or not. Constant folding
and expression normalization, for example, is typically
idempotent. Some stochastic index optimization methods based on query
statistics, in contrast, are not.

In a larger number of cases, a transformation will annotate its input,
add information or produce constructs that couldn't be possibly be
produced by other transforms upstream, and thus have an output
language that is distinct from the output language of upstream transforms.

When this happens, we can distinguish two interesting cases:

- when the output language overlaps with the input language (some
  outputs can be given back as input and the additional information
  will be ignored/discarded) - this suggests reuse of data structures
  in the implementation. Type checking in our current code is an
  example of that. The "Type" field is present (albeit unassigned by
  the parser and constant folding) in the syntax nodes that type
  checking uses as input.

- when the input language of a transform is larger than the output
  language of upstream transforms (the transform accepts more inputs
  than can be possibly produced upstream) - this suggests reuse of the
  transform in multiple places and even possibly commutativity of
  transforms. For example in our current code, name resolution
  replaces name syntax nodes with IndexedVar objects. The IndexedVar
  cannot be possibly produced by the parser, so the output language of
  name resolution is distinct from the parser's output
  language. However meanwhile the name resolution will happily ignore
  indexed vars in its input; its input language is at least as large
  as its output language, and strictly larger than the output of
  parsing. We can re-apply name resolution multiple times, even
  possibly later.

## Dealing with many transforms

In most "interesting" languages there are many transforms over input
programs before their final form or interpretation. CockroachDB's SQL
is no exception. At the time of this writing, expressions undergo at
least the 6 transforms given above and for some of them a couple more
(e.g. filter substitution).

As the number of transforms grow, so do concerns like "should this
transform be applied before or after that one?" Sometimes the concern
is one of "dependency management": the data needed by this transform
is produced by that other transform. Sometimes the concern is one of
performance, when a transform is best applied when some normalized
form has been reached already. Nevertheless, from an engineering
perspective, it is good to strive to:

- enable independent design and deployment of transforms by different
  people;
- automate the scheduling of the transforms or at least the verification
  that they are performed in an order that makes sense semantically.

In the last twenty years, advances in programming language engineering
(PLE, a field of CS) have bubbled up some best practices to help a team in
this direction.

## General design principles: IR data structures

An example of a "state of the art" consensus is that a common set of
data structures used across all semantic transforms is a better way to
implement a chain of transformations than having each "link" between
two transforms use a distinct set of data structures. So instead of
using successive and distinct `RawExpr`, `FoldedConstExpr`, `TypedExpr`,
`NormalizedExpr` data types, we use a single `Expr` type which is
enhanced with additional information as it flows through the chain of
transforms.

We already do this for expressions! Thanks to Nathan in particular who
pushed for this when introducing a separate phase for type
checking. But we don't do this for the core SQL constructs. Consider
the following chain of transformations (optimizations) which we
currently do implement:

```
                        ,----------------------.
(expr transforms)... -->| transformToPlanNodes +----.
                        `----------------------'    |
                   ,--------------------------.     |
            ,------+ Split filter expressions |<----'
            |      `--------------------------'
            |      ,-------------------------------.
            `----->| Push filters down to scanNode +----> ...
                   `-------------------------------'
```

The output language of "transform to planNodes" is currently
completely different from its input language. A bigger problem is that
it is implemented using completely different data structures (the
`planNode` hierarchy). This creates an irreducible phase difference
which prevents reusing the "split filters" transform in a different
order, in particular *before planNodes are constructed*.

Why does this matter?

In CockroachDB, in short, because it's wasteful to generate all the
planNode contents if they're not used afterwards (e.g. when we want to
run the query via the distSQL back-end, which uses different data
structures); and because it's hard to re-structure a planNode tree
after it's being generated.

The previous paragraph was written to make it concrete to
CockroachDB-educated audience, but the concern is really general. The
reason why a program transformation engine (a "compiler", really)
chooses an abstract IR removed from the concrete execution engine is
to decouple the implementation of back-end code generators /
excecution engines from the implementation of program
optimizations. This is a good goal to set in and of itself from an
organizational perspective, and that's why PLE practitioners have come
to the consensus this is the right way to go in any project with a
sizeable language processing component.

## General implementation direction: auto-generate the code

Another example "state of the art" consensus that has emerged in the
last few decades is to have the data structures used to encode the IR
in memory generated automatically from some high-level
specification. There are a couple reasons for that.

The first reason is rather straightforward: for each conceptual node
in the IR, there are multiple pieces of code involved. One to define
the data structure; one for pretty printing, one for
serialization/deserialization (if you need that), some accessors with
consistency checks, perhaps a few more for
instantiation/initialization.  A lot of this code is boilerplate with
a regular structure. That's analogous to why the protobuf generator
exists.

The second reason, incidentally the main, is one that usually becomes
clear only after some experience in PLE: often during development it
is not known from the beginning which IR nodes are needed and which
combinations of data structures will end up most useful. Should this
node be a child of this one or that other one? Should this attribute
be called this or that way? Should this concept be split into
different nodes or merged into one? The answers to these questions
usually change quite a lot, multiple times and sometimes often at the
early stage of a project -- which is a good thing, ensuring that the
IR, a central piece of the technology, quickly converges to a solid
long-time form.

It is thus rather important that the decision to change something does
not incur a large manual code refactoring, at least at the
beginning. Having the ability to change things in a *succint*
high-level specification and re-generate from there is a boon.
(Although the reader may underline that this benefit is reduced if
there is any sizeable manually written code around the IR structures
already. This may become increasingly true as the project lives on,
but might still be maneageable during the initial period of quick IR
design iterations.)

Then there's a third reason which somewhat derives from the principles
outlined in the previous sections.

Once you make the decision to reuse the same data structures
throughout multiple transforms, some attributes of the nodes will not
be initialized/useful before they go through the transform that
populates them. If you want, at the same time, to keep the freedom to
re-order transforms without too much hassle, it becomes rather
important to place assertions in the code: "this attribute must be
initialized at this point" every time there's a dependency and "this
attribute should not be initialized yet at this point" in some
cases. It's even often possible to auto-generate the code that calls
the transforms (the transform pipeline) and programmatically check
that the desired order is sound.

## General implementation direction: pattern matching

Transforms of programs to programs are a special case of graph
transformations, usually trees.

There's a general consensus that the work of the programmer is orders
of magnitude / qualitatively improved when the language in which
transforms are written supports proper pattern matching, pattern
binding and pattern substitution.

See Rust, ML, Haskell, Scala etc for references.

An example use if you've never seen this in action: removing the minimum of a RB tree in OCaml.
https://github.com/bmeurer/ocaml-rbtrees/blob/ff9818fb97e631e3f565d917caf80e55e1c5967a/src/rbset.ml#L159

When your implementation language doesn't support pattern matching,
then you can work to add it! This has been done e.g. for C++ (in Boost).

# Overview of the design & implementation in CockroachDB

Applied to CockroachDB, the general principles from above suggest the following:

1. Focus first on identifying the transforms:
   - Name them, identify what they consume and produce
   - Identify which transforms can be reused in different places, that
     will guide the definition of attributes
2. Use common IR data structures across transforms:
   - Describe which fields are used by which transforms at a high level (not Go code)
   - Fields get populated as they get through the transforms
3. Work towards auto-generating code:
   - For IR nodes
   - For serialization/deserialization
   - For chaining transforms or for asserting fields are initialized before use
   - (bonus) to ease writing transforms, by supporting pattern matching.

This process will be iterative, where we start little (few transforms
identified/separated, few IR nodes identified/recognized) and grow
incrementally. Also at the beginning auto-generation of code will not
be ready so we'll have to work manually on the other items.

Taking into consideration what already exists in the code, we identify
the following more concrete implementation aspects as starting points:

1. to start identifying/identifying transforms, work with the code
   that's already written. A lot is already organized as a pipeline of
   transforms, even if it was not explained as such so far. The first
   step is thus not to create new transforms or re-implement anything
   but simply name those transforms we already have and identify their
   input/output requirements.

   - Most of the basics transforms are already identified above (see
     ASCII art diagrams), what's missing is identifying their
     input/output requirements. Some examples given below.

   - The main thing not yet properly identified/separated is the work
     performed by `planNode` constructors. Some examples given below.
     The likely first implementation step (PoC ongoing) is to split
     some constructors into 1) a transform that populates an IR node
     and 2) another transform that actually creates the planNode.

2. regardless of how much code auto-generation we're doing (or not) we
   should work using a high-level description of the IR to describe IR
   nodes, their attributes, transform, and derive the other code from
   there, instead of the other way around. Also a HLL description will
   make it easier for contributors to keep the entire thing in their
   head at once. Some directions below.

3. the IR node code, if auto-generated, should overlap name-wise with
   the nodes already defined, so they can be used as drop-in
   replacement into the existing code (i.e. preserve struct and
   attribute names)

# Recognizing transforms

Which are the transforms we want? The following list identifies both
the transforms we already have and some that we don't yet have but
should have soon:

1. scan/parse
2. **[new]** replace statements by logical nodes; this will
   exercise the following transforms which already exist:

   1. name resolution
      - resolve table names to table IDs
      - resolve column names to IndexedVars
      - resolve function names to FunctionDefinition
      - **[new]** annotate the logical plan with the table descriptors
        that it uses, and annotate for each table the set of columns
        actually used. This is used to compute dependency information for views etc.
   2. constant folding
   3. type propagation and checking
   4. preparation of ancillary metadata for logical operation nodes:
      - annotation of join columns by index for join nodes
      - annotation of columns with default values for insert
      - semantic checks
      - permission checks
      - etc. (this extracts the logic currently present in planNode constructors)
      - **at the end of this stage, a statement is valid and can
        be run**
   5. expression reduction (currently called "normalization", see
      `sql/parser/normalize.go`), including:
      - partial evaluation of constant expressions
      - rebalancing of comparisons to "pull" non constant
        elements up the expression trees (e.g. `a+1 = 2` to `a = 1`)
      - **[new]** desugaring: replacing Expr nodes that only exist
        to enable pretty-printing back to SQL to more simple nodes.
   6. normalization (currently called "analysis" or "simplification",
      see `sql/analyze.go`), including:

      - desugaring (e.g. `a IS NOT DISTINCT FROM NULL` to `a IS NULL`)
      - strength reduction (e.g. `a LIKE 'foo%'` to `a >= "foo" AND a < "fop"`)
      - comparison simplification
      - **[new]** normalization to disjunctive normal form (an OR of ANDs)
        this is needed for filter propagation and index selection below.
      - partial evaluation of disjunctions and conjunctions
        (`true AND E` simplified to `E`, `true OR E` simplified to `true`, etc.)
4. **[new]** sub-query transposition, where sub-queries, where applicable,
   are transformed into a join operation
5. **[new]** filter propagation (also see #10633 ) which includes:
   - at each level where a filter is brought from an outer/downstream node,
     perform reduction/normalization again before recursing
6. **[new]** query structure optimization; possibly an iterative application of
   the following, with comparisons of resulting query complexity:

   1. index selection
   2. **[new]** ordering propagation (this can simplify nodes)
   3. **[new]** ordering relaxation, where every ordering that is not
      strictly required for the result is removed from the node's
      ordering metadata annotations (this can in turn yield further simplifications)

      e.g. `SELECT * FROM a JOIN (SELECT * FROM b WHERE z=3 ORDER BY
      z)` can be simplified to `SELECT * FROM a JOIN (SELECT * FROM b
      WHERE z=3)` because a JOIN is not guaranteed to preserve order.
7. generate execution nodes (distSQL physical plan or planNodes)

# Recognizing the IR nodes

Based on the list of transforms we can identify the IR nodes we
already have:

- the current Expr nodes are IR nodes.
  - there may be some minor tweaking of IN, to use different nodes for when
    an operand is a tuple generated by a subquery.
  - function references may need to capture the version of the function

The IR at a high level:

1. logical nodes for expressions over datums (what we currently call Expr)
2. logical nodes that define an abstract operation on row sets from other nodes,
   these somewhat mirror the current planNode hierarchy:
   - filter (with predicate)
   - distinct
   - group/aggregate
   - render (adds new columns)
   - window
   - sort
   - regular join, with annotations:
     - join predicate: cross/on/using/natural/equality (also see issue #10630)
     - which algorithm to use
   - set operations: union, intersect, except
   - ordinality
3. logical nodes that act as data sources, again this mirrors the
   current planNode hierarchy:
   - scan
   - show
   - explain
   - join by lookup (currently used for index joins; this is really a hybrid)
   - VALUES clauses
   - **[new]** virtual tables, this should probably be versioned
   - **[new]** set-generating functions, this should be versioned too
   - including those logical nodes that perform writes on the database (these
     are a subset of those acting as a data source semantically because of RETURNING,
     even though in the common form the result set is empty):
     - insert
     - update
     - delete
     - schema updates
4. logical nodes that structure the semantic interpretation:
   - with

The list above identifies 4 "categories" of nodes.  The most interesting
are the 2nd and 3rd category which in the abstract describe computations
that produce row sets.

These must be equipped of a standard set of annotations:

- **[new]** some information about the syntactic origin of the IR node
  in the original text coming from the client, for error reporting. 
  In other clients this can be very elaborate, keeping track of begin and end position,
  but let's not be ambitious this is just SQL after all, so a simple start
  position will be sufficient. This location is then used to
  report error messages instead of pretty-printing the IR tree back to SQL.
- **[new]** a type signature which describes its columns (akin to a
  mix between our current **ResultColumns** and **dataSourceInfo**)
- metadata about the *apparent* ordering of results (currently called `orderingInfo`)
- **[new]** for nodes that operate on the result of other nodes, metadata about
  the *exploited* ordering of the sub-nodes' results. This is to be
  used by the ordering relaxation transform identified above.

And a standard set of methods:
- adding a new filter predicate (see #10632)
- inform a node of which part of its apparent ordering is actually used,
  so that it can simplify itself or its descendants in some cases.


# Implementation directions

This section outlines a strategy for implementation:

1. The IR nodes must be appropriately *named*, at least in documentation,
   so as to provide a common language to drive further discussions and serve
   as references in code TODOs. (FIXME: perhaps this RFC should do just this?)
2. the planNode constructor recursion must be unzipped; for each
   current planNode type extract one or more IR node which carries the
   non-operational attributes of the planNode then simplify the
   planNode type accordingly. To achieve this:
   1. start at the leaf planNodes (e.g. scan, show, values)
   2. create the new IR node type, then make that IR node an attribute
      of the planNode where it comes from
   3. change the planNode constructor to populate the new IR node type via
      the planNode object
   4. split the planNode constructor into another function that just constructs the IR node,
      and another which takes the IR node as input and produces planNode.
   5. modify `newPlan` to call the IR node constructor first, then
      the `planNode` constructor on the result.
   6. take an intermediate planNode (e.g. index join),
      modify its constructor to take IR nodes for its children
      operand as input in addition to the children planNodes
   7. change the code of the intermediate planNode to use only the
      children IR nodes to initialize the intermediate IR node. Then
      split the constructor as per steps 4-5 above.
   8. modify `newPlan` to propagate the IR nodes of children to intermediate constructors.
   9. repeat for all other planNodes.
   10. when the entire hierarchy is split, split `newPlan` too, into
       one recursion to create the IR tree and another for the
       `planNodes`.

A side-effect of performing these steps will be to achieve a first
implementation of a working IR node hierarchy.

In parallel, work on auto-generation (see below) can start and proceed.

Once a first IR hierarchy is ready and plugged into the current code base,
time comes to start adding the new transforms.

# Directions for auto-generation of code

Overview:

1. the IR is formally defined in a type specification language. See
   below for details/draft.
2. a (new) tool translates the IR definition into:
   - Go structs/protobufs suitable for serialization.
   - accessor methods.
   - walk (visitor) recursion.
   - match methods meant to ease pattern matching (see below for details)
   - pretty printer (optionally scanner if need be).
3. when applicable, the structs generated by (2) coincide in name and
   field structure with the AST nodes already present in CockroachDB, so
   as to enable smooth(er) integration in the existing codebase.
4. the existing code can be evolved to use the IR:
   - DEFAULT expressions in table schemas.
   - foreign key constraints.
   - view definitions.
   - EXPLAIN.

## Formal specification

We want to define the IR in a high(er)-level language (HLL) that eases the
definition of complex type hierarchies.
This HLL must support sum and product types, probably parameterized types too.

(NB: We may want it to support record types too for readability, but from
a theoretical perspective record types are just glorified product types.)

This RFC proposes to use a subset of ML as IR definition HLL.
A draft of the definition for the proposed IR is presented in appendix below.

## IR elements

The IR contains node types for expressions, as expected.
Salient aspects of expressions:
- column references are initially (just after parsing from SQL)
  unresolved names, but collapse into integer column references during semantic analysis.
- SQL's "IN" has two IR encodings, depending on whether one of the operands is a subquery.

The bulk of what makes SQL special is the IR for "table expressions", the
IR nodes that act as data sources for others.
Salient features:
- the IR contains a node for the entire SelectClause, but this is only meant
  as transient node to be normalized into a tree of other tableexprs.
- the IR for table expressions reifies the operators from relational
  algebra: Join Filter, Render, Sort, etc.
- Insert, Update, Delete, Explain and Show are also table expressions in the IR.
  For these a COUNT operations gets automatically wrapped if they don't
  specify RETURNING.

The IR gives only minimal lip service to "statements". Because most of
the interesting semantics of "statements" are table expressions in the
IR, there is only one statement "Do" that says "run the plan for this
table expression and return its results". Every other statement can be
expressed as a composition of Do and some table expression. Some examples
are given in comments in the draft below.

## Match functions

In other languages with pattern matching (eg. Rust, ML, Haskell, and
C++ to some extent) one can make nifty operations based on tree
patterns. For example:

```ocaml
  match expr with
  | BinOp(Gt, a, b)              -> BinOp(Lt, b, a)
  | Unop(Not, BinOp(GtEq, a, b)) -> BinOp(Lt, a, b)
```

The idea of these match operators is that there is a *pattern* on the
left hand side which is used as conditional; if the input item matches
the pattern, the item it is *deconstructed* and names inside the
pattern are bound to the matching sub-part of the input item in the
scope of the right hand side.

This is really a good feature to have in general! And we can "emulate" this
with Go given sufficient support from a tool to generate the missing pieces.

What we could want to write in go:

```go
oexpr := match(iexpr,
    "BinOp(Gt, a, b)",              func(a, b Expr) Expr { return BinOp{Lt, b, a} },
    "UnOp(Not, BinOp(GtEq, a, b))", func(a, b Expr) Expr { return BinOp{Lt, a, b} },
    )
```

How to get this to work?

A tool would scan the source file and produce a separate file with the following definitions:
```go
var matchers := map[string]matcher

func match1(iexpr Expr, actionfn func(...interface{})) {
  if t, ok := iexpr.(*BinOp) ; ok {
    if _, ok := t.op.(*Gt); ok {
      actionfn(t.left, r.right)
   }
  }
}

func caller1(items ...interface{}, resultfn interface{}) interface{} {
   resultfn_real := resultfn.(func (Expr, Expr) Expr)
   return resultfn_real(items[0].(Expr), items[1].(Expr))
}

func init() {
  matchers["BinOp(Gt, a, b)"] = matcher{match1, caller1}
  matchers["UnOp(Not, BinOp(GtEq, a, b))"] = {match2, caller2}
}
```

Then the common implementation of `match` will look up the functions
in the global array `matchers` and Do The Right Thing (TM).

Note this is slow but is assuming that we don't want a Go
preprocessor. With a preprocessor, the "call" to the match function
could be replaced by the explicit conditionals and kill the overhead.

# Drawbacks

This is a semi-large refactoring of the existing code.

# Alternatives

None knowns.

# Unresolved questions

- Expression tree can contain nodes with Go values (expecially
  timestamps, dates). What to do with those with respect to
  serialization?

# Appendix: draft IR definition

```ocaml
(* How to read these type definitions: *)

(* this is a comment *)

(* this defines "ident" to be an alias for "string" *)
type ident = string

(* this defines "intpair" to be the type of a tuple of two integers: *)
type intpair = int * int

(* this defines "i2list" to be the type of a list of zero or more
     int pairs: *)
type i2list = intpair list
(* equivalent to: "type i2list = (int * int) list" *)

(* this defines "namedpair" to be the type of a record containing a
     "left" and "right" integer: *)
type namedintpair = { left  : int
                    ; right : int
                    }
(* Note: the main definitions below do not (yet) use record syntax.
   We may add them later when the generator tool is ready for them. *)

(* this defines "smallbinop" to be the type containing only the two
   values "Plus" and "Minus" (variant/union/sum type): *)
type smallbinop = Plus | Minus
(* alternate syntax: *)
type smallbinop_alt = | Plus | Minus

(* this defines "smallexpr" to be the type of expression trees that can
   express integer values and binary operations on them: *)
type smallexpr =
  | IntVal of int
  | BinOp of smallbinop * smallexpr * smallexpr
(* the syntax "of" says that the variant name on the left becomes a
   variant with the value type on the right. Note that a type can
   refer to itself. *)

(* this defines two recursive types *)
type ifstmt = { cond  : smallexpr
              ; sthen : smallstmt (* refers to smallstmt below *)
              ; selse : smallstmt
              }
 and smallstmt =  (* note the "and" keyword *)
   | Seq of smallstmt list
   | If of ifstmt (* refers to ifstmt above *)

(* You're done understanding the syntax! Now on to our SQL AST types. *)

(**********************************************************************)
(**********************************************************************)

(* The following abstract type can represent every SQL type. *)
type ty =
  | Int | Bool | Float | Timestamp | Date | Interval
  | Bytes of int (* int specifies the max width in bytes, 0 for no max *)
  | String of int (* int specifies the max width in characters, 0 for no max *)
  | CollatedString of string (* string specifies the collation format *)
  | Decimal of int * int (* precision/scale, 0 for no restriction *)
  | TupleTy of ty list
  (* We don't fully support arrays yet, but we will. *)
  | ArrayTy of ty
  (* We don't support records yet, but we will. *)
  | RecordTy of (string * ty) list
  (* TableTy is not commonly understood to
     be part of SQL, but it makes typing of subqueries *much* easier. *)
  | TableTy of (ty * string) list
  | NullTy (* not sure this belongs. I think it can be removed *)

(* This type represents operators that compute a new value from left
   and right operands. Used by "expr" below. *)
type binop =
  (* Boolean operators *)
  | And | Or
  (* Arithmetic: + - / // % *)
  | Plus | Minus | Mult | Div | FloorDiv | Mod
  (* Binary arithmetic: & | ^ << >> *)
  | BitAnd | BitOr | BitXor | LeftShift | RightShift
  (* String: || *)
  | Concat

(* This type represents operators that compute a new value from a
   single operand. Used by "expr" below. *)
type unop =
  (* Boolean negation *)
  | Not
  (* Unary negation: - *)
  | Neg
  (* Binary complement: ~ *)
  | Compl

(* The type "cmpop" represents operators that compare two values.
   It is separate from "binop" above because comparisons have
   in common that they all return booleans. *)
type casei = bool
type cmpop =
  | Eq                   (* =   -- does not match NULL *)
  | Is                   (* IS  -- matches NULL *)
  | Lt                   (* < *)
  | In                   (* IN  -- used for tuples only after normalization;
                            see InExpr for subqueries below. *)
  | Like of casei        (* LIKE / ILIKE *)
  | RegMatch of casei    (* ~ / ~* *)
  (* The following are syntactic sugar; sorta.
     They disappear soon after parsing, to simplify intermediate transforms,
     and then re-introduced when instantiating table scan filters since
     the expanded form can expose more intelligent scan orders. *)
  | Neq                  (* a <> b       === NOT(a = b) *)
  | IsNot                (* a IS NOT b   === NOT(a IS b) *)
  | LtEq                 (* a <= b       === NOT(b < a) *)
  | GtEq                 (* a >= b       === NOT(a < b) *)
  | Gt                   (* a > b        === b < a *)
  | NotIn                (* a NOT IN b   === NOT(a IN b) *)
  | NotRegMatch of casei (* a !~ b       === NOT(a ~ b) *)
  | NotLike of casei     (* a NOT LIKE b === NOT(a LIKE b) *)

(* Type of type names in expressions: *)
type tyexpr =
  | UnresolvedTypeName of ident
  | UnresolvedTupleTy of tyexpr list
  | UnresolvedArrayTy of tyexpr
type tyref =
  (* When it has just been parsed *)
  | UnresolvedType of tyexpr
  (* After type name resolution *)
  | Ty of ty

(* Type of aggregation: *)
type agg = All | Distinct

(* Name parts in unresolved names: *)
type namepart =
  | Name of ident
  | Star
  | Index of expr

(* Table name: *)
and tref =
  (* When it has just been parsed *)
  | UnresolvedTableName of namepart list
  (* After name resolution, it contains the DB ID + table ID. *)
  | TRef of int * int

(* Column reference: *)
and cref =
  (* When it has just been parsed *)
  | UnresolvedColumnName of namepart list
  (* After name resolution, it contains the index of the column at the
     current query level. *)
  | CRef of int

(* Database reference: *)
and dref =
  (* When it has just been parsed *)
  | UnresolvedDBName of namepart list
  (* After name resolution, it contains the DB ID. *)
  | DRef of int

(* Function reference: *)
and fref =
  (* When it has just been parsed *)
  | UnresolvedFuncName of namepart list
  (* After name resolution, it contains the CockroachDB version and
     function name.  We need both for forward-compatibility.  *)
  | FRef of ident * ident

(* Expressions *)
and expr =
  (* NULL *)
  | NullVal
  (* Literals. *)
  | IntVal of string (* before type checking, evaporates afterwards *)
  | DecVal of string (* before type checking, evaporates afterwards *)
  | DInt of int | DFloat of float (* ... some more omitted here ... *)
  | DString of string | DBytes of string
  | DCollatedString of string * string
  (* Compound expressions. *)
  | Tuple of expr list
  | Array of expr list
  (* Special value DEFAULT. *)
  | DefaultVal
  (* Placeholders for PREPARE/EXECUTE. *)
  | Placeholder of string
  (* A reference to a column in the "current row". *)
  | ColRef of cref
  (* Arithmetic and comparisons. *)
  | BinOp of binop * expr * expr
  | UnOp of unop * expr
  | CmpOp of cmpop * expr * expr
  (* Conditionals. *)
  | If of expr * expr * expr
  | Case of expr * (expr * expr) list * expr
  | IsOf of expr * tyref list   (* <E> IS OF (<T>, <T>, ...) *)
  | NullIf of expr * expr
  | Coalesce of expr list
  (* Function call. *)
  | Call of fref * expr list
  (* Aggregate function. *)
  | Agg of fref * agg * expr list
  (* Cast: <E>::<T>  but also things like TIMESTAMP '....' *)
  | Cast of tyref * expr
  (* CockroachDB's special <E>:::<T> to help typing. *)
  | AnnotateType of tyref * expr (* evaporates after type checking *)
  (* <E> IN <Subquery> *)
  | InTable of expr * tableexpr
  (* EXISTS(<Subquery>) *)
  | Exists of tableexpr

(* "Select clause": something that can return a table. Can be typed
   using TableTy above. *)
and tableexpr =
  | Values    of expr list list
  (* Table composition. *)
  | Union     of tableeexpr * tableeexpr
  | Intersect of tableeexpr * tableeexpr
  | Except    of tableeexpr * tableeexpr
  (* SelectClause evaporates after normalization
   into the nodes below it. *)
  | SelectClause of   isdistinct
                      * render list
                      * tableexpr             (*FROM*)
                      * expr option           (*WHERE*)
                      * expr list             (*GROUP BY*)
                      * expr option           (*HAVING*)
                      * window                (*WINDOW*)
                      * (expr * sortdir) list (*ORDER BY*)
                      * expr                  (*LIMIT*)
  | Join     of jointype * joinpred * tableexpr * tableexpr
  | Distinct of tableexpr
  | Filter   of tableexpr * where
  | GroupAgg of tableexpr * int list * expr list (* the ints refer to the grouping columns *)
  | Render   of tableexpr * expr list
  | Window   of tableexpr * window
  | Sort     of tableexpr * (int * sortdir) list (* the ints refer to the sorting columns *)
  (* Statements returning rows. *)
  | Insert   of tref * cref list * tableeexpr * onconflict
  | Update   of tref * assign list
  | Delete   of tref * where
  | Explain  of tableexpr
  | Show (* ...parameters omitted... *)
  (* Alias and RenameColumns may be used during name resolution. Probably transient. *)
  | Alias of tableexpr * ident
  | RenameColumns of tableexpr * ident list
  (* With clauses: WITH a AS (subquery) ... also probably evaporates after name resolution. *)
  | With of tableexpr * ident * tableexpr

and isdistinct = bool
and render =
  NormalExpr of expr * ident option
| RenderStar
and assign = cref list * expr
and sortdir = Asc | Desc
and joinpred = AlwaysTrue | Natural | Using of ident list | On of expr
and jointype = Inner | OuterLeft | OuterRight | OuterFull | Cross
and window = (ident * ident * expr list * expr list) list
and onconflict =
  | Error (* normal insert *)
  | DoNothing
  | Rewrite of  cref list * assign list * where

(* Top-level statements. *)
and stmt =
(* Do(T) means run the clause T (see above for clause types) and return
   all its results. *)
(* For example SELECT is encoded as Do(SelectClause). *)
| Do tableexpr

(* DoAndCount(T) means run the clause T and return the count of its results. *)
(* For example INSERT/UPDATE/DELETE is initially encoded as DoAndCount(<Clause>). *)
(* DoAndCount(T) then disappears during normalization into Do(GroupAgg(T, *, COUNT( * ))). *)
| DoAndCount tableexpr

(* DoAndReturn(T, E...) is syntactic sugar for, and is normalized to,
   Do(Render(T, E...)).  *)
(* For example INSERT/UPDATE/DELETE...RETURNING are initially encoded as DoAndReturn. *)
| DoAndReturn tableexpr * render list

(* < some more stmt types omitted here ... > *)
```
