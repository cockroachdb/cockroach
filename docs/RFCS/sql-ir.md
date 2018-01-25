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

- to accelerate tree transformations.
- to alleviate pressure on the Go heap allocator/GC.
- to enable sharing query analysis and optimization between the local
  and distributed execution engines.
- to enable more straightforward definitions for query optimization algorithms.
- to encode the query of non-materialized views.
- to provide robustness against db/table/column/view renames in a lightweight fashion.
- (hopefully) to version built-in function and virtual table references.
- (optionally) to transfer filter expressions more efficiently across the wire when spawning distSQL processors.

Non-constraints:

- The IR needs not be pretty-printable back to valid SQL beyond a
  certain transformation point. In the few cases where something needs
  to produce SQL syntax back to a user (e.g. SHOW CREATE), the IR node
  can keep a copy of the string in original syntax before encoding in
  the IR.

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
block takes representations of some language as input and produces
representations of some language as output.

## Some fun stuff about transforms and languages

(This is theory trivia - not strictly necessary for this RFC, feel
free to skip ahead to the next section.)

In some transformations (blocks) the input and output language are the
same -- the output of one block can be passed back to it as
input. When this happens, we can classify the blocks either as
*idempotent* (you get the same result if you apply it two or more
times in a row as if you run it just once) or not. Constant folding
and expression normalization, for example, are typically
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

1. logical nodes for the abstract syntax of SQL input, including:
   - expressions over datums (what we currently call Expr)
   - relational operators that can be expressed in SQL prior to
     semantic analysis and desugaring (including the distinction
     between USING/NATURAL/ON for joins, for example)
	 
2. logical nodes that support transforms by providing additional
   "internal" semantics, including:
   - a "Let" node to create identifier bindings within a subtree (will
     be used for CTEs and correlated subqueries)

3. logical nodes for logical plans. This includes the planNode hierarchy.

All these IR nodes should contain:

- **[new]** some information about the syntactic origin of the IR node
  in the original text coming from the client, for error reporting. 
  In other clients this can be very elaborate, keeping track of begin and end position,
  but let's not be ambitious this is just SQL after all, so a simple start
  position will be sufficient. This location is then used to
  report error messages instead of pretty-printing the IR tree back to SQL.

The IR nodes for logical plans should contain:

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

The proposed list of node names and required properties is given in
the appendix "proposed naming of nodes".

# Implementation directions

This section outlines a strategy for implementation:

1. the Go-specific values (e.g. decimal) which cannot/should not be easily
   encoded in an IR must be extracted out of the current tree data structure
   using the [Auxiliary data pattern](XXXX_auxiliary_data.md).
2. the IR nodes must be appropriately *named*, at least in documentation,
   so as to provide a common language to drive further discussions and serve
   as references in code TODOs.
3. the planNode constructor recursion must be unzipped; for each
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

This has been addressed in a separate RFC [Go infrastructure for algebraic data types](20170517_algebraic_data_types.md).

This includes its own definition language with some ancestry shared
with the protobuf syntax (in particular, tags are explicit so that
stored values are resilient to code changes).

## IR elements

The IR contains node types for expressions, as expected.
Salient aspects of expressions:
- column references are initially (just after parsing from SQL)
  unresolved names, but collapse into integer column references during semantic analysis.
- SQL's "IN" has two IR encodings, depending on whether one of the operands is a subquery.

The IR gives only minimal lip service to "statements". Because most of
the interesting semantics of "statements" are table expressions in the
IR, there is only one statement "Do" that says "run the plan for this
table expression and return its results". Every other statement can be
expressed as a composition of Do and some table expression. Some examples
are given in comments in the draft below.

# Drawbacks

This is a semi-large refactoring of the existing code.

# Alternatives

None knowns.

# Appendix: proposed naming of nodes

## AST: Type nodes

| Name | Description |
|------|-------------|
| (simple types) | represents the type of that name |
| TArray | array type |
| TTuple | tuple type |
| TTable | table type ("setof") |

## AST: Expression nodes

Leaf expressions:

| Name | Description |
|------|-------------|
| NumVal | exact numeric literal, evaporates after type checking |
| StrVal | exact string literal, evaporates after type checking |
| (other leaf datum nodes) | SQL values |
| Default | special value `DEFAULT` used in INSERT |
| Placeholder | for `$1` etc in prepared statements |
| ColName | named reference to a column from the current source, evaporates into IndexedVar after name resolution |
| IndexedVar | for `@1` etc - ordinal column references |

Subquery expression:

| Name | Description |
|------|-------------|
| Subquery | a table node used as expression |

Composite constructors:

| Name | Description |
|------|-------------|
| Tuple | tuple literal constructor |
| Array | array literal constructor |
| SingleRow | node wrapped around a subquery to extract its only row of results or a NULL tuple if there were no results |

Type assertions with optional run-time conversion:

| Name | Description |
|------|-------------|
| AnnotateType | type enforcement during type inference |
| Cast | type conversion at eval time |
| Collate | string collation |

Conditional expressions:

| Name | Description |
|------|-------------|
| Case | case when else (also used for `IF`) |
| IsOf | run-time type test |
| NullIf | `NULLIF(...)` |
| Coalesce | first non-NULL value |

Operations that combine values to produce results:

| Name | Description |
|------|-------------|
| Apply | function call (not aggregate, prev. FuncExpr) |
| BinArith | binary arithmetic operator (prev. BinaryExpr) |
| UnArith | unary aritmetic operator (prev. UnaryExpr) |

Comparison/logical expressions:

| Name | Description |
|------|-------------|
| And | conjunction |
| Or | disjunction |
| Not | logical negation |
| BinCompare | binary comparison (prev. CompareExpr) |
| Exists | non-empty predicate on a subquery |
| CompareFold | node for `<op> ALL (...)` / `<op> ANY/SOME(...)` / `IN` / `NOT IN`  |
| CompareOne | node for `value <op> (array/tuple/subquery)` when the lhs is a scalar/tuple and `<op>` is a simple comparison |

Notes:

- `ARRAY(subquery...)` should probably be represented using a cast of the subquery to array type instead of its own node.
- `(subquery...)` used as operand to some other expression node than CompareFoldS/CompareOneS, e.g. `X + (subquery...)` 
  should be represented as `SingleRow`.
- in other languages typically BinArith/UnArith/BinCompare are desugared to FunCall. In SQL it is advantageous to keep them separate
  because they need to be handily visible to optimizations.

Syntax nodes, disappear fully during desugaring:

| Name | Description |
|------|-------------|
| Aggregate | call to an aggregation function. Evaporates during de-sugaring of GroupAgg below. Prev. FuncExpr. |
| NotExists | empty predicate on a subquery, desugars into Not(Exists) |
| Between | BETWEEN/NOT BETWEEN, desugars into And/BinCompare/Not |

## AST: Table nodes

| Name | Description | Leaf node | Issues KV requests |
|------|-------------|-----------|--------------------|
| Insert | result of inserted rows | no | yes |
| Delete | result of deleted rows | yes (probably no after optimizations) | yes |
| Update | result of updated rows | yes (probably no after optimizations) | yes |
| Split | result of split keys | yes | yes |
| Show | used during parsing; evaporates into a chain of the nodes below | yes | n/a |
| Help | used during parsing; evaporates to Generator("function_help", ...) | yes | n/a |
| Select | used during parsing; evaporates into a chain of the nodes below | no | n/a |
| Values | produces rows by evaluating expression tuples | yes | no |
| Scan | result of reading data from a named table or index | yes | yes |
| Virtual | result of generating rows for a virtual table | yes | yes |
| Generator | produces rows using an internal function | yes | no |
| IndexJoin | result of looking up rows from a named table using a source of primary keys (probably Scan with index) | no | yes |
| Count | produce one row with the count of rows in the source (combine with Insert/Update to create a RowsAffected DDL stmt) | no | no |
| Filter | rows from source table matching a given predicate | no | no |
| Distinct | unique rows from source | no | no |
| GroupAgg | aggregation results from grouping the source using specified columns | no | no |
| Render | render given expressions using values from source | no | no |
| Rename | rename the columns of the source, evaporates after name resolution | no | no |
| Window | result of window functions on source | no | no |
| Sort | result of sorting rows from source | no | no |
| Join | result of a join between two sources | no | no |
| Ordinality | result of numbering rows from source | no | no |
| Union | union of results of two sources | no | no |
| Intersection | union of results of two sources | no | no |
| Except | set substraction of one source by another | no | no |
| Explain | inform about the structure of the node(s) below it | no | no |
| Skip | skip a given number of rows (OFFSET) | no | no |
| Limit | up to a given number of rows (LIMIT) | no | no |
| With | syntax node for `WITH a AS (...) ...`, evaporates after name resolution | no | no |

## AST: Statement nodes

| Name | Description |
|------|-------------|
| Do | run the table node and return all its results |
| DoAndReturn | syntactic sugar for, and normalized to, Do(Render(...)) - used for INSERT/UPDATE/DELETE |

The statement nodes (and not the clauses they contain) carry as
attribute the tag to return via the pgwire protocol. This needs to be saved in the statement node
because the clause inside the node may be simplified during optimization and lose its tag.

## Logical plan nodes

(Same list as in the current planNode zoo.)
