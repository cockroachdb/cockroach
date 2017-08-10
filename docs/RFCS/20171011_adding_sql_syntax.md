- Feature Name: Adding new SQL features with syntax - recommendations
- Status: active
- Start Date: 2017-08-06
- Authors: knz, Dan Harrison, Andrew Couch
- RFC PR: #17569
- Cockroach Issue: N/A

# Summary

This RFC recommends some general steps for projects that add features
visible through SQL syntax (either by adding new syntax or repurposing
existing syntax).

There are two aspects to the RFC:

- one that focuses on changes to the SQL grammar; in short, new
  syntactic features should be discussed via the RFC process and
  merged with the same PR that actually implements the feature.

- one that focuses on the general process of adding new language
  features; this serves as point-in-time reminder of the work
  currently needed.

Acknowledgement: This RFC complements/accompanies the [codelab for
adding a new SQL statement](../codelabs/00-sql-function.md) by Andrew Couch.

# Motivation

Until this point there have been instances in the past where the SQL
grammar was modified/extended to "prepare" for some
yet-to-be-implemented feature, thereby reserving a space in the set of
valid SQL statements for future use.

The main motivation for that approach was to ensure that a later PR
introducing said functionality would contain fewer commits / changes
to review, to ease the review process.

This RFC posits this motivation is misguided and instead the SQL
contributors should aim to avoid the following pitfalls:

- "Things happen" with the grammar over time - cleanups, refactorings,
  optimizations, renames, etc. This is routine work for the SQL team,
  but it's also work where the people doing the work need to have
  ownership of the SQL grammar. The problem with having an orphan
  syntax, especially without a comment about who's responsible for it,
  what it is to become and when, is that we can't really work around
  that in the grammar without risking breaking that person's plans.

- there is such a thing about the UI of a language, and that is its
  grammar. There is also such a thing as UI design, which ensures a
  number of properties:

  1) that the UI is consistent
  2) that it's orthogonal
  3) that it is extensible at low cost
  4) that it's well-documented and, most important for us
  5) that we don't run the risk of defining something that will
     conflict with some industry standard later (such as a standard
     SQL feature *we* may not have heard of yet but our customers will
     want us to look at).

  Granted, SQL is not really "pleasant" to look at but it has some
  internal structure and general patterns regarding extensibility that
  have been adopted as the result of 40 years of accumulated
  wisdom. For example, the first keyword in a statement has semantic
  properties all over the place in the language. One cannot just add a
  new statement prefix and not expect *lots* of work; whereas adding
  an alternate form for an existing prefix is relatively harmless.

- the natural progression of this practice we've started already
  would be a grammar definition that is fostering a large population of
  orphan syntax without functionality, created for projects abandoned
  before completion. We do not have this problem now, but from
  experience this just happens with team/company growth unless we're
  unrealistically careful (in other words, in this author's experience
  this really is a problem waiting to happen).

# Mindset

A contributor to CockroachDB often has good reasons to suggest
a SQL language extension.

One should always approach this task with a clear mindset that
distinguishes the *mechanisms* which deliver the desired feature from
the *interface* presented to users to control it.

The SQL syntax is the "interface". The code behind the syntax is the
mechanism. We can have multiple interfaces for the same mechanism:
either two or more ways to access the mechanism in SQL directly, or
other interfaces (e.g. an admin RPC) that enable access to the
mechanism besides SQL.

**As a matter of good engineering, it is always useful to discuss and
define the mechanisms separately from the design of the interfaces.**
(And preferably mechanisms no latter than interfaces.)

Soliciting input on both is equally important. That's why both should
undergo the RFC process.

*If your functionality mainly "takes place" elsewhere than the SQL
layer but you still need a SQL interface, be sure to involve the SQL
team in the SQL-related discussion for the interface part of your
RFC.*

# Case for really reserved syntax

This RFC proposes to delay merging syntax additions until the change
that actually implements the functionality.

We foresee three accepted and documented exceptions to this policy.

## Standard SQL syntax

We can reserve syntax that is specified in the SQL standard but
currently unsupported in CockroachDB. This provides us a convenient
way to ensure we don't introduce conflicting syntax while also giving
us a natural mechanism to inform the user that, yes, we know that's a
reasonable thing to ask, and that there is GH issue where they can see
why we don't support it yet and perhaps provide arguments to influence
our roadmap.

## Features for sole consumption by CockroachDB internals

We can reserve syntax also when a new mechanism RFC has been accepted
which benefits from exposing common functionality to different parts
of CockroachDB, *but not client applications*, via a SQL syntax
extension.

In this case, the contributor should be mindful that a feature that is
once considered "internal" may be serendipitously found later useful
by end-users; to prepare for this eventuality we must pave the road
to "publicize" a feature once internal:

- the SQL syntax for the internal feature should still follow the
  principles presented in "Choice of syntax" below.
- the new syntax contains the word "INTERNAL" or similar, to denote
  explicitly in the syntax the feature is not meant for public
  consumption.
- the new syntax is fully hidden from user-facing documentation.

Preferably, the code should also contain provision to make the syntax
invalid for regular client sessions, e.g. by requiring special
permissions or a planner flag.

## Multi-person or multi-stage projects

We can reserve syntax also when all the following conditions hold:

- a new mechanism RFC in demand of dedicated syntax has been discussed
  and resources have already been allocated to start the work (i.e.
  even if the RFC may not yet be finalized, the project has a green
  light to proceed);
- one or more candidate user-facing SQL interfaces have been discussed
  in an RFC (possibly bundled with the corresponding mechanism RFC).
- the MVP (minimum viable product) for the functionality requires
  intervention from two or more contributors, or requires spreading
  across multiple layers of work (= multiple PRs), or both.
- the contributors deem it preferable to establish a common set of
  tests using the new SQL syntax upfront of the work that needs to be
  done.

In this case, adding new syntax is acceptable, given the following conditions:

- the new syntax contains the word "EXPERIMENTAL" either standalone or
  as prefix to some keyword (new syntax rules) or identifier (new
  built-in functions) (e.g. `experimental_uuid_v4()`, `ALTER TABLE
  EXPERIMENTAL ...`)
- the new syntax is properly documented, yet marked clearly
  in documentation as a feature that is subject to change without
  notice.
- it is still possible to build and run CockroachDB successfully in
  production (albeit without full support for the new feature) if the
  corresponding grammar/built-in functions are elided from the source
  code.

# Structure of a proposal

In CockroachDB SQL features will need work at multiple levels,
touching the various architectural components discussed in the
[SQL architecture document](../tech-notes/sql.md).

An RFC planning to add a new SQL feature would therefore do well to
outline and clearly distinguish the following aspects:

- Mechanism:
  - Specification of input parameters
  - Specification of desired behavior
  - Logical planning changes (if needed)
  - Transaction logic changes (if needed)
  - Schema change logic changes (if needed)
  - Updates to the SQL execution engine(s) or CCL interface
  - Updates to other parts of CockroachDB (if needed)

- Interface:
  - Specification of lexical and syntactic elements
    - Using examples!
  - Definition of new/extended Abstract Syntax representations (if needed)
  - Desugaring to existing abstract syntax (highly recommended, if possible)
  - Typing and normalization rules
  - **Envisioned documentation: how do we explain this?**

# Choice of syntax

Somewhere in the process of writing about those things comes the
question: *what should the SQL interface be?*

Whether the author already has a clear idea about what they
want/recommend or whether they don't, the RFC should at least
cursorily examine past work in other SQL engines and suggest how other
SQL engines have offered similar functionality (if at all). Then two
situations can arise:

- either similar functionality is available in one or more other SQL engines:
  - if similar functionality has already been *standardized*, then just use that, unless
    strongly argued against in the RFC.
  - is it possible to reuse the same syntax as some other engine?
    - possible to use as is: use just that, unless strong arguments against.
    - possible to use with minor alterations: propose reuse, but also suggest alternatives.
    - possible to use with major alterations: suggest reuse, and emphasize alternatives.

- no similar functionality *as a whole* is available elsewhere:
  - does the functionality "group" with other existing SQL statements?
    - if so, reuse at least the first keyword and the general structure
  - does the functionality create / delete entitites?
    - do not forget about IF EXISTS / IF NOT EXISTS
  - does the functionality take optional parameters?
    - consider WITH
  - does the functionality need to input data from a relation/query?
    - consider FROM

In general:

- if a *part* of another SQL statement corresponds more or less to
  what your new envisioned syntax requires, consider reusing the same
  syntactic structure. This is moreso true if there are two or more
  other SQL statements that have these commonalities.
- If possible, reuse existing keywords rather than defining new
  keywords, particularly if the new keyword would have to be reserved.

# New abstract syntax or desugaring?

During parsing the code translates the input text into an abstract
syntax tree (AST). Different node types carry different semantics.

Sometimes a new feature with different syntax can *reuse* existing AST
nodes; we (compiler community) say that the new syntax is "sugar" for
a semantic construct that was already valid in principle.

For example, the syntax `a IS UNKNOWN` can be desugared into `a IS NULL`,
`SHOW ALL CLUSTER SETTINGS` can be desugared into `SHOW CLUSTER SETTING "all"`, etc.

**In general, the fewer the AST nodes the better.** Each new AST node
multiplies the work by the number of algorithms/transforms in the SQL
middle-end layer.

Sometimes, you can't reuse an existing AST node as-is, but it is
possible to update/enhance it so that it becomes reusable both for the
new features and the statement(s) that already use it. Consider doing
that instead of adding a new AST node.

And *in general it is not required that a given SQL
input text can be parsed and its AST pretty-printed back to exactly
the same SQL input*. Desugaring is *good*, use it whenever possible.

# Composability and generality

Even though *you* may only have a few use cases in mind to use the new
feature being proposed, try to not restrict/tailor the syntax and
semantics of the SQL interface to just these use cases.

For example, suppose you are designing a feature to "import CSV data into CockroachDB":

- Ad-hoc, use-case-specific, not general: `IMPORTCSV FILE <filename> INTO <tablename> USING DELIMITER <string>`
- Slightly more general: `IMPORT <format> FILE <filename> INTO <tablename> [WITH <option>...]`
- Even more general: `IMPORT [INTO] <relation> <format> DATA (<URL>...) [WITH <option>...]`

General principles:

- make a feature as general and orthogonal as possible given the two points below;
- you may only support a sub-set of the cases in your MVP/PoC (e.g. just `format` = `CSV`);
- do not make proposals that prevent us from changing our mind later, e.g. by promising
  generality in a domain where there is no 100% upfront confidence that all the possible
  cases make sense.

# Code highlights / pitfalls

- Do not forget `parse_test.go`

- Do not forget contextual inline help. Check out `sql/parser/README.md`.

- If your feature needs to use SQL expressions:
  - try at all costs to avoid changes or contraints to the typing system!
  - look at how other statements use `analyzeExpr()` and do the same;
  - use `RunFilter()` for conditions instead of evaluating manually and comparing
    the `DBool` value.

- If your feature needs to support placeholders:
  - do not forget to extend `WalkableStmt` if you're adding a new statement type;
  - add a placeholder test in `pgwire_test.go`.

- If your feature is adding a new SQL statement type:
  - what should its PG tag be in the pgwire protocol? Are clients going to notice/care?
  - be mindful about the difference between DDL and non-DDL statements

# Alternatives

- Not do anything - makes the work of the SQL team harder.
- Delegate all syntax choices to a specifically mandated committee - not scalable.

# Unresolved questions

None.
