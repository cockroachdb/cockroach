- Feature Name: Deprecate the distinction between "traditional and modern syntax"
- Status: completed
- Start Date: 2017-04-16
- Authors: Matt Jibson, knz
- RFC PR: [#14968](https://github.com/cockroachdb/cockroach/pull/14968)
- Cockroach Issue: [#15064](https://github.com/cockroachdb/cockroach/pull/15064)

# Summary

This RFC discusses the background of the distinction between "traditional
and modern syntax" in CockroachDB pre-1.0, and discusses how to move forward
from there. The short conclusion of the proposal is to reduce the code to
support just one dialect for now.

# Context

Prior to 1.0, CockroachDB supported two dialects, also known as "syntaxes",
called `traditional` and `modern`.  The differences between these two
(`traditional` and `modern`) were not documented. The historical motivation
for their existence was to offer compatibility with Postgres' SQL dialect
via `traditional` and Spanner's SQL dialect via `modern`. This motivation
predates the public release of Spanner, hence the obfuscated names.

When this design direction was taken, there was no visible distinction in
the SQL semantics supported by CockroachDB (CockroachDB was implementing the
common subset between the two) so the only visible difference was lexical
structure, and the word "syntax" was used throughout the code (and in the
user-visible `set syntax`) to name the two dialects.

# Motivation for this RFC

## Compatibility with Postgres' SQL dialect

As time went by, CockroachDB came to implement more semantic features
that are specific to Postgres' SQL dialect, but beyond that we even
implemented some that are specific to CockroachDB.  For example we have
already made some choices to differ from Postgres ([integer division
returning decimals](https://github.com/cockroachdb/cockroach/pull/3308),
for example) because we thought the Postgres semantics could be improved.
As these CockroachDB-specific improvements didn't seem to break compatibility
with PostgreSQL clients, the work continued without conscious reflection on
what level of compatibility CockroachDB really aims to attain with either
Postgres or Spanner.

With our recent goal of supporting various ORMs, these previous decisions
(or lack thereof) become more complicated. We are advertising ourselves
as an implementer of the Postgres protocol and syntax, and we mostly
do. There are some syntax or featurs we do not yet support, and we rightly
return errors for that. However we now have various features ([integer
division](https://github.com/cockroachdb/cockroach/pull/3308), [bitwise xor
(~) operator](https://github.com/cockroachdb/cockroach/pull/14908), [shift
argument modulo](https://github.com/cockroachdb/cockroach/issues/14874))
that are known to behave differently (performance-wise or in resource usage)
or return different results that Postgres.

When we have decided to differ, it was because we either believe we learned
from Postgres' past mistakes and want to do better, or because CockroachDB
is targeting a different set of use cases where different behavior/semantics
will match user expectations better.

Nevertheless, in our effort to support ORMs, we are also constantly comparing
the output of CockroachDB with that of Postgres' to maximize compatibility
with existing client code. Any choice we make in CockroachDB that diverge
from PostgreSQL become a point of attention when we compare this output. At
worst it may become a point where client applications will need to care about
the difference. All in all, **we do not yet but we will have to document** an
ongoing list of differences between CockroachDB's and Postgres' SQL dialects,

## Deprecating the "modern syntax"

All the while this evolution took place, there was still plenty of code in
CockroachDB to support "modern syntax".  Although this feature was intended
to provide a modicum of *syntax* compatibility with Spanner, it even fell
short at achieving that, and instead merely provides an alternate *lexer*
for identifiers, strings, and comments.

Our current implementation does not allow us to *parse* differently
(for example, change the precedence of the `^` operator which
is exponent in Postgres and xor in Spanner). Some syntax we
had was inspired from Spanner, but because it differed too much
from Postgres we had to remove or change it ([precedence of |, #,
&](https://github.com/cockroachdb/cockroach/pull/14944), [^ is power instead
of xor](https://github.com/cockroachdb/cockroach/pull/14882)).

No effort was ever made to actually test compatibility between CockroachDB
and Spanner for clients. Given the lack of demand for such compatibility,
and the observation that a significant proportion of Spanner advocates are
clearly aware of CockroachDB, it does not seem necessary to us at this point
to invest into increasing compatibility with Spanner to acquire mindshare.

Therefore, the code in CockroachDB that aims at (and fails to achieve)
Spanner compatibility **can simply be removed**.

# Proposal

This is a proposal to:

1. Remove support for "`modern` syntax" and the `set syntax` statement. We
will only have one syntax/dialect going forward.

2. Document the existing known differences between Postgres' and CockroachDB's
dialects. That is, for identical input that produces differing output, we will
document that this happens. This is to provide users migrating from Postgres
to Cockroach with a higher degree of certainty that their application will
continue to work correctly.

3. Proceed as we currently do when we discover differences on a case-by-case
basis. There is some disagreement about if we want to provide "as close as
we can reasonably achieve" or "we target Postgres compatibility when it is
necessary for ORM compatibility and technically achievable within the design
of CockroachDB". Instead of looking for consensus on that, we will use our
existing process of discussing these issues in PRs. When we do decide to
deviate from Postgres, we won't do so in a way that makes it difficult to write
queries that function identically in both Postgres and Cockroach. For example,
differing operator precedence can easily be rewritten with parenthesis.

# Drawbacks

Some users may encounter surpsising behavior when they discover that
CockroachDB produces different results for identical queries. These users will
silently get unexpected results until they discover the differences. However
since they are moving to a completely new database and we will not be
advertising exact compatibility, this is acceptable.

# Alternatives

Make the default syntax be our new-style syntax, but also somehow provide a
syntax that is as close to Postgres as possible. This will allow for users
with large or complicated applications to have the lowest amount of work
possible when switching to CockroachDB.

## Detailed design for an alternate dialect

Since our current parser can only lex differently, it will have to be
extended somehow to plumb or branch on each dialect's grammar. These
differences cannot be handled solely in the command blocks of the current
`sql.y` because some differences involve syntax precedence for the same
operators. This will possibly involve splitting `sql.y` into multiple files
and some make logic to merge them together. It seems like this would result
in one large `sql.y` with nearly all common logic, and two tiny yacc files
with the few parsing differences.

Beyond this, any distinction in the semantics (typing rules, semantics of
built-in functions, semantics of virtual tables, optimization strategies)
would also need to be split in the code appropriately as well.

# Unresolved questions

How much compatibility do we want to provide to Postgres? We were not able to
come to agreement about if our goal should be "as close as we can reasonably
achieve" or "we target Postgres compatibility when it is necessary for ORM
compatibility and technically achievable within the design of CockroachDB".
