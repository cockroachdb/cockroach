- Feature Name: Postgres Syntax Compatibility
- Status: draft
- Start Date: 2017-04-16
- Authors: Matt Jibson
- RFC PR: [#14968](https://github.com/cockroachdb/cockroach/pull/14968)
- Cockroach Issue:

# Summary

This proposal is to define the specific goals of the two syntax types
(traditional and modern) so we can correctly decide how to triage parsing
differences between CockroachDB and Postgres, Spanner, or other databases.

# Motivation

The differences in syntax types (`traditional` and `modern`) in CockroachDB
are not documented and have no defined goals or reasons for existing or
differing. The unwritten reasons are that traditional is the old Postgres
style and modern is the new Spanner style. However, we have already
made some choices to differ from Postgres ([integer division returning
decimals](https://github.com/cockroachdb/cockroach/pull/3308), for example)
because we thought the Postgres syntax could be improved. This style worked
fine for a while.

With our recent goal of supporting various ORMs, these previous
decisions become more complicated. We are advertising ourselves
as an implementer of the Postgres protocol and syntax, and we
mostly do. There is some syntax we do not yet support, and we
rightly return errors for that. However we now have various ([integer
division](https://github.com/cockroachdb/cockroach/pull/3308), [bitwise xor
(~) operator](https://github.com/cockroachdb/cockroach/pull/14908), [shift
argument modulo](https://github.com/cockroachdb/cockroach/issues/14874)) query
expressions that are known to return different results that Postgres. In all
of those cases we decided to differ because we believe the Postgres syntax
to be flawed.

Meanwhile, the modern syntax. It is undocumented, but it appears to
differ in 3 ways from the traditional syntax: identifiers, strings, and
comments. These all occur in the lexing phase of the parser. Our current
implementation does not allow us to parse differently (for example, change
the precedence of the `^` operator which is exponent in Postgres and xor
in Spanner). Some syntax we had was inspired from Spanner, but because it
differed too much from Postgres we had to remove or change it ([precedence
of |, #, &](https://github.com/cockroachdb/cockroach/pull/14944), [^ is
power instead of xor](https://github.com/cockroachdb/cockroach/pull/14882)).

Overall we are moving away from both the Postgres and Spanner syntaxes and
making our own. In our support of ORMs, we will have to document an ongoing
list of differences between us and Postgres, and we will never be able to
well support the Spanner syntax.

In order to resolve these problems, this is a proposal to:

1. Remove `modern` and the set syntax statement. We will only have
one syntax/dialect going forward.
2. Document the existing known differences between Postgres and Cockroach. That
is, for identical input that produces differing output, we will document that
this happens. This is to provide users migrating from Postgres to Cockroach
with a higher degree of certainty that their application will continue to
work correctly.
3. Proceed as we currently do when we discover differences on a case-by-case
basis. There is some disagreement about if we want to provide "as close as
we can reasonably achieve" or "we target Postgres compatibility when it is
necessary for ORM compatibility and technically achievable within the design
of CockroachDB". Instead of looking for consensus on that, we will use our
existing process of discussing these issues in PRs. When we do decide to
deviate from Postgres, we won't do so in a way that makes it difficult to write
queries that function identically in both Postgres and Cockroach. For example,
differing operator precedence can easily be rewritten with parenthesis.

# Detailed design

Since our current parser can only lex differently, it will have to be extended
somehow to plumb or branch on each syntax type. These differences cannot be
handled solely in the command blocks of sql.y because some differences involve
syntax precedence for the same operators. This will possibly involve splitting
sql.y into multiple files and some make logic to merge them together. It
seems like this would result in one large sql.y with nearly all common logic,
and two tiny yacc files with the few parsing differences.

Until that work is done, and for the 1.0 branch, we will assume `traditional`
is the most used syntax and thus change any differing expressions to be
more Postgres-equivalent.

# Drawbacks

Some users may encounter surpsising behavior when they discover that Cockroach
produces different results for identical queries. These users will silently
get unexpected results until they discover the differences. However since
they are moving to a completely new database and we will not be advertising
exact compatibility, this is acceptable.

# Alternatives

Make the default syntax be our new-style syntax, but also somehow provide a
syntax that is as close to Postgres as possible. This will allow for users
with large or complicated applications to have the lowest amount of work
possible when switching to Cockroach.

# Unresolved questions

How much compatibility do we want to provide to Postgres? We were not able to
come to agreement about if our goal should be "as close as we can reasonably
achieve" or "we target Postgres compatibility when it is necessary for ORM
compatibility and technically achievable within the design of CockroachDB".
