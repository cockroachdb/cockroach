- Feature Name: Postgres Syntax Compatibility
- Status: draft
- Start Date: 2017-04-16
- Authors: Matt Jibson
- RFC PR:
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

The proposal here is to define the `traditional` syntax to be as close to
Postgres as we can reasonably achieve, and `modern` to be like Spanner. That
is, for the exact same input, we will attempt to produce the same results
as Postgres or Spanner. This allows for our own unique syntaxes like index
hinting to remain, since those do not appear in any Postgres ORM.

This would resolve all future questions about what to do about different
results for identical queries in CockroachDB and Postgres, and allow us to
easily document the differences between the two.

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

Limiting ourselves to Postgres' decisions from 20+ years ago will continue
to perpetuate those same problems. Using the Spanner syntax is not enough
of a reason for many people to change because it is not widely adopted. So
we may end up doing a lot of work supporting both for almost no benefit
since no one will use the Spanner syntax. People switching from Postgres to
CockroachDB will expect to be required to change some syntax for things to
work, so it's not unreasonable to show them the list of differences.

# Alternatives

We stick with our current story, but document all differences, however minute,
from Postgres. Again, these are not differences like "we don't support X
type", but differences where both Postgres and Cockroach accept exactly the
same syntax but return different results.

Another alternative is to have the default syntax be our new-style syntax, but
also somehow provide a syntax that is as close to Postgres as possible. This
will allow for users with large or complicated applications to have the
lowest amount of work possible when switching to Cockroach.

# Unresolved questions
