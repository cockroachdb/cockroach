- Feature Name:  Experimental features
- Status: draft
- Start Date: 2017-10-11
- Authors: knz, Diana Hsieh
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes to:

1) define the "Experimental" status of new functionality that also has UX
   aspects (as opposed to new functionality that has no user-visible
   changes at all, for which this RFC is not applicable).
2) present recommendations as to how to present beta features to
   users: command-line flags, SQL language extensions, cluster
   settings, etc.

In a nutshell, *this RFC intends to make it unambiguous to users when
functionality is still considered as beta/experimental* -- useful in at least the following contexts:

- when the CockroachDB team reserves itself the right to change UX
  aspects without notice, and without running afoul of the expectation
  of backward compatibility.
- for features that don't "make data easy," it's better for us to
  clearly mark them as experimental so as not to hurt the perception
  of our brand.

Out of scope: "internal" experimental features that only support
another feature but are entirely invisible to users.

# Motivation

In CockroachDB a single feature may pop up in the UX surface
area in multiple locations, including but not limited to:

- command-line flags;
- environment variables;
- cluster settings;
- session variables;
- local commands in the `cockroach sql` interactive prompt;
- the SQL language that can be used both in `cockroach sql` and any client app;
- SQL contextual help;
- the online documentation;
- the web UI (sometimes called "web console" by users);
- log messages.

For each location where features are visible to users, a user
discovering the feature may (obviously!) start to use it, and start
*expecting* the feature to *remain available* in the future. Since
CockroachDB also advertises backward compatibility across releases,
this expectation seems reasonable at first.

Yet, when new features are added to CockroachDB, there is a period of
time where the UX aspects of the feature may not be stabilized yet: we
may not be certain yet that the UX choices are good, or perhaps early
users will provide feedback that change our UX choices. Thus, the
CockroachDB team may want to preserve the ability to change the UX
aspects without violating the promise of backward compatibility.

This is analogous to the process of "API stabilization" when a library
or online services adds a new feature -- the feature is initially
released, then early adopters will try it out, and then based on the
feedback the API may change slightly before it is stabilized and
becomes "frozen" for the purpose of defining backward compatibility.
In this initial phase, the new feature/API is typically marked
"experimental" in its documentation.

In contrast to libraries or online services, however, the "API" of
CockroachDB has a large surface area, and there is not a single point
in the product nor the documentation where "features" are identified
as a bullet list, where it would be convenient to add the text
"experimental" next to their description.

More to the point, a SQL database is typically a product where users
are likely to discover features via experimentation as opposed to
checking the documentation beforehand. Without a clear mention *within
the product*, users may fail to learn altogether that a feature is
experimental, and erroneously start to develop an expectation of
backward compatibility.

This motivates guidelines to make the experimental nature of new
functionality more clear throughout the product, not only the
documentation.

# Guide-level explanation

When implementing new features, first determine whether the feature
should be considered beta/experimental at all. For this refer to the
section below.

Then, if applicable, clarify the experimental nature of the feature
in the UX elements explicitly:

- for the log messages: left out of scope in this RFC. We are not
  considering marking features as experimental when they touch log
  messages, as we are not currently providing any guarantees about log
  formatting / messages.
- for command-line flags: add the marker `[EXPERIMENTAL]` in the help text (`--help`);
- for cluster settings: add the marker `[EXPERIMENTAL]` in the setting description (column "Description" in `SHOW ALL CLUSTER SETTINGS`). Also see below.
- for session variables: add the marker `[EXPERIMENTAL]` in the variable description (column "Description" in `SHOW ALL`, also see below).
- for local commands in the CLI shell: add the marker `[EXPERIMENTAL]` in the online help (output of `\h`)
- for environment variables: add the word `EXPERIMENTAL` in the name of the environment variable.
- for SQL contextual help: add the word `[EXPERIMENTAL]` in the description.
- for the online documentation: be explicit that the feature is experimental.
- for the web UI: some discreet UI element that marks the feature as experimental.
- for SQL language extensions:

  - use the word EXPERIMENTAL in the new syntax.
  - hide the extension from "stable" docs (make them only appear in
    the current "dev" version docs).

Gating the feature behind a cluster setting:

If possible, make the feature subject to a cluster setting that
enables it.

The cluster setting should be named `experimental.name-of-the-feature.enabled`.
Make it default to `false`.

For example we have `experimental.importcsv.enabled` at the time of
this writing.

When an experimental feature becomes non-experimental, the setting
that controlled it will remain (until some later version, e.g. the
next major version), but it may become an error to attempt to set it
to false. This allows cluster setup/testing scripts to enable
experimental features that an application requires without breaking
when the features lose their experimental status".

Be mindful to distinguish:

- *enable / disable* settings that gate experimental features, these
  should be *named* with the prefix `experimental.`
- new tuning settings that configure the behavior of some feature (either
  experimental or not). These should be *described* (column
  "description") as `[EXPERIMENTAL]`. The rationale is that when the
  feature transitions from experimental to non-experimental, the users
  will want to preserve their configuration for that feature. So the
  configuration parameter, if set via a cluster setting, must remain
  valid.

# What is an Experimental feature?

If the feature has **no** user-facing changes:

- If the feature replaces a previous code path entirely (so there is
  no way to obtain the previous behavior), treat the feature as
  non-experimental.

- If the feature proposes an alternative code path, and the previous
  one is still available, treat the feature as experimental and:

  a) verify that the choice of code paths is effectively configurable;
  b) verify this configuration choice is documented;
  c) verify that the change is duly documented so users know about the tradeoffs.

If the feature has user-facing changes:

- If the feature is CockroachDB-specific (doesn't aim for
  compatibility):
  
  - if the UX of the feature falls short of our standards / QA at the
    time of release, treat the feature as experimental.

  - otherwise, treat as non-experimental.

- If the feature aims for compatibility with another product:

  - if the part of its UX that aims to correspond to an equivalent
    feature does not mirror 1-1 the surface aspects (functional) of a
    competing product, treat the feature as experimental.

  - if it does not match existing user expectations (e.g., those
    expectations developed via a competing product) about how it
    should perform at an extra-functional level (e.g., performance),
    then treat the feature as experimental. Check that the differences
    with the competing product(s) are properly documented in the RFC
    that introduces the new feature, and later in online docs.

  - if it does match user expectations, then:

    - if the change is "small" as defined in the contributor guide,
      and the feature mirrors 1-to-1 a SQL standard or a competing
      product, then consider the feature as non-experimental.

    - otherwise, consider the feature as experimental, unless the RFC
      that introduces the new feature makes a successful argument
      otherwise.

# Reference-level explanation

## Detailed design

- To prepare for the application of this RFC, the session variable mechanism
  (`pkg/sql/vars.go`) must be extended to support a "description" field, like
  cluster settings already have.

- Other features currently experimental at the time of this writing must be
  reviewed and possibly documented as such in all their UX surface area
  (e.g. IMPORT CSV, the experimental SQL statements, maybe
  others. Also see unresolved questions below.)

- The RFC on SQL syntax changes must be updated accordingly.

No other technical changes needed.

## Drawbacks

None known.

## Rationale and Alternatives

The motivation section outlines the reason why "do nothing" is not a
viable alternative: we do want to commit to backward compatibility and
*also* want to keep some wiggle room and release new features before
their UX is finalized; so we must find a way to carve out new features
away from the scope of backward compatibility.

Some alternatives that were considered:

- design and implement the feature "as finished" and only gate it behind a
  `experimental` cluster setting. Problems with this approach:

  - some features are not amenable to cluster settings, e.g. flags
    that control the behavior of `cockroach start` or `cockroach sql` when
    the server is not active yet;

  - the DBA may know about the experimental nature of the feature,
    because they set the cluster setting, but then developers with
    access to the database may start discovering the feature after
    it's enabled and not understand that the feature is experimental,
    because nothing else would tell them.

- design and implement the feature "as finished" and only document it
  in the online docs. Problems with this approach:

  - users may discover features without reading the docs and develop a
    wrong expectation.

## Unresolved questions

- What is the list of features that are already implemented but still
  currently considered experimental, and which thus need to be made more
  explicitly so according to this RFC? Noted during review:

  - IMPORT CSV
  - disqSQL temporary storage
  - stats-based rebalancing
