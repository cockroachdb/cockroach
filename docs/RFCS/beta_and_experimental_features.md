- Feature Name: Beta / Experimental features
- Status: draft
- Start Date: 2017-10-11
- Authors: knz, Diana Hsieh
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes to:

1) define the "Beta" status of new functionality that also has UX
   aspects (as opposed to new functionality that has no user-visible
   changes at all, for which this RFC is not applicable).
2) present recommendations as to how to present beta features to
   users: command-line flags, SQL language extensions, cluster
   settings, etc.

In a nutshell, *this RFC intends to make it unambiguous to users when
functionality is still considered as beta/experimental* and when the
CockroachDB team reserves itself the right to change UX aspects
without notice without running afoul of the expectation of backward
compatibility.

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
- the admin UI ("web console");
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
the product* a users may fail to learn altogether that a feature is
experimental, and erroneously start to develop an expectation of
backward compatibility.

This motivates guideliens to make the experimental nature of new
functionality more clear throughout the product, not only the
documentation.

# Guide-level explanation

When implementing new features first determine whether the feature
should be considered beta/experimental at all. For this refer to the
section below.

Then, if applicable, to display the experimental nature of the feature
to the UX elements explicitly:

- for the log messages: TBD?
- for command-line flags: add the marker `[EXPERIMENTAL]` in the help text (`--help`);
- for cluster settings: add the marker `[EXPERIMENTAL]` in the setting description (column "Description" in `SHOW ALL CLUSTER SETTINGS`).
- for session variables: add the marker `[EXPERIMENTAL]` in the variable description (column "Description" in `SHOW ALL`, also see below).
- for local commands in the CLI shell: add the marker `[EXPERIMENTAL]` in the online help (output of `\h`)
- for environment variables: add the word `EXPERIMENTAL` in the name of the environment variable.
- for SQL contextual help: add the word `[EXPERIMENTAL]` in the description.
- for the online documentation: be explicit that the feature is experimental.
- for the admin UI: some discreet UI element that marks the feature as experimental.
- for SQL language extensions:

  - use the word EXPERIMENTAL in the new syntax.
  - hide the extension from docs.

Gating the feature behind a cluster setting:

If possible, make the feature subject to a cluster setting that
enables it. This way a user that wants to run CockroachDB in
"pristine" condition can disable the feature cluster-wide.

The cluster setting should be named `experimental.name-of-the-feature`.

For example we have `experimental.importcsv.enabled` at the time of
this writing.

# What is a Beta / Experimental feature?

(Note: we use Beta/Experimental interchangably in this document. See Unresolved questions below.)

General guiding principles

If the feature has **no** user-facing changes:

- If the feature replaces a previous code path entirely (so there is
  no way to obtain the previous behavior), treat the feature as
  non-experimental. Of course during the design of the feature one
  should strive to avoid such replacements.

- If the feature proposes an alternative code path, and the previous
  one is still available, treat the feature as experimental and:

  a) verify that the choice of code paths is effectively configurable;
  b) verify this configuration choice is documented;
  c) verify that the change is duly documented so users know about the tradeoffs.

If the feature has user-facing changes:

- If the UX of the feature does not mirror 1-1 an equivalent feature
  in a competing product (e.g. for SQL feature parity), treat the
  feature as experimental.

- If it does not perform very similarly to how it performs in a
  competing product, then treat the feature as experimental. Check
  that the differences with the competing product(s) are properly
  documented in the RFC that introduces the new feature, and later in
  online docs.

- If it does perform very similarly, then:

  - if the change is "small" as defined in the contributor guide,
	and the feature mirrors 1-to-1 a SQL standard or a competing product,
	then consider the feature as non-experimental.

  - otherwise, consider the feature as
	experimental, unless the RFC that introduces the new feature makes a
	successful argument otherwise.

# Reference-level explanation

## Detailed design

- To prepare for the application of this RFC the session variable mechanism
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

- use the word `experimental` in the *name* of the flags / settings instead
  of their description. Problems with this approach:

  - if the feature evolves beyond the experimental phase to "stable"
	without change, the client apps / deployments must still be
	updated to use the new name. We want a path to promote a feature
	to stable without disruption for early users.

## Unresolved questions

- Is it reasonable to keep the terms "Beta" and "Experimental"
  synonymous for now?

- What is the list of features that are already implemented but still
  currently considered experimental, and which thus need to be made more
  explicitly so according to this RFC?
