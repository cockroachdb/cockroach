- Feature Name: CockroachDB Feature Registry
- Status: draft
- Start Date: 2021-11-15
- Authors: Jordan Lewis
- RFC PR: 72800

# Summary

This RFC proposes the CockroachDB Feature Registry, a system for tracking
CockroachDB features that helps our developers safely add, test, and evolve
features in the database. The Feature Registry is a system of checks, tests,
documentation and warnings that:

- Nudges developers toward following the best practices for creating features
  in the codebase, like adding randomized testing, upgrade tests, telemetry,
  metrics, logs, feature flags, and so on.
- Helps developers classify the maturity of their features via a maturity
  matrix, which configures the strictness of the checks against a feature, and
  helps advertise features to users based on their maturity
- Reminds developers to register their new features in the registry as code is
  added to certain key packages

Why a feature registry? As CockroachDB grows, it becomes more complex. With
increased complexity comes increased risk of bugs due to unexpected feature
interaction and oversights. We've seen this cause bugs before. This proposal
aims to help developers avoid mistakes.

We'll aim to incrementally grow the Feature Registry, by introducing the
registry with just a few tests and checks at first, to see how and whether it
works out. Experimenting with the Feature Registry is low cost, because it
contains no runtime components.

We expect that the impact is improved developer onboarding and reduced bugs.

# Motivation

CockroachDB is a complex, highly inter-connected software system that demands a
high degree of reliability, rigor and testing for each of its features. As a
result, it’s very difficult for a single developer to get a new feature done
end-to-end on the first try, since most developers may not know or remember all
of the right things to test or build for their new feature.

This problem leads to defects, unfinished features, poor observability, and inconsistent UX.

For example, new features in CockroachDB should:

- Have telemetry counters or telemetry logs that help Product and Engineering
  assess the usage patterns and frequency of the new feature.
- Emit tracing when appropriate for debuggability and observability.
- Be exposed via virtual tables in SQL and the CockroachDB Console when
  appropriate.
- Have mutators or schema generators implemented for SQLSmith, TLP, Random
  Syntax, and other randomized testing, or metamorphic testing when
  appropriate.
- Be exposed via EXPLAIN or EXPLAIN ANALYZE when appropriate.
- Be exposed via Prometheus metrics when appropriate.
- Be tested in upgrade scenarios.
- Be tested in backup/restore scenarios.
- Be tested in CDC scenarios.
- Have feature flags when appropriate.
- Work in a multi-tenant deployment

How do we ensure that everyone looks through this list for their features
without imposing too high of a documentation, reading and reviewing burden?

Further, how do we teach new developers how to add new features in a complete
way?

# Technical design

The proposed solution is a system of self-registration, tests, and lints that
help developers learn or remember all of the points above via our ordinary CI
system. Rather than force developers to read through optional checklists that
are confusing, out of context, or unhelpful, we’ll develop a system that forces
developers to confront these questions as they do work, helped by the Go type
system and friendly test failure messages.

The resultant system will be an extensible way for our team to add new checks,
tests, and requirements for all features, as well as a set of guardrails,
guidelines, teaching tools and tests that ensure that new features are
well-tested, complete, and as bug-free as possible.

There are 3 main components to the Feature Registry:
- The Feature struct and the list of Features.
- Tests that scan the list of Features and, depending on each Feature’s
  attributes, ensures that developers have thought through all of the
  dimensions relevant to their Feature.
- Tests that scan the codebase to ensure that, conversely, all code in packages
  that should be registered by a Feature is in fact registered by a Feature.

## Feature definition and registration
The CockroachDB Feature Registry will be a Go package that defines a Feature
type and contains a list of Features.

The `Feature` type is a struct that contains fields that describe a feature. A
`Feature` contains description fields, which describe a feature and declare some
attributes about it, and link fields, which contain function or var pointers to
aspects of the Feature’s implementation.

An initial proposal for the `Feature` type is listed here.

```
// Feature represents a “feature”, a piece of work that should be
// tracked as a unit of telemetry, testing, logging, metrics, and
// feature flag.
type Feature struct {
       // Name identifies the feature.
       Name string
       // FeatureAttributes is a list of the attributes of this
       // feature, like “adds grammar”, “adds schema features”, “adds
       // builtin functions”, and so on.
       FeatureAttributes FeatureAttributes
       // Maturity represents the feature’s maturity. Can be
       // DEVELOPMENT, INCUBATING, MATURE. In DEVELOPMENT, checks
       // are disabled, but the FeatureFlag must default to false.
       // In INCUBATING, some checks are disabled.
       // In MATURE, all checks are enabled.
       Maturity Maturity

       // Mutators is a slice of randgen.Mutator that randomly
       // modifies a schema to include the feature.
       Mutators  []randgen.Mutator
       // Telemetry is a slice of telemetry.Counter that the feature’s
       // implementation increments when used.
       Telemetry []telemetry.Counter
       // FeatureFlag contains at least one method for enabling or
       // disabling this feature.
       FeatureFlag struct {
           // ClusterSetting is set if this feature is disableable
           // globally.
           ClusterSetting settings.BoolSetting
           // SessionSetting is set if this feature is disableable
           // on a per-session basis.
           SessionSetting sql.SessionVar
      }
       
       // ASTStatements is a slice of the tree.Statements that this
       // feature adds, if applicable.
       ASTStatements []tree.Statement
       // ASTExprs is a slice of the tree.Exprs that this feature adds
       // if applicable.
       ASTExprs []tree.Expr
       
       // VersionKey is the minimum clusterversion.Key that a cluster
       // must be upgraded to to use this feature, if the feature
       // requires a version gate.
       VersionKey clusterversion.Key

       // ExposedViaExplain indicates whether the feature is exposed
       // via EXPLAIN.
       ExposedViaExplain FeatureStatusItem
}

// FeatureAttributes is a list of attributes that features can
// expose.
type FeatureAttributes struct {
    // AddsGrammar indicates that a feature adds grammar and/or AST
    // nodes.
    AddsGrammar bool
    // AddsSchemaFeature indicates that a feature adds a feature to
    // persisted database schemas by adding fields to a Descriptor.
    AddsSchemaFeature bool
    // AddsBuiltins indicates that a feature adds SQL builtin
    // functions.
    AddsBuiltins bool
    ...
}

type FeatureStatus int
const (
    statusInvalid FeatureStatus = iota
    statusImplemented
    statusNotApplicable
    statusTODO
)

type FeatureStatusItem struct {
    Status FeatureStatus
    // If Status = statusTODO, this points to a GitHub issue ID
    // to resolve the TODO.
    GitHubIssueID int
}
```

When adding a feature to the database, developers will add a new Feature to a
list of Features in the feature registry package. An example of a new Feature
follows:

```
var PartialIndexes = Feature {
       Name: “PartialIndexes”,
       FeatureAttributes: AddsGrammar || AddsSchemaFeature,
       Mutators:  []randgen.Mutator{randgen.PartialIndexMutator},
       Telemetry: []telemetry.Counter{sqltelemetry.PartialIndexCounter},
       // There was no feature flag for Partial Indexes.
       FeatureFlag: nil,
       ASTStatements: nil,
       ASTExprs: nil,
       
       // The version key for Partial Indexes has already been
       // deleted, since it’s existed since before 21.1, but normally
       // we’d see something like clusterversion.PartialIndexes here.
       VersionKey: clusterversion.V21_1,

       Maturity: MATURE,
}
```

## Feature Tests

The feature registry package will contain a set of tests that ensure that, for
each registered Feature, the Feature contains sufficiently filled-out link
fields for the Feature’s maturity level and declared attributes.

Here are some initial test ideas, to start with:.

A DEV or INCUBATING feature must have a boolean FeatureFlag whose default is
false.

A MATURE feature that has the AddsSchemaFeature attribute must have a non-empty
Mutator slice that adds the schema feature to the SQLSmith schema generators.

An INCUBATING or MATURE feature must have a non-empty Telemetry slice.

A Feature that fails any tests will cause CI to fail. Each Feature test will
include a message that helps a developer understand how to implement the
missing Feature attribute correctly.

These tests, fields, and attributes will expand over time as teams encode more
requirements for new features in their database layers.

## Missing Feature Detection

To help developers and their team leads remember to add Features when adding
new features, there will be an additional suite of Missing Feature Detection
tests that attempt to notice when code has been added that should be in a
Feature but is not.

For example:

A TestASTIsCoveredByFeatures test will collect all implementations of tree.Node
and ensure that each is either in a Feature struct or is included in an initial
Allowlist of tree.Nodes that is populated when this test is invented (to avoid
a painful, pointless backfill process).

A TestDescriptorFieldsAreContainedWithinFeatures test will ensure that
newly-added fields to protobuf Descriptors are contained via Features that have
the AddsSchemaFeature attributes.

These tests will fail with an error message that explains to the developer
that, for this kind of code change, they must add a Feature. Adding a Feature
will lead them to discover the Feature maturity matrix along with the list of
tests, checks, and links that each Feature will have, and therefore teach the
developer what other kinds of missing work should go along with the Feature’s
implementation.


## Drawbacks

Being too overzealous with lints and checks could reduce the productivity of
engineers. We'll have to take care to make sure that the checks are lightweight
and actionable, and not too onerous.

## Rationale and Alternatives

An alternative was explored in an internal document entitled
[PR Checklists](https://docs.google.com/document/d/1laI5A5GdqvNiJs6SvtKNiGmoWXitfVJN1bSjtZNZdvQ/edit#)
which aimed to create nudges in pull requests based on the Checklist Manifesto
system. This alternative was less extensible and served as an inspiration to
this work. It was never implemented.

The other alternative is to do nothing, hoping that feature development in
CockroachDB is slowing, and gambling that we'll see fewer and fewer bugs caused
by cross-cutting feature development. This might not be a terrible alternative,
but doing nothing has costs in bug count and slower onboarding for our
developers.

# Unresolved questions

This RFC does not propose an exhaustive set of checks. We'll have to discover
the right checks as we create a prototype.
