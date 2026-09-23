- Feature Name: Deployment-Agnostic Multi-tenant OX
- Status: draft
- Start Date: 2022-08-12
- Authors: knz with help from ben
- RFC PR: [#86736](https://github.com/cockroachdb/cockroach/pull/86736)
- Cockroach Issue: [CRDB-18904](https://cockroachlabs.atlassian.net/browse/CRDB-18904)

# Summary

We propose to create an Operator eXperience (OX) for a multi-tenant
CockroachDB which erases externally-visible differences between two
possible deployment styles:
- shared-process: 1 process serves KV, SQL for system tenant, and SQL for one or more secondary tenants.
- separate-process: 1 process serves KV and SQL for system tenant, and other process(es) serve SQL for secondary tenants.

As a result of this work, it becomes possible to switch between
deployment styles without changes to client app configuration,
orchestration, administration playbooks or security
configuration/rules.

The motivations for this work includes:
- It makes it possible to switch randomly between deployment styles
  during QA and testing inside CRL (e.g. via roachprod / roachtest),
  without changes to test infrastructure or frameworks, as a form of
  metamorphic testing. This will help safeguard our commitment to
  support our features equally well in both cases.
- It enables a future where we can gradually onboard a customer to
  multi-process deployments without requiring a lengthy review/upgrade
  of their orchestration and configuration.

We will achieve this by introducing support for two-process
deployments (without CC Serverless' sqlproxy component), then changing
the default behavior of `cockroach start`, `start-single-node` and
perhaps `cockroach demo` to conditionally start two-process
deployments automatically.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Strategic background](#strategic-background)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [Resulting OX common to both](#resulting-ox-common-to-both)
    - [How we would achieve the common interface in shared-process deployments](#how-we-would-achieve-the-common-interface-in-shared-process-deployments)
    - [How we would achieve the common interface as a separate-process deployment](#how-we-would-achieve-the-common-interface-as-a-separate-process-deployment)
    - [Sequencing, V1: prototype inside the Docker image](#sequencing-v1-prototype-inside-the-docker-image)
    - [Sequencing, V2: extend `cockroach start`, `start-single-node`, `demo`](#sequencing-v2-extend-cockroach-start-start-single-node-demo)
    - [Drawbacks](#drawbacks)
    - [Rationale and Alternatives](#rationale-and-alternatives)
        - [Alternative 1: "do nothing"](#alternative-1-do-nothing)
        - [Alternative 2: rely on our public Kubernetes operator](#alternative-2-rely-on-our-public-kubernetes-operator)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Strategic background

Please refer to [this internal document](https://go.crdb.dev/mt-strategy-231) for an explanation of the overall strategy behind this work.

# Motivation

We would like to create an environment where both shared-process
multitenancy (as envisioned for CC Dedicated) and separate-process
multitenancy (as used in CC Serverless) are equally easy to test and
experiment with, and possibly to place in the hands of customers.

Today, this is difficult, because the deployment modes differ in the following ways:
- The commands to run (in orchestration) are different.
- The networking interfaces (TCP ports) are different.
- Separate-process also needs to deploy the sqlproxy component which is CC-only.

We would like a solution which erases these differences so that a user
can experiment switching between deployment styles without changing
their orchestration or their client app configuration.

# Technical design

We propose to change the behavior of the CockroachDB server commands
to support both a shared-process deployment and a multi-process
deployment, with an option to change between them without changing CLI
flags, via an env var.

The actual selection of deployment mode would be as follows:
- During development, would default to randomize (metamorphic testing).
- In production, would default to shared process.
- Can be overridden for testing/evaluation.

## Resulting OX common to both

- 1 application tenant for user workloads + 1 system/admin tenant for SREs.

- Consistent networking interface using 3-4 TCP ports (from just 2-3 today):
  - 1 gRPC TCP port for node-node connections, and CLI administrative
    commands

    Default: 26257, configurable
  - 1 "backward-compatibility" SQL port with automatic tenant
    selection, includes access to system/admin tenant (like before)
    and also app secondary tenant (NEW).

    Default 26257 (can be shared with gRPC port), configurable
  - 1 HTTP port with automatic tenant selection, include access to
    APIs and DB Console for both system/admin tenant (like before) and
    app secondary tenant (NEW)
  - (NEW) 1 SQL TCP port for connecting exclusively to the application secondary tenant.

    Default: 27257, configurable. Optimized for performance.

- Shared command interface:
  - Same commands and CLI flags to start servers in either deployment mode.
  - Same commands/CLI to stop/restart servers and orchestrate upgrades.

## How we would achieve the common interface in shared-process deployments

The gRPC port would be configured as usual.

The main SQL/HTTP port would be configured as usual, but we would add
the technology proposed in [this other
RFC](https://github.com/cockroachdb/cockroach/pull/84700) to determine
which tenant serves incoming connections (based on client parameters
for SQL, or a cookie/header for HTTP).

We would open a secondary SQL listener that exclusively routes to the
app tenant.

The CLI and startup code would remain unchanged.

## How we would achieve the common interface as a separate-process deployment

Here we would add an alternative code path inside `cockroach start`,
`start-single-node` and perhaps `demo`. This new code path would take
care of:
- spawning a sub-process to the main command to serve the SQL for app tenant workloads.
- translate the outer CLI configuration into suitable CLI flags for the sub-process.
- coordinate the two processes such that whenever one terminates, the
  other one immediately terminates too (we need to bind their life
  cycles together).

In this mode, the top (parent) process would implement KV and the SQL
layer for the system tenant; and the bottom (child) process would
implement the SQL layer for the app tenant.

The TCP ports would be implemented as follows:
- The top process would implement gRPC and the hybrid SQL/HTTP ports.
  - When the incoming connection selects the app tenant, the SQL/HTTP
    listeners would forward (proxy) the connection to the bottom
    process.
- The bottom process would implement the 2nd SQL listener for the app
  tenant. This would be (slightly) more efficient than routing via the
  shared port, because it would avoid 1 network hop.

In this configuration, both processes would share a logging and TLS
configuration, resulting in the same externally-visible behavior as
the shared-process deployment.

The behavior for timeseries would be made agnostic using
the tools presented in [this separate RFC](https://github.com/cockroachdb/cockroach/pull/86524).

## Sequencing, V1: prototype inside the Docker image

In this V1, we would implement the separate-process orchestration as a
modification to the `entrypoint.sh` script that we already embed in
our Docker image. This means we would not need to introduce this
complexity inside CockroachDB directly. It would also be easier to
test.

This V1 would also only support a subset of the CLI flag interface,
exactly that subset currently in use at some key customers.

When splitting processes, both processes would run inside the Docker
container and would thus be completely opaque to the orchestration
layer around it.

## Sequencing, V2: extend `cockroach start`, `start-single-node`, `demo`

In this V2, we would embed the new separate-process logic inside the
CockroachDB code, ensuring that all the existing CLI flag surfaces of
CockroachDB results in coherent behavior when the process is split.

We would also perform more QA to verify that the behavior is correct
when customers orchestrate with e.g. SystemD or other system managers
(i.e. without using Docker).

## Drawbacks

Both the top and bottom process would run on the same VM.

This creates interference from the perspective of admission control.

We find this risk acceptable: this separate-process MVP would be
mostly a development and testing tool, and would not be used in
production (except possibly at customers where the resulting behavior
would be acceptable.)


## Rationale and Alternatives

### Alternative 1: "do nothing"

This alternative would preserve the OX complexity gap between
shared-process and separate-process, which would continue to make
separate-process deployments unattractive.

### Alternative 2: rely on our public Kubernetes operator

The situations (Eng QA/testing, some key customers) where we envision to use
shared-process/separate-process agnosticism do not yet use our K8s
operator.


# Explain it to folk outside of your team

TBD

# Unresolved questions

N/A
