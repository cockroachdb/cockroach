- Feature Name: Observability server
- Status: draft
- Start Date: 2021-02-22
- Authors: knz, davidh, tbg
- RFC PR: [#65141](https://github.com/cockroachdb/cockroach/pull/65141)
- Cockroach Issue: [#57710](https://github.com/cockroachdb/cockroach/issue/57710)

# Summary

This document outlines a product vision and **architecture** where we
would serve **CockroachDB observability using a separate server process
from the CockroachDB nodes** being monitored.

Today CockroachDB nodes are responsible for their own monitoring and
serving the DB console. When nodes or cluster ranges become
unavailable (or nodes crash), the monitoring systems, APIs and DB
console become unavailable too. Using a separate server component
would ensure the monitoring remains available when the nodes or
cluster are not.

*This proposal solves an ancestral misdesign in
CockroachDB: the monitoring APIs and dashboards should never have been
built within the db nodes. A monitoring system is worthless if it is
part of the system that it monitors.*

Using a separate observability server with its own local storage for
monitoring data would also make it possible to copy the entire
observability server as troubleshooting artifact. This would make the
'debug zip' command altogether obsolete and drastically shorten the
time required to retrieve and interpret artifacts during
troubleshooting.

Grouping monitoring data for multiple tenants inside a single
observability server also makes it possible to reliably serve DB-level
observability data and APIs to CC users, even when their SQL tenant
nodes have been scaled down to reduce CC infra costs.

Who would carry out this project: Observability Infrastructure, with
support from Server and KV teams.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Summary](#summary)
- [Motivation](#motivation)
- [Technical design](#technical-design)
    - [Overview](#overview)
    - [Current architecture](#current-architecture)
    - [New architecture](#new-architecture)
        - [Discussions about the architecture](#discussions-about-the-architecture)
    - [Debug zip replacement](#debug-zip-replacement)
    - [Deployment recommendations](#deployment-recommendations)
        - [In CC Intrusion](#in-cc-intrusion)
        - [In `cockroach start-single-node`  and `demo` for developers](#in-cockroach-start-single-node--and-demo-for-developers)
    - [Drawbacks](#drawbacks)
    - [Rationale and Alternatives](#rationale-and-alternatives)
- [Explain it to folk outside of your team](#explain-it-to-folk-outside-of-your-team)
- [Unresolved questions](#unresolved-questions)

<!-- markdown-toc end -->


# Motivation

This approach comes in response to the following Problems To Solve:

- CC Platform 21.2
  - Obj 2: Internal teams face friction in debugging Cloud issues due
    to lack of appropriate tools.  “we need to eliminate more internal
    friction and make it seamless for the TSE team to provide
    support.”

- Observability 21.2
  - Node or cluster unavailability impairs observability
  - DB Console is unreliable during a cluster outage making troubleshooting unhealthy clusters difficult
  - Debug.zips are slow to collect data and overly verbose leading to long troubleshooting times

- Security 21.2
  - Node removal or scale-down in CC Free/Shared-Tier cause loss of security artifacts


# Technical design

## Overview

This proposal decouples the following server components into a standalone process:

- Serving DB Console static artifacts (Javascript, images, fonts)
- Serving DB Console APIs over HTTP
- Serving the debug APIs

Additionally, the new server process would act as a cache for the
following artifacts, and retain copies of them for the "last 24
hours" (configurable):

- Current SQL metaschema
- SQL statement stats
- Problem ranges, hot ranges etc
- Goroutine dumps, heap profiles and CPU profiles
- Log files
- Time series DB ranges if available (e.g. not for serverless pods)

The observability data would be stored locally to the new server and
would not be stored in the cluster being monitored. Because the
observability data is read-only, it becomes possible to simply
instantiate the new server multiple times for reliability, without any
consensus protocols.

(NB: each instance pulls data to itself, independently from any other
intents. We do not need coordination. We do not care much about the
copies being out of sync because the data is still going to be useful
for troubleshooting anyway—as long as they maintain a timestamp of
their most recent successful sync.)

## Current architecture

Today (up to and including CRDB v21.1), the DB console, APIs and
monitoring data are served by CockroachDB nodes directly. The same
commands `cockroach start` and `start-single-node` run both DB nodes
and observability servers.

Inside the server process, the APIs are *implemented* using the gRPC
interface, then they are served to clients (or the front-end code of
the DB console) using a HTTP-to-RPC conversion component called
"gRPC-gateway".

In the serverless architecture, observability services (DB console &
APIs) are only served by the KV nodes, and individual tenants have no
access to observability data “at their level”.

## New architecture

A new command `cockroach monitor start` is introduced that only starts
the observability server (this would not be the only way to start the
service, see section "Deployment recommendations" below).

It accepts the same `--join` flag as `cockroach start` to learn about
actual DB nodes and SQL pod processes.  It has its own local storage
that can be configured with `--store`.

Inside the observability server process, the APIs are implemented
directly using a HTTP server.

To the maximum extent possible, we will try to reuse existing server
code inside CockroachDb. This reduces the amount of work required to
realize this project and maximizes protocol compatibility between the
observability server and the CockroachDB nodes.

The handlers of the HTTP APIs work as follows:

- Whenever the observability server has local data to serve the query
  itself, it uses that to respond to the client. This includes:

  - DB Console static assets
  - SQL metaschema
  - SQL statement stats
  - Problem ranges, hot ranges etc
  - Goroutine dumps, heap dumps, CPU profile dumps
  - Log files (until we remove log file retrieval from DB APIs)

- Whenever local data is unavailable, they forward the HTTP API request
  to one of the DB nodes, chosen randomly (see note below). This is used e.g. to
  enable/disable CPU profiling cluster-wide, enable the "logspy", etc.

Separately, in the background, each observability server periodically
fetches copies of the observability data from DB nodes.

Additionally, **the data on disk is partitioned with the cluster ID and
tenant ID as partitioning key**. This makes it possible to use a single
observability server for multiple clusters and multiple tenants. **The
authentication key passed in API calls determine which cluster and
tenant to retrieve data for.**

### Discussions about the architecture

**Note about the random node selection when data is missing:** we
often want to troubleshoot a particular node. It will thus be useful
if the obs server can forward a request to a specific node when given
e.g. a NodeID parameter in the request object.

**Note about authentication:**

- (DavidH) how would these queries handle authentication? Right now
  you can be blocked from accessing certain SQL data if you don't have
  the VIEWACTIVITY role, for instance. If the O11y server caches the
  response it must be in the context of a SQL user but it might not
  match with the role of the person accessing the endpoints.

  (knz) here's where we need to revisit our authn&authz story,
  especially to allow information bout role options to come in from
  outside (i.e. not from the db itself).
  In my view, the access would be made using a CC API token with a
  signature and containing a bearer token for the VIEWACTIVITY role
  option.

**Note about including per-range debug info**

- (PeterM) What about the per-range Raft debug info? That has been
  quite useful in debug.zip powered debugging, yet I think it might be
  quite expensive to be retrieving this state periodically in the
  background.

  (tbg) We can figure out ways that give the debug server a view of
  this reactively, based on interesting things happening. Seems fine
  to omit this in the first pass, since this is not actually something
  that becomes unavailable when the cluster does.

**Note about how the obs server becomes aware of the node membership**

- (tbg) All it needs is to be given at least one reference to the
  cluster, right? As long as it connects to one node, it will be able to
  use that node to resolve the other nodes. Or is there some desire to
  be resilient towards failures within CRDB?  This sounds as though the
  debug srv is pulling from the nodes, is it simply polling the gossip
  registry of live nodes to determine who to pull from?

  (knz) think we want the list of nodes to be passed explicitly and
  not use any auto-discovery. The particular reason why I care is
  because we want to not transport obs data across regions, we want
  one such server per region to preserve data locality. Auto-discovery
  of that topology would be hard.

  (tbg) So what's the default k8s deployment then? The debug srv gets
  a DNS balancer that lists only the region-local hosts?

  (knz) I assume so. Note that we don't have k8s configs that are
  multi-region. There's just 1 k8s config per region. And in that case
  it's simple, we could have 1 debug srv per k8s cluster that connects
  to all the pods within the k8s cluster.
  However for CC I think we want to be smarter and have these servers
  somewhere else, and also make them collect data across more clusters
  to share some costs?

## Debug zip replacement

Today the `debug zip` command is a CLI tool that performs SQL and RPC
requests to DB nodes to retrieve data. The debug zip command is slow
because it needs to serialize all the data over individual requests.
Additionally `debug zip` can impair cluster stability because the data
APIs that power `debug zip` accumulate data in RAM, and this RAM
consumption can cause nodes to crash during the zip retrieval.
Finally `debug zip` obviously doesn't work when nodes are down, or
when ranges are unavailable.

The observability server **makes the `debug zip` mechanism entirely
obsolete** (but see discussion below).  Indeed, the entire
observability server data (i.e. its own data directory) has all the
artifacts needed for troubleshooting, and we can simply take a
filesystem-level copy of the observability server to obtain all the
artifacts. This is much faster, impervious to DB nodes downtime or
range unavailability, and is impervious to cross-version
incompatibilities.  To use this filesystem-level copy, we would then
simply start a standalone server in our environment.

Most enterprise customers do not allow TSEs or other staff to access
the filesystem, so for UX simplicity we will also want to define a
HTTP API that does the same.  Ultimately, this should result in a
single (.zip) file that can be attached to a ZenDesk ticket, allowing
TSEs to view the customer's time-series data offline, e.g. by running
a copy of the observability server on their own machine.

**Discussion**

1. (Irfan) For large clusters, this could still be data in the order
   of gigabytes which makes transferring things to us over
   call/tickets painfully slow and error prone. How to deal with this?

2. (Irfan) What if a cluster needs help but did not run an obs server?
   (Or the obs server doesn't have up-to-date data?)

3. (AlexL) How would this work with PII? Would we have to always store
   the data in this server in scrubbed mode?

   (knz) I think that initially to be on the safe side we need to
   default to scrub by default. However, I can imagine a hybrid mode
   where the observability server is in a secure environment and
   connected to an authorization server, and determines who is
   accessing the data, and decides to redact or not depending on who
   is accessing.

## Deployment recommendations

### In CC Intrusion

The CC intrusion software must evolve to start observability servers
alongside CockroachDB nodes and configure the `--join` flag and TLS
certificates.

### In `cockroach start-single-node`  and `demo` for developers

To ease adoption, the sub-commands `cockroach start-single-node` and
`cockroach demo` continue to serve the observability systems using a
single process.

## Drawbacks

(Not discussed yet)

## Rationale and Alternatives

We need a separate process and separate storage to ensure that the
monitoring data remains available when nodes / clusters / ranges are
not available or running any more.

No alternative approach is known at this point.

# Explain it to folk outside of your team

CockroachDB now provides two types of nodes: DB nodes and
Observability nodes. DB nodes server SQL and operate using a consensus
protocol to ensure ACID transactions for SQL.

Observability nodes are responsible for monitoring the DB nodes and
serve read-only monitoring data over user-facing APIs.

# Unresolved questions

None known.
