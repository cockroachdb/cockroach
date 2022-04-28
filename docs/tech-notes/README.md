CockroachDB technical notes
============================

This directory contains documents written by CockroachDB contributors
for other CockroachDB contributors. These documents are aimed to ease
onboarding and capture organizational knowledge.

Technical notes differ from RFCs in that they describe the state of
the project as it *currently* is, not the way we wish it to be.

The codebase also contains a gold mine of large code comments that provide a
deeper conceptual dive into some aspect of the codebase. Below is a list of links
to some of these comments. Note that these link to stable commits, so if the link looks stale,
please update it!
- [Cockroach Versioning](https://github.com/cockroachdb/cockroach/blob/90054c6b7b9629aeb904ffd4c0b9161f70e214b7/pkg/clusterversion/cockroach_versions.go#L18)
- [The SQL Optmizer](https://github.com/cockroachdb/cockroach/blob/c097a16427f65e9070991f062716d222ea5903fe/pkg/sql/opt/doc.go#L12) 
- [The Keyspace](https://github.com/cockroachdb/cockroach/blob/13880c9e8d6abc5c7fbd624254f088432b500004/pkg/keys/doc.go#L14)
- [On Stores, Ranges, and Replicas](https://github.com/cockroachdb/cockroach/blob/f06da926cd39a2c3b89b061c7bf356658d3524e1/pkg/kv/kvserver/store.go#L433)
- [Hybrid Logical Clocks](https://github.com/cockroachdb/cockroach/blob/d676d7e0a6739661c9993ca8dbb6d14feeb6b33d/pkg/util/hlc/doc.go#L12-L304) 

Standard disclaimer: each document contains parts from one or more
author. Each part was authored to reflect its author's perspective on
the project at the time it was written. This understanding is
necessarily subjective: its context is both the state of the project
and the authors', and their reviewers', experience around that
particular date. In case of doubt, consult your local historian and
your repository's timeline.

Of course, updates and corrections are welcome at any time.
