CockroachDB technical notes
============================

This directory contains documents written by CockroachDB contributors
for other CockroachDB contributors. These documents are aimed to ease
onboarding and capture organizational knowledge.

Technical notes differ from RFCs in that they describe the state of
the project as it *currently* is, not the way we wish it to be.

The codebase also contains a gold mine of large code comments that provide a deeper conceptual dive of some aspect of the codebase. Below is a list of links to some of these comments. Note that these link to master, so if a link dies, please update it!
- Overview of the Keyspace: https://github.com/cockroachdb/cockroach/blob/master/pkg/keys/doc.go
- On Stores, Ranges, and Replicas: https://github.com/cockroachdb/cockroach/blob/master/pkg/kv/kvserver/store.go#L433

Standard disclaimer: each document contains parts from one or more
author. Each part was authored to reflect its author's perspective on
the project at the time it was written. This understanding is
necessarily subjective: its context is both the state of the project
and the authors', and their reviewers', experience around that
particular date. In case of doubt, consult your local historian and
your repository's timeline.

Of course, updates and corrections are welcome at any time.
