- Feature Name: Multitenancy code movement
- Status: draft
- Start Date: 2022-05-25
- Authors: knz ajs ben
- RFC PR: #82516
- Cockroach Issue: Jira CRDB-16053, CRDB-16091 (non-public)

**Note: this RFC document is a placeholder that describes the
high-level changes incurred by an actual RFC proposal that is kept
internal to CRL employees.**

# Summary (abridged)

We would like all clusters including self-hosted and dedicated to run
multitenant, albeit with just 2 tenants; this is one of the goals of
the "arch unification project". To achieve this, we bring under BSL
the capability to automatically define and run 2 tenants in-process.

# Technical design (abridged)

1. Bring the API definition (go interface) of KV Connector
   (`pkg/ccl/kvccl/kvtenantccl/connector.go`) under the BSL. However:
   - Keep the current implementation of the API, which supports
     network connections, under CCL.
   - Define a new implementation of the API suitable for in-process
     tenant servers, in the BSL tree.

2. Update TestServer and the related tenant server creation code in
   unit tests (and cockroach demo) to use the in-memory KV Connector
   implementation.

3. Move all the unit test code that is only dependent on the ability
   to start multitenant servers under the BSL directories where it
   belongs.

# Impact for end-users

This proposal makes it possible to use a BSL-only build of CockroachDB
with multitenancy.

# Discussion

The full RFC proposal is here: https://docs.google.com/document/d/1s0PVIKFywX3G9lZ3ds8Hi51cAIHph-pp2PCcKt7iwaw/edit#
(internal only)
