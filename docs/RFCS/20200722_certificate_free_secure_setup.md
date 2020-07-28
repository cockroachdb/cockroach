- Feature Name: Certificate-free Secure Setup
- Status: draft
- Start Date: 2020-07-22
- Authors: @aaron-crl
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes a means of creating a CRDB single or multi-node cluster running in the default secure mode without requiring an operator to create or manage certificates. The use of an operator-opaque join token would prove membership and enable nodes to self-manage the cluster's internal trust mechanisms. 

# Motivation

Certificates and their management have traditionally been major sources of toil and confusion for system administrators and users across all sectors that depend upon them for trust. Our current user story for CockroachDB does not stand out against this tradition.

After reviewing the existing CockroachDB trust mechanisms, it seems that effort and complexity of certificate management drives users to test and prototype in `--insecure-mode` rather than the default (secure) state. Our getting started guide highlights this well as the "insecure" guide (https://www.cockroachlabs.com/docs/v20.1/start-a-local-cluster.html) starts with `cockroach start` whereas the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) has four complex certificate generation steps before the user may even start their cluster.

In addition, the functional security gap between default and "insecure" mode is not well presented ([TBD], [TBD]) and has resulted in unpleasant user surprises.

This RFC supports an approach where an operator would be able to start a CockroachDB cluster in secure mode and have it "just work" without worrying about certificates. This is expected to decrease usage of the `--insecure-mode' flag (which we can measure internally against our own engineering teams) resulting in a smoother and more secure experience for all CockroachDB users.

It will also help with orchestration in both our customers deployments and our own SaaS offering.

# Guide-level explanation

This introduces the concept of a `join-token` that is provided to a `node` to permit it to join an existing cluster. `nodes` will self manage certificates and trust on cluster internal interfaces enabling operators to focus on configuring and securing external access.

Borrowing from the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) this feature will allow all four of the existing certificate management steps to fall away. In their place an additional output from the first step of "Start the Cluster" will emit the two join tokens for the second and third nodes. These join tokens would then be added to the start lines for each node and the cluster would start with all internode traffic secured by strong TLS.

This removes the _requirement_ that operators manage inter-node certificates themselves. They may still do so if they have functional or business needs that do not permit this pattern. This will also help with orchestration as any node may then be used to create a `join-token` for another node providing the same high availability as any other CockroachDB feature. The ability to create these tokens can be gated behind the same permissions as those used to add and remove nodes to existing clusters.

This removes strict external dependence on fully qualified domain names ([TBD])for internode TLS removing the need to have internode certificates signed by globally trusted Certificate Authorities and reducing the blast damage of a compromised CockroachDB certificate.

Existing roachers deploying with the default secure mode will find that they have fewer steps to get to a running cluster. Roachers who have relied on `--insecure-mode` to avoid the hassle of managing certificates when testing will now be able to test with the system in a secure state by default with minor adjustment to their workflow.

# Reference-level explanation

**New term:**   `join-token`
This will be a user opaque token that can be requested by a cluster operator from any online node and supplied to a fresh unprovisioned node to join it to the cluster.

A `join-token` must contain a means to
 - Confirm the identity of the server (certificate public key fingerprint, plus host)
 - Identify the joining node (shared secret)
 - Establish token uniqueness (for auditing and to avoid reuse)
 - Expire (hygiene)

In this approach the pre-init configuration of CockroachDB nodes has a new starting state where it does not attempt to connect to other nodes. It will prompt or accept a flag that tells it that it is a new node and the first in a cluster or that it will be joining a cluster with a supplied join-token.

#### In the case of “first node / new cluster”

The node will take the following steps:
- Generate a new internode CA
- Use this CA to generate a new node certificate
- Generate a new cluster user CA
- Use this CA to generate a new root user certificate
- Write all certs to the standard certificate directory for CockroachDB
- Wait for init command or a provisioning request

**(TBD/Unknown)**
*I’m not sure how init works so we may have to tweak this around joining initial nodes to the base cluster before the init command is sent.*

My hypothesis is that we can use the root cert to request `join-token`s from the first node and use them to join other nodes to the first node before issuing the init command. If this presents complications it might be easier to init the first node and then grow the cluster with `join-token`s immediately after.

#### In the case of “joining a cluster”

Upon launch:
- The new node will be provided a `join-token` containing the hostname of the surrogate node (node that generated the join token) and will attempt to connect to, authenticate, validate, and provision itself from the surrogate.
- The surrogate will then generate a new node certificate for the joining node and return it as part of a provisioning bundle. The bundle will also include the internode CA (public and private keys), the user certificate CA (if present) and any other initialization information.
- The joining node may then use its valid node certificate and gossip to align itself with the rest of the cluster.

### Success monitoring

Success will present itself through:
- A decrease in the percentage of clusters running in insecure mode
- Reduce operator toil and friction easing adoption of the product where proof of concepts may have been conducted with the --insecure-mode flag

## Detailed design

Outline both "how it works" and "what needs to be changed and in which order to get there."

Describe the overview of the design, and then explain each part of the
implementation in enough detail that reviewers will be able to
identify any missing pieces. Make sure to call out interactions with
other active RFCs.

### What does it do

`TBD`

### How it works

`TBD`

## Drawbacks

`TBD`

## Rationale and Alternatives

This appraoch removes certificate management of cluster internal interfaces from the operator. It cleanly separates the trust relationships for application internal communications from external user and administrative interface greatly enhancing security boundaries. It removes almost all existing friction for experimenting and testing with a secure cluster reducing dependence on insecure configurations. It simplifies the documentation and user story to achieve a more secure configuration.

Other patterns are being pursued to address or work around the challenges of our existing certificate story:
- A custom kubernetes operator([TBD]) is being constructed to aid with certificate management and pod deployment.
- The docs team has invested heavily in attempting to improve the documentation around our use of certificates with some success.

Niether of these approach the gap between enterprises with well developed certificate management infrastructures and independent developers. Enterprises are generally willing to work through a hardening guide to deploy a production system. Independent developers tend to want the thing to "just work."

If we do not address this now we will continue to experience certificate-based frustration internally, among customers, and external developers. More insecure clusters will continue to be deployed. We may miss future opportunities where a smoother certificate user experience would have rendered success.

## Unresolved questions

- How does this interact with the existing init functionality?
- Can we use gossip for more configuration details (including more of provisioning/enrollment)?
- What will the exact format and content of the join token be?
