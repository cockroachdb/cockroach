
- Feature Name: Certificate-free Secure Setup
- Status: draft
- Start Date: 2020-07-22
- Authors: @aaron-crl, @knz, @bdarnell
- RFC PR: ([#51991](https://github.com/cockroachdb/cockroach/pull/51991))
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes a means of creating a CRDB single or multi-node cluster running in the default secure mode without requiring an operator to create or manage certificates. _It should "just work"._ In addition it proposes a means by which an operator may add nodes to a running cluster via an operator-opaque `join-token` which would prove membership and enable nodes to self-manage the cluster's internal trust mechanisms.

# Motivation

Certificates and their management have traditionally been major sources of toil and confusion for system administrators and users across all sectors that depend upon them for trust. Our current user story for CockroachDB does not stand out against this tradition.

After reviewing the existing CockroachDB trust mechanisms, it seems that the effort and complexity of certificate management drives users to test and prototype with the `--insecure` flag rather than the default (secure) state. Our "Getting Started" guide highlights this well as the "insecure" guide (https://www.cockroachlabs.com/docs/v20.1/start-a-local-cluster.html) starts with `cockroach start` whereas the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) has four complex certificate generation steps before the user may even start their cluster.

In addition, the functional security gap between default and "insecure" mode is not well presented ([https://github.com/cockroachdb/cockroach/issues/53404], [https://github.com/cockroachdb/cockroach/issues/44842], [https://github.com/cockroachdb/cockroach/issues/16188#issuecomment-571191964], [https://github.com/cockroachdb/cockroach/issues/49532], [https://github.com/cockroachdb/cockroach/issues/49918]) and has resulted in unpleasant user surprises.

Implementation of the approach in this RFC is expected to decrease usage of the `--insecure' flag (which we can measure internally against our own engineering teams) resulting in a smoother and more secure experience for all CockroachDB users.

This also addresses the deployment challenge where some CAs demand issued certificates match fully qualified domain names ([https://aws.amazon.com/certificate-manager/faqs/?nc=sn&loc=5#ACM_Public_Certificates]) for internode TLS; removing the need to have internode certificates signed by globally trusted Certificate Authorities and reducing the blast damage of a compromised CockroachDB certificate.

This removes the _requirement_ that operators manage inter-node certificates themselves. They may still do so if they have functional or business needs that do not permit this pattern. This may also help with orchestration as any node may then be used to create a `join-token` for another node providing the same high availability as any other CockroachDB feature. The ability to create these tokens can be gated behind the same permissions as those used to add and remove nodes to existing clusters.

Existing users deploying with the default secure mode will find that they have fewer steps to get to a running cluster. Users who have relied on `--insecure` to avoid the hassle of managing certificates when testing will now be able to test with the system in a secure state by default with minor adjustment to their workflow.

## Goals

### 0 Support for multiple deployment patterns without UX regression
All subsequent goals are expected to be met for manual, kubernetes, and CLI launchable configurations. We should avoid solutions that create UX regressions when compared to the existing `--insecure` configurations of these wherever possible.

### 1 Initial startup path
Initialization must complete the following two tasks in addition to starting the base nodes:
* Generating and propagating cluster trust primitives
* Providing means to access the new cluster 
(These are currently achieved through the manual configuration of node and root certificates, we seek to automate this)

Any solution must satisfy the following list of requirements:
* All nodes have distinct node-to-node trust based on a node CA (`interNodeCa`)
* Nodes rely on a distinct user CA for access to the cluster (`userAuthCa`)
* Node user-facing services have a distinct CA (`sqlServiceCa` and `rpcServiceCa`)
* A valid administrative certificate (traditionally "root" in CockroachDB) or analog has been emitted

Optional requirements:
* A valid user certificate may be emitted
* A distinct admin-ui CA may be created (`adminUiServiceCa`)

### 2 Add/Remove nodes
Support secure and atomic add/remove operations for existing clusters.

### 3 Adding new regions
Enable manual and k8s users to add new regions (collections of nodes) to an existing cluster

## Non-Goals

We do not seek to remove the "init container" but anticipate the changes this RFC will require may not be compatible with its existing function.

# Guide-level explanation

Except where otherwise noted, manual, scripted, and kubernetes flows are identical. This approach attempts to accomodate for cases where operators wish to manage some, but not all of their certificates.

## Initial Configuration

Operators may use an `initialization-token` to avoid all certificate management, or supply their own `interNodeCa` if desired. Both provide auto-initializing paths to a secure cluster.

**initialization-token**
To support the generation and propagation of trust primitives a starting token may be provided to all nodes in the initial set. The token contains a seed for the PRNG to permit generation of a deterministic internode CA which all nodes can independently construct. Once nodes have constructed this CA, they mint their own host certificates, and establish trust with peers via mTLS. Once a quorum of nodes trust each other, a leader self-nominates that will generate any missing CA's and/or relevant certificates. The leader then serves these to all peers to enable TLS on all interfaces across the cluster. The initial deterministic CA is rotated after a short period of time to reduce the window for cryptologic attacks.

Borrowing from the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) this feature will allow all four of the existing certificate management steps to fall away. We may add an implicit `initialization-token` to the single-node start case or a flag that tells the node to synthesis its own `initialization-token` (such as `--self-secure-init`). For the second and third nodes, since they share the same certificate directory, the documentation would remain unchanged and the cluster would start with all internode traffic secured by strong TLS.

**BYO interNodeCa**
If an operator wishes to supply their own `interNodeCa` they may skip the use of initialization tokens altogether by placing it on each nodes filesystem. The nodes will then follow the initialization process identical to that listed above.

_This will likely be the preferred kubernetes deployment pattern._

## Add/Join operation

Any node with access to the `interNodeCa` may be joined to the cluster (at any time) by simply providing it a list of peers (as is already the case). On startup, it will use the `interNodeCa` to mint its own host certificate, then use that to connect to peers and request the current `initialization-bundle` from any of them.

_In a kubernetes environment it is conjectured that this internode CA may be presented as a secret and managed by an operator if desired._

In a configuration where it is impossible or undesirable to directly manage the internode CA, a SQL function is exposed for generating `join-token`s that are single-use, time-limited primitives that enable a node without access to the `interNodeCa` to submit proof of membership.

This would manifest as a new flag passed to the `start` command (https://www.cockroachlabs.com/docs/v20.1/cockroach-start.html) and look something of the form:
```
cockroach start --certs-dir=certs \
    --advertise-addr=node2 \
    --join=node1,node2,node3 \
    --cache=.25 \
    --max-sql-memory=.25 \
    --join-token=b122701da0ade08fc31b54ebdac40a6b23322ae5c74d7a5d0f798f6663af6c67c4b0ef741644bc355c0945ddb8995375d05825cd5918bbc7445c6c8451a34262
```

# Reference-level explanation

**Documentation aids:**
The following names to refer to node certificate authorities.
* `interNodeCa`: CA governing inter-node TLS
* `userAuthCa`: CA governing user authentication
* `sqlServiceCa`: CA governing SQL service port TLS
* `rpcServiceCa`: CA governing RPC service port TLS
* `adminUiServiceCa`: CA governing admin-ui/HTTP service port TLS
_Node will look for these as encoded files at a specified certificate directory path._

**New term:** `initialization-token`
This is a user friendly token shared with all initial nodes that enables them to securely establish mutual trust.

An `initialization-token` must:
 - contain a means to establish a common CA for inter-node trust
 - be short-lived
 - be easy to copy/paste

**New term:** `initialization-bundle`
This is the collection of CAs that a new node node needs to generate its certificates. Any validly authenticated node may request these. The bundle includes:
* `userAuthCa`: CA governing user authentication
* `sqlServiceCa`: CA governing SQL service port TLS
* `rpcServiceCa`: CA governing RPC service port TLS _(if present)_
* `adminUiServiceCa`: CA governing admin-ui/HTTP service port TLS
* `root` user certificate _(if present)_

**New term:** `join-token`
This is a user opaque token that can be requested by a cluster operator from any online node and supplied to a fresh unprovisioned node to join it to the cluster.

A `join-token` must contain a means to:
 - Confirm the identity of the server (certificate public key fingerprint, plus explicit verification of shared secret)
 - Identify the joining node (shared secret)
 - Establish token uniqueness (for auditing and to avoid reuse)
 - Expire (hygiene)

## Initial Configuration (establishing trust)

Nodes will use the below flow to reach an mTLS-ready state:

![
@startuml
start
:node start;
if (Is `interNodeHostCertificate` (public, private) key present) then (yes)
else (no)
  if (Is `interNodeCa` (public, private) key present) then (no)
    if (Is `initToken` commandline argument present) then (yes)
      :generate short-lived `interNodeCa`;
    else (no)
    :fail normally with missing CA;
    stop
    endif
  endif
  :generate `interNodeHostCertificate` (public, private) keys;
endif
:restart to bootstrap valid `interNodeHostCertificate`;
stop
@enduml
](http://www.plantuml.com/plantuml/png/bP6nJiCm48PtFyK_9XAyGBCKTO65YGUeMr_QK-sxo7SfojiZEKD080HUV8E_pv_ilNciFfSSsX1seedGvY447j6z68uiJlLL4psh-O6gyyZdw7H4DysdpES7J9NlqQd7ZHPCbOp4U_YL1Dr2rWVAHkX4-m1yynxY7rKh_zd0_gOtaYFEMagKb5a8iLxcGk8_bg2jtOM4QdI2NRNwO-OxfQz9GpkwXJTiZ9mXMalCUS4x-nM5pLWkE3ojXBbEAog9nx3jsvt_VL8RmYhPLMg_0rUSLDsynWdtc3dz8Xr2QzgJfBda3m00)

In the case where a `interNodeHostCertificate` is already present on the system. It is assumed that all other intended certificate provisioning has already occurred.

Failing that, the node will attempt to use a supplied `interNodeCa`, if present, to create its own `interNodeHostCertificate`. (_This permits k8s operators to use an externally generated CA for these interfaces mounted as a secret._)

Failing that, the node will look for a ***valid*** `initToken` command line argument. If present it will use this to generate a deterministic certificate that will be used as above to generate its `interNodeHostCertificate`.

**Generating Interface Certificates**

Once nodes have successfully joined each other via mTLS and reached quorum, one of the nodes will grab a mutex (or analog), set it to an initializing state (`INIT`) and craft any needed certificates that are not already present on its local filesystem (or analog). This will follow the below flow for each of the interface CAs:
* `sqlServiceCa`
* `rpcServiceCa`
* `adminUiServiceCa`

![
@startuml
start
repeat: for each `interface`;
if (Is `interface` host certificate (public, private) key missing) then (yes)
  if (Is `interface` CA (public, private) key missing) then (yes)
    :generate `interface` CA (public, private) keys;
  endif
  :generate `interface` host certificate (public, private) keys;
endif
repeat while (done with interfaces?) is (no)
->yes;
stop
@enduml
](http://www.plantuml.com/plantuml/png/bT31IeCn40JW-px571FG5sX1AvvyYyPqqYp-JNwoMyl_zlve4K4FzROCp0SxE_Cq_7oQvFEGmPd9DoXzW2bNxBKvHqcP-ws85eGN-ncYTdDa3jUYEJaHvlFhfFaEyz3tDOXuuuAJccaxHdXbGrXeKO0_lEUdVmd0viZ6aPmtCROLWEsWHNvNR__e2icNT9qDbweJ4Gwz4HVrYc_L7YFK45gFSl-mS6sRzrbsAx2k_m40)

**Root User**
The node then checks for a `userAuthCA` containing a _public_ key. If found it continues with the remaining initialization path. If NOT found, if generates a `userAuthCA` and a `root` user certificate and emits both to the filesystem.

**Distribution**
The node upon satisfying the above checks and/or generation steps. Sets the mutex to `READY`, marks itself initialized, then restarts to pick up all newly minted certificates.

While this is happening, its peers poll the mutex waiting for the `READY` state. Once they detect this state change, they begin polling their peers until one of them provides an `initialization-bundle`. Upon receipt of an `initialization-bundle` the node installs the received certificates and restarts to begin using them on all relevant interfaces.

![
@startuml
database Node_Orange
control ClusterInitStatus
database Node_Cherry
database Node_Banana
== mTLS established ==
group Interface Initialization
Node_Orange -> ClusterInitStatus : Set get (lock) and set status to `INIT`
activate ClusterInitStatus
Node_Banana --> ClusterInitStatus : Poll for `READY` status
Node_Cherry --> ClusterInitStatus : Poll for `READY` status
note over Node_Orange : Generate Interface Certs
note over Node_Orange : Generate Root User Cert if necessary
Node_Orange -> ClusterInitStatus : Set status to `READY`
note over Node_Orange : Restart
Node_Cherry -> ClusterInitStatus : Read `READY` status
Node_Banana -> ClusterInitStatus : Read `READY` status
end
group Nodes begin polling neighbors to request certificate bundle
Node_Banana --> Node_Cherry : request certBundle (fail)
Node_Banana -> Node_Orange : request certBundle (success)
note over Node_Banana : Restart
Node_Cherry -> Node_Orange : request certBundle (success)
note over Node_Cherry : Restart
end
== Certificates propagation conplete ==
@enduml
](http://www.plantuml.com/plantuml/png/dPF1Yjj044JlynLrN1_s1mmEctL2C8HDi3U7dEBMg2GF6VSeCws5pTS72qC6Em9dhvAgkkgf-w6OP1kFqRLYqaWXdbBB7r-oQ4_darfE4Uiu5cFUQB2TYOtbulFrWJc_NZny51KLvrOh79y_xy1YqiHG3conMZdd-fp60HirvauySR8F4iDliP3KLK5m_-uw0vROqT3JS1UJ_xc0Q8j2GvdUmnBscwVDyzw9j_0YndyKgQBYVcRCrnGZkfInttvy_-7x_Zp0LTt_Mwl9YFJ2N0F74f-ep6AikAoPxGR1DYN3jy8y2H0wA3rBaVnw8yiAt3djxDGjfysf4SmPRodjDRcA-kqoHstFUpFbAMZO1yMGOWpQGndwGvFoL27pryXYyCmMkk35Y6RKDj9T_VMwnXAryd5IuAwJ41SNkMicCx8oUizI5XN8scCMv9kSAu_AcTfEHxZ-ow5Wo6cGVZeq-AH3fF5qbG_KTZp6Fm00)

The cluster is now ready.

**Kubernetes (additional note)**
If the operator elects to use a kubernetes secret for the `interNodeCa` they will be responsible for rotating it as that path may not be writable.

## Adding Nodes

**Kubernetes**

In the kubernetes case, adding nodes is trivial if the `interNodeCa` is mounted in the container of the joining node. The node starts, detects the CA, generates it's host certificate, then connects to a peer and asks for an `initialization-bundle`. After installing this it restarts and is ready to function.

**Other orchestration (using interNodeCa )**
Identical to the kubernetes case except that the orchestration solution must place the `interNodeCa` on the node's local filesystem.

**Manual (join-tokens)**
Any user with the permissions to add or remove nodes would have the ability to generate a `join-token` via a SQL call. The `join-token` would then be passed as an initial argument to the new node enabling it to prove membership and receive the `interNodeCa`, restart, and request provisioning (and populate its own certificates directory). The token would be consumed upon successful join to reduce risks associated with join token spillage.

**Adding New Regions**
To add a new region an operator may copy the `interNodeCa` to the nodes in the new region, allow them to initialize themselves, then join them. Since all nodes share the same `interNodeCa` there should be no additional complexity beyond any other multi-region concerns.


# Detailed Design

`TODO(aaron-crl): More here pending consensus on the above`

## Establishing Initial Node Trust

Internode trust is predicated on being able to generate identical copies of the `interNodeCa` on all nodes. This is accomplished by seeding a PRNG with a common secret and generating all certs with an identical sign date and validity window. 

**initialization-token**
The initialization token must provide all required information to perform this process. For convenience and portability it should be user editable and easily copied. It may also carry relevant configurable information (such as cluster name).

Strawman JSON token:
```json
{
  "clusterName": "indominable",
  "numNodes": 3,
  "initSecret": "super secret string",
  "initStartValidity": "2020-01-02T03:00:00.000Z",
  "lifespanInSeconds": 3600
}
```

**Generating a CA deterministically**
All CA's generated across the cluster should have identical signatures. That means that in addition to using the same names, serial numbers, and options, they must have the same "randomly chosen" keys and must also use the same timestamps. This can be accomplished by specifying the validity window in the `initialization-token` and generating a PRNG seed value from the `initSecret`.

In order to reduce the feasibility of attacks against the initial certificates for their short lifespans the PRNG seed should be generated with a key derivation function like PBKDF2 to increase the amount of time required to brute force the certificate generation key space. The key derivation function should include at least the `initStartValidity` in addition to the `initSecret`  and possibly the cluster name to increase the key space.

## Joining a Cluster

Nodes must prove membership to join a cluster. This is affected with a join token that provides the material for a Diffie-Hellman-like mutual assertion of trust using HMACs. The tokens are ephemeral and single-use to reduce risk associated with spillage.

**Generating a join-token**
Any existing node may be used to generate a `join-token` if the user has appropriate permissions (let us assume these will be the same as current requirement for adding nodes).
On existing node:
- A user or operator invokes a new token request.
- The node (called the surrogate from here forward) generates a token of of the form:
`<token-uuid>|<surrogate-node-ca-public-key-fingerprint>|<shared-secret>|<expiration>`
- The surrogate will place the token in a `join-token`'s table where its `token-uuid` and `expiration` are noted.
- The new `join-token` is returned to the invoker.

**Upon new node launch:**
- The new node will be provided a `join-token` with its start flags.
- The new node will attempt to connect to a peer and upon success will attempt to validate the existing node ("server" from here forward) through the following two steps:
    1. Check that the provided certificate of the host was signed by a CA with the expected fingerprint.
    2. Send its `join-token`,  `token-uuid`, and a random plaintext `server-challenge` to the host.
        - The server will:
            - Look up the token associated with this `token-uuid`
            - Generate an HMAC using the `server-challenge` and the `shared-secret` contained in the `join-token` with the specified `token-uuid`
            - Return this HMAC via the existing TLS connection. The server will also return it's own plaintext `node-challenge` for node validation in a later step.
- Once the node has validated the server by checking the returned HMAC it will perform the same form of HMAC using the `shared-secret` and the server's `node-challenge`. It will present this HMAC to the server as proof that it possesses a valid join token.
- The server will then return the `interNodeCa`.
- The server will mark the `join-token` as consumed in the cluster registry to avoid reuse.
- The joining node writes the CA to its local private storage, mints its own host certificate, then restarts into a joining state.
- The node reconnects to a peer secured by mTLS and receives the `initialization-bundle`.
- The node initializes itself in the same way as the initial nodes.

** Before checking for `token-uuid` presence in the `join-token`'s table the system checks expiration for all unexpired `join-token`s then proceeds to check node supplied `token-uuid` against available valid tokens. It is expected that this is an infrequent operation and a sparse table allowing us to bear this pruning cost on access as opposed to as part of scheduled maintenance. This check should also probably be atomic to avoid potential pruning races. This process should probably also log and remove expired tokens to keep the table small.


### Success monitoring

Success will present itself through:
- A decrease in the percentage of clusters running in insecure mode.
- Reduce operator toil and friction easing adoption of the product where proof of concepts may have been conducted with the --insecure flag.

### What does it do

This creates new functionality by which an operator or orchestration solution may create a fully secured cluster and/or add nodes without directly managing certificates.

### How it works

**Kubernetes**


**Manual Deployment**


## Drawbacks

The cluster can reach a state of deadlock if the node that generates all interface certificates dies before it comes back online. 

`TBD`

## Rationale and Alternatives

This approach removes certificate management of cluster internal interfaces from the operator. It cleanly separates the trust relationships for application internal communications from external user and administrative interface greatly enhancing security boundaries. It removes almost all existing friction for experimenting and testing with a secure cluster reducing dependence on insecure configurations. It simplifies the documentation and user story to achieve a more secure configuration.

Other patterns are being pursued to address or work around the challenges of our existing certificate story:
- A custom kubernetes operator([https://github.com/cockroachdb/cockroach-operator]) is being constructed to aid with certificate management and pod deployment.
- The docs team has invested heavily in attempting to improve the documentation around our use of certificates with some success.

Neither of these approach the gap between enterprises with well developed certificate management infrastructures and independent developers. Enterprises are generally willing to work through a hardening guide to deploy a production system. Independent developers tend to want the thing to "just work."

If we do not address this now we will continue to experience certificate-based frustration internally, among customers, and external developers. More insecure clusters will continue to be deployed. We may miss future opportunities where a smoother certificate user experience would have rendered success.

## Unresolved questions
- It may also be worth adding a startup option that generates a non-`root` _user_ and _generated password_ to facilitate local testing and development.
- How does this interact with the existing init functionality?
- What will the exact format and content of the join token be?
- Can we just store the node provisioning certificates in the database permanently instead of writing them to external storage?
- If the operator elects to use a kubernetes secret for the `interNodeCa` they will be responsible for rotating it as that path may not be writable.