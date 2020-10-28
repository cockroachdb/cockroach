- Feature Name: Certificate-free Secure Setup
- Status: draft
- Start Date: 2020-07-22
- Authors: @aaron-crl
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
* Node user-facing services have a distinct CA (`sqlServiceCa`)
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

This guide is split into kubernetes and manual cases.

## Kubernetes

**N.B.** _Only if the operator is not already managing their own certificates._

**Initial Configuration**
In the Kubernetes case we leverage the ability to share secrets across a namespace to manage the CA for inter-node trust and a lock (or friendly k8s analog).
* `interNodeCa`: Node certificate authority
* `clusterInitLock`: Initialization lock to ensure atomic cluster secret population

When a new collection of nodes is started as part of a set. Each node checks for the existence of `interNodeCa`.
* If found it uses it to mint a node certificate for its hostname then connects with peers following the normal startup process.
* If not found it attempts to acquire a lock `clusterInitLock`.
  * If unsuccessful in acquiring the lock it pauses _polling_delay_ and then repeats the above two steps polling until one of the two above actions succeeds.
  * If successful in acquiring the lock, it generates any missing CAs including the `interNodeCa`, writes `interNodeCa` last, then restarts itself (leaving the lock to signify that the cluster has been initialized).

**SQL Port CA**
If a SQL port certificate is not present, the node that acquired the lock will also generate a SQL port CA to be written to a secret. Nodes will use this CA to generate their own SQL service certificates.

**Admin UI CA**
Same as SQL port above. Default to share SQL port CA if not otherwise specified.

**Root User**
If a user CA is not already present, the node that acquired the lock will also generate a user CA to be written to a secret. It will use this CA to generate the `root` credentials and emit them to another k8s shared secret making this available to the operator and any administrative containers.

**Add Node**
Adding a node in this case is no different from the existing workflow as the node will self-provision and join with the above logic with no additional steps.

**Add Region**
Adding a region securely would simply be a matter of pre-populating the new region's namespace with the above generated CAs and setting the `clusterInitLock`.

## Manual or scripted configuration

**Initial Configuration**

Borrowing from the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) this feature will allow all four of the existing certificate management steps to fall away. In their place an additional output from the first step of "Start the Cluster" will emit the `initialization-token` for the second and third nodes. This token would then be added to the start lines for each node and the cluster would start with all internode traffic secured by strong TLS.

This cut-and-pasteable `initialization-token` contains a short-lived (strawman: 60 minutes/configurable) inter-node CA. The token is passed to all nodes in the initial start group enabling them to extract the CA, mint host certificates, and connect. It is generated by the first node by passing it an `initializeSecurityContext` flag or some analog. After the initial CA expires, certificates will rotate automatically and the `initialization-token` will be rendered inert.

**SQL Port CA**
If a SQL port certificate is not present, the initializing node will generate a SQL port CA to be written to the certificates directory. Nodes will use this CA to generate their own SQL service certificates.

**Admin UI CA**
Same as SQL port above. Default to share SQL port CA if not otherwise specified.

**Root User**
If a user CA is not already present, the initializing node will generate a user CA to be written to the certificates directory. It will use this CA to generate the `root` credentials and emit them to the same directory.

**Add/Remove nodes (join-tokens)**
Any user with the permissions to add or remove nodes would have the ability to generate a `join-token` via a SQL call. The `join-token` would then be passed as an initial argument to the new node enabling it to prove membership and receive the cluster's CAs (and populate its own certificates directory). The token would be consumed upon successful join to reduce risks associated with join token spillage.

**Adding New Regions**
[TBD]

# Reference-level explanation

## Kubernetes

To support the above k8s approach the system will need to be able to generate both internal and external-facing CAs depending on what information already exists in the k8s namespace.

For reference we use the following names to refer to k8s secrets.
* `interNodeCa`: CA governing inter-node TLS
* `userAuthCa`: CA governing user authentication
* `sqlServiceCa`: CA governing SQL service port TLS
* `adminUiServiceCa`: CA governing admin-ui/HTTP service port TLS

If a node comes up and does not detect an `interNodeCa` it will attempt to acquire the `clusterInitLock`. If unsuccessful it will repeatedly poll for new certs then attempt to acquire the lock again but avoid taking further initialization actions. It may be possible to affect this lock using etcd.

Once a lock is acquired the node checks for the presence of each CA listed above.
* If CA certificates are found, it should validate that they have the correct shape and options and emit an error if the certificate is not appropriate.
* If a CA certificate is not found, it should generate one based on any supplied configuration or reasonable application defaults.

**N.B.** The `interNodeCa` must be written ***LAST*** to ensure that all other CAs have been generated before other nodes begin to initialize their services.

_Failure to generate the `interNodeCa` is the only generation step that must be fatal in this process as all other certificates may be refreshed/updated after nodes are online._

**Root User**
* If a `userAuthCa` is already present. The initializing node will check its shape and continue.
* If a `userAuthCa` is not present. The initializing node will generate a new user CA, store it to the `userAuthCa` secret, then use it to generate a certificate for the `root` user.

Once CA generation/checking is complete the initializing node may leave the `clusterInitLock` held to note that the cluster has been initialized and prevent nodes attempting to reinitialize shared secrets in circumstances where storage and node state have otherwise lost integrity.

Finally, once certificate check/generation is complete. The initializing node returns to the polling state; this time detecting its CAs and minting its own service certificates (see below).

**Service Certificates**
If a node comes up and detects an `interNodeCa` secret. It will iterate through the above list of CAs and attempt to generate service certificates for its hostname from each one.

**Root User**
All nodes will use the `userAuthCa` generated or checked above and implicitly trust user certificates issued by this CA.

**Add Node**
Unremarkable. Adding a node node follows the same process as above any time after initialization. A node is provisioned in the k8s namespace, finds its CAs, mints its service certs, and comes online.

**Add Region**
[TBD]

## Manual Initialization

In this approach the pre-init configuration of CockroachDB nodes has a new starting state where it does not attempt to connect to other nodes. It will prompt or accept a flag that tells it that it is a new node and the first in a cluster or that it will be joining a cluster with a supplied `join-token`.

**New term:** `join-token`
This is a user opaque token that can be requested by a cluster operator from any online node and supplied to a fresh unprovisioned node to join it to the cluster.

A `join-token` must contain a means to:
 - Confirm the identity of the server (certificate public key fingerprint, plus explicit verification of shared secret)
 - Identify the joining node (shared secret)
 - Establish token uniqueness (for auditing and to avoid reuse)
 - Expire (hygiene)

**New term:** `initialization-token`
This is a user opaque token that is generated for a cluster operator by the first node and supplied to all other nodes in the starting set to join them to the cluster.

An `initialization-token` must contain a means to:
 - establish a common CA for inter-node trust

**New term:** `initialization-bundle`
This is the collection of CAs that a new node node needs to generate its certificates. Any validly authenticated node may request these.

#### In the case of “first node / new cluster”

Any cockroachdb binary may be used to generate an `initialization-token` to be supplied to the initial nodes. It may be the first node started but it does not need to originate in the starting cluster as it is simply an encoded CA and cluster config details. In the above guide example, it is assumed that the initial node will use the token it generates automatically.

Nodes will initialize in a slightly different fashion from the k8s configuration. Each node will extract its `interNodeCa` from the `initialization-token`, mint its own host certificate and connect to peers. As soon as a node determines that it is appropriately peered it will check for the presence of a `clusterInitMutex` mutex within CRDB. If it does not exist it will create and set it to `INITIALIZING` then follow similar steps for certificate check/generation as in the above k8s case (except that it will use the certificates directory by default instead of k8s secrets). Namely:
- Generate relevant CAs:
  - Check/Generate a new cluster `sqlServiceCa`.
  - Check/Generate a new cluster `adminUiCa` (or use the SQL one if default/desired).
  - Check/Generate a new cluster `userAuthCa`.
- Write all generated certs to the standard certificate directory for CockroachDB (or configured paths)
- Store the `initialization-bundle` within the database.
- Mark the `clusterInitMutex` to `READY`.
- Restart the node process.

**Mutex Polling and Provisioning**
For any node that finds itself appropriately peered and `clusterInitMutex` created, it will poll until the mutex reaches the `READY` state. Once this has occurred the node will read the `initialization-bundle` from the database, extract the CAs and generate its own host certificates. Namely:
- Use the `sqlServiceCa` to generate a new SQL service certificate (if applicable).
- Use the `adminUiCa` to generate a new admin-ui service certificate (if applicable).
- Use the `userAuthCa` to generate a new `root` user certificate (if applicable).
- Restart to pick up its own service certificates.

**Additional Notes**
- The lifespan of the initial internode CA is short (strawman: 60 minutes).
- Presence of an appropriate leaf certificate with key should satisfy any `sqlServiceCa` and `adminUiCa` checks.
- Presence of a user CA public key should satisfy any `userAuthCa` checks.

**initialization-token**
An `initialization-token` carries a short-lived CA in a user copyable form and any other relevant configurable information (such as cluster name).

Strawman token structure:
`base64encode( <interNodeCa bytes>|<cluster name string> )`

#### In the case of “joining a cluster”

Any existing node may be used to generate a `join-token` if the user has appropriate permissions (let us assume these will be the same as current requirement for adding nodes).
On existing node:
- A user or operator invokes a new token request.
- The node (called the surrogate from here forward) generates a token of of the form:
`<token-uuid>|<surrogate-node-ca-public-key-fingerprint>|<shared-secret>|<expiration>`
- The surrogate will place the token in a `join-token`'s table where its `token-uuid` and `expiration` are noted.
- The new `join-token` is returned to the invoker.

**Upon new node launch:**
- The new node will be provided a `join-token` and the hostname(s) of any active node(s).
- The new node will attempt to connect to a supplied hostname and upon success will attempt to validate the existing node ("server" from here forward) through the following two steps:
    1. Check that the provided certificate of the host was signed by a CA with the expected fingerprint
    2. Send its `join-token` `token-uuid` and a random plaintext `server-challenge` to the host
        - The host will:
            - Look up the token associated with this `token-uuid`
            - Generate an HMAC using the `server-challenge` and the `shared-secret` contained in the `join-token` with the specified `token-uuid`
            - Return this HMAC via the existing TLS connection. The server will also return it's own plaintext `node-challenge` for node validation in a later step.
- Once the node has validated the server by checking the returned HMAC it will perform the same form of HMAC using the `shared-secret` and the server's `node-challenge`. It will present this HMAC to the server as proof that it possesses a valid join token.
- The server will then return the `interNodeCa` and any other information contained in the `initialization-token`.
- The server will mark the `join-token` as consumed in the cluster registry to avoid reuse.
- The joining node writes the CA to its local private storage, mints its own host certificate, then restarts into a provisioning state.
- The node upon restart enters the mutex polling state above until it can receive the provisioning bundle.

`TODO(aaron-crl):` I'm not happy with where this provision tail landed but it forced me to reconsider storing more configuration in the database.

** Before checking for `token-uuid` presence in the `join-token`'s table the system checks expiration for all unexpired `join-token`s then proceeds to check node supplied `token-uuid` against available valid tokens. It is expected that this is an infrequent operation and a sparse table allowing us to bear this pruning cost on access as opposed to as part of scheduled maintenance. This check should also probably be atomic to avoid potential pruning races. This process should probably also log and remove expired tokens to keep the table small.


### Success monitoring

Success will present itself through:
- A decrease in the percentage of clusters running in insecure mode.
- Reduce operator toil and friction easing adoption of the product where proof of concepts may have been conducted with the --insecure flag.

## Detailed design

Outline both "how it works" and "what needs to be changed and in which order to get there."

Describe the overview of the design, and then explain each part of the
implementation in enough detail that reviewers will be able to
identify any missing pieces. Make sure to call out interactions with
other active RFCs.

`TBD`

### What does it do

`TBD`

### How it works

**Kubernetes**
In the Kubernetes case, this approach allows whichever node is ready first to grab the `initialization-lock` and generate any CAs that were not supplied within the namespace. This provides opportunities for an operator to prepopulate a namespace secret store with their own user CA and/or intermediate service CAs.

By allowing all nodes to poll for the presence of the `interNodeCa` the _node_ initialization process is identical for all nodes including the one that grabs the `initialization-lock` and handles cluster CA provisioning. An additional option for operators is to grab the lock before nodes are started and then release it when they are ready for the cluster to initialize. This may be useful if an operator needs time for the secrets store to be populated between container creation and initialization.

**Manual Deployment**
In the manual case, initial trust is established by sharing a short-lived key across all nodes. We reduce the risk associated with this key being mishandled by expiring it quickly. Cluster initialization otherwise follows a virtually identical control flow to the kubernetes case minimizing asymmetries.

In the add node case using a `join-token` we again use a short-lived key but with a more severely limited scope as it is also consumed by the join operation. The `join-token` contains enough information to reasonably establish for the new node that it is talking under TLS to an existing trusted member of the cluster and enables mutual verification of the token before allowing a node to be provisioned. Once the node receives its CAs it restarts and makes use of similar self provisioning logic as in the kubernetes process.

## Drawbacks

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
- Can we use gossip for more configuration details (including more of provisioning/enrollment)?
- What will the exact format and content of the join token be?
- How would this work in multi-region?
- Can we just store the node provisioning certificates in the database permanently instead of writing them to external storage?
- Would it be better not to store node service CAs in the database at all? I worry that the bundle will become stale if operators modify the certificates on disk. Maybe it would be better to use a procedural call to generate a bundle on demand from any functioning node?