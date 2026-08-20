
- Feature Name: Certificate-free Secure Setup
- Status: draft
- Start Date: 2020-07-22
- Authors: @aaron-crl, @knz, @bdarnell
- RFC PR: ([#51991](https://github.com/cockroachdb/cockroach/pull/51991))
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes a means of creating a CRDB single or multi-node cluster running in the default secure mode without requiring an operator to create or manage certificates. _It should "just work"._ In addition it proposes a means by which an operator may add nodes to a running cluster via an operator-opaque `joinToken` which would prove membership and enable nodes to self-manage the cluster's internal trust mechanisms.

**A note on kubernetes and PKI**
While it is expected that the proposed approach may improve the kubernetes deployment experience, this RFC preserves compatibility with and makes no attempt to substitute for an externally managed PKI.

Due to the prefered readonly mount nature of Kubernetes Secrets we avoid their use to faciliate certificate rotation without requiring the application to understand and be provisioned for kubernetes secret APIs.

# Motivation

After reviewing the existing CockroachDB trust mechanisms, it seems that the effort and complexity of certificate management drives users to test and prototype with the `--insecure` flag rather than the default (secure) state. Our "Getting Started" guide highlights this where the "insecure" guide (https://www.cockroachlabs.com/docs/v20.1/start-a-local-cluster.html) starts with `cockroach start` whereas the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) has four complex certificate generation steps before the user may start their cluster.

In addition, the functional security gap between default and "insecure" mode is not well presented ([https://github.com/cockroachdb/cockroach/issues/53404], [https://github.com/cockroachdb/cockroach/issues/44842], [https://github.com/cockroachdb/cockroach/issues/16188#issuecomment-571191964], [https://github.com/cockroachdb/cockroach/issues/49532], [https://github.com/cockroachdb/cockroach/issues/49918]) and has resulted in unpleasant user surprises.

Implementation of the approach in this RFC is expected to decrease usage of the `--insecure' flag (which we can measure internally against our own engineering teams) resulting in a smoother and more secure experience for all CockroachDB users. It cleanly separates the trust relationships for application internal communications from external user and administrative interface enhancing security boundaries. It removes friction from experimenting and testing with a secure cluster reducing dependence on insecure configurations. It simplifies the documentation and user story.

This also eases the deployment challenge where some CAs require issued certificates match fully qualified domain names ([https://aws.amazon.com/certificate-manager/faqs/?nc=sn&loc=5#ACM_Public_Certificates]) for internode TLS; removing the need to have internode certificates signed by globally trusted Certificate Authorities and reducing the blast damage of a compromised CockroachDB certificate.

This removes the requirement that operators manage inter-node certificates themselves. They may still do so if they have functional or business needs that do not permit this pattern. It may also help with orchestration as any node may then be used to create a `joinToken` for another node providing the same high availability as any other CockroachDB feature. The ability to create these tokens can be gated behind the same permissions as those used to add and remove nodes to existing clusters.

Existing users deploying with the default secure mode will find that they have fewer steps to get to a running cluster. Users who have relied on `--insecure` to avoid the hassle of managing certificates when testing will now be able to test with the system in a secure state by default with minor adjustment to their workflow.

Other patterns are being pursued to address or work around the challenges of our existing certificate story:
- A custom kubernetes operator([https://github.com/cockroachdb/cockroach-operator]) is being constructed to aid with certificate management and pod deployment.
- The docs team has invested heavily in attempting to improve the documentation around our use of certificates with some success.

Neither of these address the gap between enterprises with well developed certificate management infrastructures and independent developers. Enterprises are generally willing to work through a hardening guide to deploy a production system. Independent developers tend to want the thing to "just work."

If we do not address this now we will continue to experience certificate-based frustration internally, among customers, and external developers. More insecure clusters will continue to be deployed. We may miss future opportunities where a smoother certificate user experience would have rendered success.

## Goals

### 0 Support for multiple deployment patterns without UX regression
All subsequent goals are expected to be met for manual, scriptable, and kubernetes configurations. We should avoid solutions that create UX regressions when compared to the existing `--insecure` configurations of these _wherever possible_.

### 1 Initial startup path
Initialization must complete the following two tasks in addition to starting the base nodes:
* Generating and propagating cluster trust primitives
* Providing means to access the new cluster 
(These are currently achieved through the manual configuration of node and root certificates, we seek to automate this)

Any solution must satisfy the following list of requirements:
* All nodes have distinct node-to-node trust based on a node CA (`interNodeCa`)
* Nodes rely on a distinct user CA for access to the cluster (`userAuthCa`)
* Node user-facing services have distinct CAs (`sqlServiceCa` and `rpcServiceCa`)
* A valid administrative certificate (traditionally "root" in CockroachDB) or analog has been emitted

Optional requirements:
* A valid user certificate may be emitted
* A distinct admin-ui CA may be created (`adminUiServiceCa`)

### 2 Add/Remove nodes
Support secure and simple for an operator add/remove operations for existing clusters with a minimal number of managed steps.

### 3 Adding new regions (Stretch Goal)
Enable manual and k8s users to add new regions (collections of nodes) to an existing cluster

## Non-Goals

We do not seek to remove the "init container" but anticipate the changes this RFC will require may not be compatible with its existing function.

# Guide-level explanation

Except where otherwise noted, manual, scripted, and kubernetes flows are identical. This approach attempts to accomodate for cases where operators wish to manage some, but not all of their certificates.

## Initial Configuration

Operators may use an `initialization-token` to avoid all certificate management, or supply their own `interNodeCa` if desired. Both provide auto-initializing paths to a secure cluster.

**initialization-token**
To support the generation and propagation of trust primitives, a starting token may be provided to all nodes in the initial set. The token is any string of bytes that may serve as a shared secret. The starting set of nodes will use this shared secret to create mutual trust relationships before generating and sharing their initial certificate bundle including a common interNodeCa. The initial generated CA is rotated after a short period of time to reduce the window for cryptologic attacks.

Borrowing from the "secure" guide (https://www.cockroachlabs.com/docs/v20.1/secure-a-cluster.html) this feature will allow all four of the existing certificate management steps to fall away. We may add an implicit `initialization-token` to the single-node start case, a flag that tells the node to synthesis its own `initialization-token` (such as `--self-secure-init`), or a file path. For the second and third nodes, since they share the same certificate directory, the documentation would remain unchanged and the cluster would start with all internode traffic secured by strong TLS.

**BYO interNodeCa**
If an operator wishes to supply their own `interNodeCa` they may skip the use of initialization tokens altogether by placing it on each node's filesystem. The nodes will then follow the initialization process identical to that listed above.

## Add/Join operation

Any node with access to the `interNodeCa` may be joined to the cluster (at any time) by simply providing it a list of peers (as is already the case). On startup, it will use the `interNodeCa` to mint its own host certificate, then use that to connect to peers and request the current `initialization-bundle` from any of them.

_In a kubernetes environment it is envisioned that this internode CA will be copied from any existing node to the joining node by an operator._

In a configuration where it is impossible or undesirable to directly manage the internode CA, a SQL function is exposed for generating `joinToken`s that are single-use, time-limited primitives that enable a node without access to the `interNodeCa` to submit proof of membership.

This would manifest as a new flag passed to the `start` command (https://www.cockroachlabs.com/docs/v20.1/cockroach-start.html) and look something of the form:
```
cockroach start --certs-dir=certs \
    --advertise-addr=node2 \
    --join=node1,node2,node3 \
    --cache=.25 \
    --max-sql-memory=.25 \
    --joinToken=b122701da0ade08fc31b54ebdac40a6b23
```

# Reference-level explanation

It is assumed that any customer-facing TLS configuration is separate from inter-node config in this approach. This will require additional TLS work within CRDB. The RPC service is also given its own certificate helping separate authentication surfaces until the RPC service's features are fully migrated to SQL and we can remove it entirely.

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

**New term:** `joinToken`
This is a user opaque token that can be requested by a cluster operator from any online node and supplied to a fresh unprovisioned node to join it to the cluster.

A `joinToken` must provide a means to:
 - Confirm the identity of the server (node to join)
 - Identify the joining node
 - Establish token uniqueness (for auditing and to avoid reuse)
 - Expire (hygiene)

## Initial Configuration (establishing trust)

Nodes will use the below flow to reach an init-ready state:

![
@startuml
start
:node start;
if (Is `interNodeHostCertificate` (public, private) key present) then (yes)
else (no)
  if (Is `interNodeCa` (public, private) key present) then (no)
    if (Is `initToken` commandline argument present) then (yes)
      :mutually establish trust and pick node to generate CAs;
    else (no)
    :fail normally with missing CA;
    stop
    endif
  else (yes)
  endif
  if (node picked to generate CAs) then (yes)
  :generate short-lived `interNodeCa`;
  :generate any missing interface CAs;
  else (no)
  : wait for `initialization-bundle`;
  : install CAs from `initialization-bundle`;
endif
:generate `interNodeHostCertificate` (public, private) keys;
:generate any host interface certificates (public, private) keys;
:restart to bootstrap valid `interNodeHostCertificate` and any interface certificates;
endif
: node starts normally;
stop
@enduml
](http://www.plantuml.com/plantuml/png/bPFF3fim3CRlF8MF8Aql09TElMoNdVO0JS4Kg_c3OgSLU_gvYKHfrquRfv3u-_dxJDYpc2Z9sIelgjQ70I6lkuf6gByp7CaBnXzwy2sm730AZTGRmIFKSpfPwh_07EcgEmrSSD4NPFJIW4peeLwGcmej8zG-D1N07zo3-KTKqMy993_31VqH-k2SyOCbZs3YEJcLlNK2-MbTacIiNG0rhNRc2IGc5b08pDHV88z20fpHOpI2SFZANPPlmoXgD6IrEhhCkv5Cu8YP_5abHS4IvYBr0urLGQo6ocQ9TU-Q--FmslmbHhkUyHIYx2nTSNYUQlTKPloo6ijLe-dNLDjCBTmC2OmXbX6JiVJB20M_EoMTCHQmGbWqynq1OmpkStL9k5hvxpkb5fzpJ2hRXEWV1FvCY9XlDaY0KmZ24iqCLxKx_CsGyKDk-RxRcWqUlmwljw6hyfVVQqrozZS0)

In the case where a `interNodeHostCertificate` is already present on the system. It is assumed that all other intended certificate provisioning has already occurred.

Failing that, the node will attempt to use a supplied `interNodeCa`, if present, to create its own `interNodeHostCertificate`. (_This permits k8s operators to use an externally generated CA for these interfaces mounted as a secret._)

Failing that, the node will look for an `initToken`. If present it will use this to mutually authenticate with peers and coordinate generation of a common `interNodeHostCa`, relevant interface CAs, and subsequently its own `interNodeHostCertificate`.

**Interface Certificates**

Once nodes have established common trust, the same node that generates the `interNodeHostCa` will examine its filesystem for any interface CAs and craft any needed certificates that are not already present on its local filesystem (or analog). This will follow the below flow for each of the interface CAs:
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

Any CAs generated in this step will be included in the initial certificate bundle distributed to the rest of the nodes.

**Root User**
The node then checks for a `userAuthCA` containing a _public_ key. If found it continues with the remaining initialization path. If NOT found, if generates a `userAuthCA` and a `root` user certificate and emits both to the filesystem.

**Distribution**
The node upon satisfying the above checks and/or generation steps distributes the `initialization-bundle` to all peers before restarting to pick up all certificates per normal operation.

For peers that receive an `initialization-bundle`, they install the received certificates, generate their own host specific certificates and restart to begin using them on all relevant interfaces.

The cluster is now ready for a normal `init` instruction.

## Adding Nodes

**N.B.:** If a node starts and detects an `interNodeCa` but does not find a corresponding `interNodeHostCertificate` it will generate its own `interNodeHostCertificate` and use that to connect to a peer and request an 'initialization-bundle' to aquire any other CAs which it will then use to generate certificates for related interfaces with the same logic as above.

**Kubernetes**

In the kubernetes case, adding nodes is trivial if the `interNodeCa` is placed in the container of the joining node. The node starts, detects the CA, generates it's host certificate, then connects to a peer and asks for an `initialization-bundle`. After installing this and generating related certificates it restarts and is ready to function.

**Other orchestration (using interNodeCa )**
Identical to the kubernetes case except that the orchestration solution must place the `interNodeCa` on the node's filesystem.

**Manual (joinTokens)**
Any user with the permissions to add or remove nodes would have the ability to generate a `joinToken` via a SQL call. The `joinToken` would then be passed as an initial argument to the new node enabling it to prove membership and receive the `interNodeCa`, restart, and request provisioning (and populate its own certificates directory). The token would be consumed upon successful join to reduce risks associated with join token spillage.

**Adding New Regions**
To add a new region an operator may copy the `interNodeCa` to the nodes in the new region, allow them to initialize themselves, then join them. Since all nodes share the same `interNodeCa` there should be no additional complexity beyond any other multi-region concerns.


# Detailed Design



## Establishing Initial Node Trust

_Reference implementation here:_ https://github.com/aaron-crl/toy-secure-init-handshake/tree/n-way-join

Internode trust is established though a bootstrapped TLS process. Nodes provided with the right constraints and an `initialization-token` will take the following steps:
* generate a self-signed CA
* use that CA to generate a service certificate signed by that CA
* Write these certificates to local storage to enable a node to survive a restart
* start a listener using that certificate that hosts two endpoints
  * the CA public key and a MAC generated using the shared secret
  * an `initialization-bundle` receiver
* Nodes will also make **insecure** TLS connection requests to all peers to request their CA public key and MACs (in the reference implementation this is a bidirectional share to reduce the number of required connections)
* Nodes will verify the MAC for each peer and (if correct) add the CA to a peer address map (it may be prudent to store this map to disk to reduce brittleness)

Once all nodes have exchanged public CA keys*, the node with the "lowest" certificate signature value self nominates to generate the `initialization-bundle`. Once this bundle is created, this node uses the public key for each peer's CA to create strong TLS connections to each peer using their trusted CA and deliver the `initialization-bundle` in addition to a MAC of the bundle generated with the `initialization-token` to prove authenticity.

\* Drawing from the reference implementation; all peering above establishes bidirectional trust meaning that once a node has the correct number of peers it can safely cease to respond to binding requests. If it has the "lowest" signature it has the trust materials to provision its peers and can immediately begin doing so. Otherwise it will wait until its lowest signature peer has finished binding to all of its other peers.

**initialization-token**
The initialization token can be any string of bytes though for security it is recommended that they be of a adequate length and entropy to resist guessing for the duration of the initialization period.

### Initialization Failure states

In general the cluster is fragile during binding and operators should watch for any unanticipated restarts or container failures during this critical period. We're enumerated some failure states that may occur during binding and provisioning. This list is not exhaustive but may be useful to an operator. 
1. Node dies before writing CA and leaf to disk.
Same node or a replacement is spun up to replace it.
2. Node dies between certificate generation and binding
Node restart adequate
3. Node dies with some binds complete, without loss of storage
Node restart should be adequate
4. Node dies mid-bind after delivering certificate to server but before storing the server's CA
Potentially dead cluster. Restarted node will attempt to rebind and should succeed as long as other nodes are still attempting to bind.
5. Node dies with some binds complete, loss of storage
Dead cluster: wipe autogenerated certificates and restart all nodes
6. Node dies before provisioning takes place but with all binds complete, no loss of storage
Node restart adequate
7. Node dies before provisioning takes place but with all binds complete, loss of storage
Dead cluster: wipe autogenerated certificates and restart all nodes
8. Provisioner dies after partial provisioning
Broken cluster, possible to repair by manually copying certificates to remaining nodes but not recommended.

**N.B.:** Since at this point all nodes should be provisioned and functional, it may be reasonable to have the same node that generates the `initialization-bundle` also begin the KV init process.

### Kubernetes Considerations

The use of an `initialization-token` may reduce the deployment complexity for kubernetes by providing the token along with other node starting arguments. After initialization this flag will be ignored by members of this initial set since they will already have the expected certificates installed.

Certificates are expected to be stored in a node specific r/w persistent mount. Care should be used to ensure that this mount is adequately protected as access to these certificates may translate to database access.

**Example configurations:**
1. Eternally signed SQL certificate but nodes self manage internode trust:
Deploy containers with the signed SQL certificates in place and supply a `initialization-token`. The nodes will provision the rest themselves.
2. Intermediate CA scoped just to CRDB cluster for the `interNodeCa` with a permissive security policy:
Deploy nodes with that CA mounted and have nodes automatically generated their host certificates.
3. Wildcard SQL certificate (public, private keys), Corporate CA (public key only), self manage `interNodeCa`
Mount SQL cert and key, Corporate CA cert, and start nodes with `initialization-token`. Node will self provision internode CA, host using provided SQL certificates, and only accept users with certificates signed by the Corporate CA.

## Joining a Cluster

Nodes must prove membership to join a cluster. They also must ensure they are communicating with a trusted endpoint. This is affected with a join token that provides the material for validation of the CA cert and proof of valid join token via HMACs. The tokens are ephemeral and single-use to reduce risk associated with spillage.

**Establishing client/server trust and a new provisioning service**

**N.B.:** _This process requires the addition of two unauthenticated provisioning endpoints that behave similiarly to the cluster initialization service._

We shoud make this an optional endpoint to avoid exposing additional attack surface on clusters that do not use this feature.

A reference implementation is available here: https://github.com/aaron-crl/secure-join-handshake

The provisioning service does not require mTLS. It has:
* An endpoint that responds to HTTPS GET requests with the PEM encoded CA public key.
* An endpoint that respondes to HTTPS POSTs and provides a valid cluster CA bundle. These requests must contain a `proof-of-membership`.

**New Term:** `proof-of-membership`
Proof of membership is a struct containing `tokenId`, and a `sharedSecret`.
```
type addJoinProof struct {
	TokenID      []byte // tokenID of joinToken
	SharedSecret []byte // sharedSecret
}
```

**Generating a joinToken**
Any existing node may be used to generate a `joinToken` if the user has appropriate permissions.

**N.B:** _This will require a new database permission._

On existing node:
- A user or operator invokes a new token request.
- This node generates an entry in the `joinTokens` capturing
```golang
type joinToken struct {
	tokenID      uuid.UUID // Generated at time of token creation
	sharedSecret []byte    // 32 byte crypto-rand string
	expiration   time.Time // configurable time
}
```
- The node then computes an authentication fingerprint of HMAC(interNodeCA.PublicKey, sharedSecret) and returns the that concatinated with the sharedSecret and a one byte checksum in a copy/paste friendly encoding (possibly base62) as the `joinToken` to the invoker as a single string.
```golang
// Example using hex encoding and a truncated checksum
func (jt joinToken) Marshal(caPublicKey []byte) string {
	id, _ := jt.tokenID.MarshalBinary()
	authFingerprint := computeHmac256(caPublicKey, jt.sharedSecret)
	token := append(id, authFingerprint...)
	token = append(token, jt.sharedSecret...)
	cSum := crc32.ChecksumIEEE(token)
	token = append(token, byte(cSum)) // + 1 Truncated checksum
	return hex.EncodeToString(token)
}
```

**Upon new node launch:**
- The new node will be provided a `joinToken` or the location of a file containing it with its start flags.
- The node will request the `interNodeCa` public key from a peer
- The node will recompute the authentication fingerprint from above key and `sharedSecret` from its `joinToken` to confirm it has connected to a trusted endpoint.
- If validation succeeds
    0. The new node will add this CA to its CA pool
    1. The new node will compute its `proof-of-membership` as detailed above
    2. The new node will create a secure TLS connection to a peer's provisioning service and request an `initialization-bundle` by sending its `token-uuid`, and `proof-of-membership`.
        - The server will:
            - Look up the token associated with this `token-uuid`
            - Confirm the `proof-of-membership` by checking the `sharedSecret` stored in the `joinTokens` table with the specified `token-uuid`
            - Upon success the server will collect and return an `initialization-bundle` to the new node.
            - The server will delete the `joinToken` from the `joinTokens` table to avoid reuse.
    3. The new node installs the `initialization-bundle` to its local private storage, mints any required host certificates, then restarts into an operational state.

**N.B.:** Before checking for `token-uuid` presence in the `joinToken`'s table the system checks expiration for all unexpired `joinToken`s then proceeds to check node supplied `token-uuid` against available valid tokens. It is expected that this is an infrequent operation and a sparse table allowing us to bear this pruning cost on access as opposed to as part of scheduled maintenance. This check should also probably be atomic to avoid potential pruning races. This process should probably also log and remove expired tokens to keep the table small.

**Certificate rotation and `joinToken`'s**
Because `joinToken`s are pinned to the cluster `interNodeCa` at time of creation, operators should avoid issuing `joinToken`s with lifespans that overlap with a rotation of the cluster CA. In the event that an otherwise valid `joinToken` is used after an `interNodeCa` is rotated it will fail to validate the certificate for any peer it attempts to join. This can remedied by reissuing a `joinToken` and using it to restart the joining node. The defunct `joinToken` will be pruned after its marked expiration naturally.

### What does it do

This creates new functionality by which an operator or orchestration solution may create a fully secured cluster and/or add nodes without directly managing certificates. In order to do this it makes use of a pre-shared secret provided to a node or nodes at start which they may use to establish mutual trust.

### How it works (generally)

This approach assumes that a malicious entity may manipulate any traffic between all nodes at any time. The first step is bootstrapping the shared secret to mTLS.

Several approaches were explored to accomplish this. Discussion summarized below.

**Deterministic certificate generation from the `initToken`**
This appeared to work well and could have providedd for a simple common format between the init and add/join functions as all endpoints would be able to start in an mTLS-ready state. However, the Golang authors agressively protect their cryptologic implementation against inadvertent insecure misuse and include traps to prevent exactly this type of approach within the certificate generation libraries. Reference: [Maybe read a byte - how Go crypto library prevents you from getting overdependent on it](https://nogoegst.net/post/maybereadbyte/).

**Encoding an initial CA _as_ the join token**
This approach would simplify the implementation logic at a usability and add/join security cost.
* Simpler since the nodes could just use the CA to configure trust, then rotate it. However, the token would be hundreds of bytes long making text manipulation difficult and error prone.
* Less secure for the add/join case as a valid CA would have much greater potential splash damage if spilled as opposed to a one-time-use token.

**Use of shared secret as a signing value** 
This has the advantage of not requiring any format or length (beyond basic guessability constraints).

For the init case it can be used to establish mutual trust by signing individual node self-signed CAs but is rendered inert the moment the cluster finishes adding initial nodes.

For the add join case, a slight varient of this approach including a token UUID and signature for the existing cluster CA is needed.

**Kubernetes**

A kubernetes case may either make use of the `initToken` and add/remove function (using valid credentials) or simply copy the valid `interNodeCa` from an existing node to the container of the joining node.

**Manual Deployment**

[TBD]

## Drawbacks

The cluster can reach a state of deadlock if the node that generates all interface certificates dies before it comes back online.

The initialization process may also deadlock if any node dies before establishing mutual trust.

The process will fail to provide a full set of joined nodes if a node dies between establishing mutual trust and provisioning of a common `interNodeCa`.

An operator will need to monitor node health to determine if any of these have occurred. Mercifully they should all manifest during a single short window.

## Rationale and Alternatives

**Why not create a multi-use join token, then it could share the same characteristics as the init token and simplify both code flows?**
Join tokens are consumable by design to decrease the risk associated with transporting and using them. It's expected that join tokens may appear in kubernetes or other orchestration solution logs making it highly desirable that they are rendered inert upon use.

The same is roughly true for the init token set which is why init tokens are only used to bootstrap mTLS for the initial node set before they too become inert.

**What can't we use the same token for both?**
We can but it creates an increased denial of service attack surface.

In the current proposal, the init token can be any string of bytes. However, in order to make join tokens consumable, valid tokens must be stored somewhere. If the joining node does not supply an ID to find a valid token, the receiving cluster would be responsible for testing every valid token against the client join request. In practice this would mean an unauthenticated attacker could force a target cluster to repeatedly query the database for valid join tokens and compute hashes for each one against all incoming join requests.

In addition to the above denial of service angle, the joining node will need a means to identify that it has connected to a node via valid TLS. Otherwise an attacker could man-in-the-middle the exchange of node proof of membership and receipt of the CA enabling the attacker to mint valid node certificates and join the cluster.

The proposed mitigations are to include a UUID token ID with the valid token and a cryptologic hash of the inter-node CA public key. This allows receiving nodes to check for presence of a valid UUID (only) before undertaking hashing and validation work, and allows the joining node to verify that the certificate of the node it is connecting to was issued using the same CA that was signed by its shared secret.

## Success monitoring

Success will present itself through:
- A decrease in the percentage of clusters running in insecure mode.
- Reduce operator toil and friction easing adoption of the product where proof of concepts may have been conducted with the --insecure flag.


## Unresolved questions
- It may also be worth adding a startup option that generates a non-`root` _user_ and _generated password_ to facilitate local testing and development.
- How does this interact with the existing init functionality?
- What will the exact format and content of the join token be? (base62 string)
- Can we just store the node provisioning certificates in the database permanently instead of writing them to external storage?
- If the operator elects to use a kubernetes secret for the `interNodeCa` they will be responsible for rotating it as that path may not be writable.
- Does it make sense to attempt to document the multiregion pattern here?