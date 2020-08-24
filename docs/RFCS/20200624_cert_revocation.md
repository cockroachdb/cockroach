- Feature Name: TLS client certificate revocation
- Status: accepted
- Start Date: 2020-06-24
- Authors: knz, ben
- RFC PR: [#50602](https://github.com/cockroachdb/cockroach/pull/50602)
- Cockroach Issue: [#29641](https://github.com/cockroachdb/cockroach/issues/29641)

# Summary

This RFC proposes to introduce mechanisms to revoke TLS client
certificates when used by CockroachDB for authentication.

The motivation is to enable revoking certificates earlier than their
expiration date, in case a certificate was compromised.

This is also required for various compliance checklists.

The technical proposal has multiple components:

- it extends the TLS certificate validation code to support checking
  the cert against
  [OCSP](#online-certificate-status-protocol-ocsp) (explained below).

- it introduces code to fetch OCSP responses from the
  network and cache them.

- it introduces a cluster setting `security.ocsp.mode` which controls
  the strictness of the certificate validation.

- it reports on the status of OCSP responses in the
  `/_status/certificates` endpoint.

- it introduces a cluster setting to control the expiration of
  the cache:  `server.certificate_revocation_cache.refresh_interval`.

- it introduces an API endpoint `/_status/certificates/cached_revocations`,
  which, upon HTTP get requests, produces a report on all currently
  cached OCSP responses upon HTTP GET requests.

  (In a later phase, also available via SQL built-in function, pending
  [#51454](https://github.com/cockroachdb/cockroach/issues/51454) or
  similar work.)

- the same API endpoint also supports HTTP POST request to manually
  force a refresh.

**Note: as of August 2020, [PR
#53218](https://github.com/cockroachdb/cockroach/pull/53218)
implements a MVP of the checking logic. However it does not implement
caching as described in this RFC. The caching remains to be done.**

# Motivation

see above

# Background

## Online Certificate Status Protocol (OCSP)

OCSP is a network protocol designed to facilitate online validation of TLS certificates.
It performs the same role as CRLs but is intended to be lightweight in comparison.

The way it works is the following:

- upon observing a TLS cert for validation, the service extracts the cert's serial number.

- the service sends the cert's serial number to an OCSP server. The
  server's URL is known ahead of time (configured separately) or
  embedded in the client/CA certs themselves under the
  `authorityInfoAccess` field.

- the OCSP server internally checks the cert's validity against CRLs etc.

- the OCSP server returns a response to the service with status either
  `good`, `revoked` or `unknown`. The response itself is signed using
  a valid CA. The service must verify the signature in the OCSP response.

The cost to implement / validate a cert using OCSP is typically lower
computationally than using CRLs. The OCSP server typically caches
response across multiple requests.

Nevertheless, OCSP incurs a mandatory network round-trip upon every
verification. In CockroachDB it would be unreasonable to query OCSP
upon every incoming client connection. Therefore, for our purpose OCSP
does not obviate the need for a service-side cache.

References:

https://www.ssl.com/faqs/faq-digital-certificate-revocation/

https://jamielinux.com/docs/openssl-certificate-authority/online-certificate-status-protocol.html

https://en.wikipedia.org/wiki/Online_Certificate_Status_Protocol

# Guide-level explanation

After a cluster has been set up, a security event may occur that
requires the operator to revoke a client TLS certificate. This event
could be a compromise (i.e. certificate lands in the wrong hands) or
the deployment of corporate IT / compliance processes that requires a
revocation capability in all networked services.

For this CockroachDB offers the ability to check OCSP servers.
Technically OCSP is a network service that can validate certificates
remotely.

## Upon starting a CockroachDB node

No additional command-line flags are needed.

The TLS certificates are assumed to contain an `authorityInfoAccess`
field that points to their OCSP server.

## Manually revoking a certificate

To revoke a certificate, the operator should proceed as follows:

- add the revocation information to the OCSP database in the OCSP
  service configured via the `authorityInfoAccess` in certs.

- finally, force a refresh of the CockroachDB cache: invoke
  `crdb_internal.refresh_certificate_revocation()` in SQL or send an
  HTTP POST to `/_status/certificates/cached_revocations`.

The manual refresh is not required if the revocation is not urgent:
the cache is refreshed periodically in the background. The maximum
time between refreshes is configured via the cluster setting
`server.certificate_revocation_cache.refresh_interval`, which defaults to 24 hours.

Remark from Ben:

> OCSP responses include an optional `NextUpdate` timestamp field which
> defines the validity period of the response. If this is set, we may
> want to use it to set the cache expiration time. We should ask the
> customer how long they want the cache to be valid for (24h seems
> long to me; I'd expect a default more like an hour since this is
> opt-in for users who care about revocation) and whether they use
> this field.

## Checking the status of revocations

To inspect a CockroachDB node's opinion of revocations, use the
`/_status/certificates/cached_revocations` API endpoint.

# Reference-level explanation

## Detailed design

Each node implements a certificate verification broker.  Each
verification of a TLS cert for authn is changed to go through this new
broker component. An error from the broker causes the cert verification to fail.

The broker implements a cache internally. The cache is refreshed
periodically at the frequency set by the new setting
`server.certificate_revocation_cache.refresh_interval`. The refresh
protocol is explained further below.

## Changes to the HTTP endpoints

The Admin endpoint `/_admin/v1/certificate_revocation` maps to an RPC which
supports both Get and Post requests.

The Get version produces a representation of the cache. This supports
a "node ids" list argument like we have for other RPCs. When provided
the "local" ID it reports on the local cache only. When provided no
ID, it reports on the entire cluster. When provided a specific ID, it
reports the cache on that nodes. This logic uses the node iteration
code `status.iterateNodes()` that is already implemented, see
`EnqueueRange()` for an example.


The Post request forces a cluster-wide cache refresh. This is
explained below.

## Changes to SQL

A new built-in function
`crdb_internal.refresh_certificate_revocations()` also forces a
cluster-side refresh of the cache.  See below for details.

The name of the built-in remains to be
refined later, pending further investigation of
[#51454](https://github.com/cockroachdb/cockroach/issues/51454).

## Cluster-wide trigger for cache refreshes

A refresh of the revocation cache can be triggered from any
node. However, it's possible that a refresh be triggered from one node
while another node is disconnected/down/partitioned away. We want that
manual refresh requests do not get lost, especially when
`server.certificate_revocation_cache.refresh_interval` is configured
to a large interval (default 24 hours).

To achieve this, we define a new system config key `LastCertificateRevocationRefreshRequestTimestamp`.

Upon triggering the cache refresh, the node where the refresh was
triggerred writes the current time to this key. Gossip then propagates
the update to all other nodes. Eventually all nodes learn of the the
refresh request.

Concurrently, an async process on every node watches this config
key. Every time its value moves past the time of the last refresh on
that node, an extra refresh is triggered.

Ben's remark:

> If the cache is short-lived, we may be able to avoid creating a
> system to manually force a refresh (or make it testing-only and it
> doesn't need to handle disconnected nodes).

## Cache refresh process

For OCSP cache entries, all entries in the cache with status `good`
are flushed, so that a new OCSP request will be forced upon the next
use of the TLS certs.  All entries with status `revoked` are
preserved: any already-revoked cert is considered to remain revoked.

Refreshes and errors are logged.

## Cache queries during TLS cert verification

When a TLS cert is to be checked, the code asks the broker for confirmation.

The broker first checks that the certs are properly signed by their
CA. If that fails, the verification fails.

The broker then inspects the `security.ocsp.mode` cluster setting. If
set to `off`, then no further logic is applied.

Otherwise, the broker then inspects the certificate and its CA chain
to collect all the OCSP reference URLs. For every cert in the chain
where one of the parent CAs has an OCSP URL, the code checks the OCSP
response cache for that URL, cert serial pair. If there is an entry
with `good` or `revoked` already in the cache, that is used directly.

If there is no cached entry yet, the OCSP server is queried.  The response
from OCSP is then analyzed:

- if the OCSP response is badly signed, then the response is
  ignored. The cert validation fails.
- if the OCSP response is `revoked`, the cert validation
  fails.
- if the OCSP response is `good`, the cert validation succeeds.
- if the OCSP response is `unknown`, or there is an error, then the behavior
  depends on the new cluster setting `security.ocsp.mode`:

  - if `strict`, then cert validation fails.
  - if `lax`, then cert validation succeeds.


If the response was `revoked` or `good` (and properly signed), an
entry is added in the cache for the URL/serial pair.

## Drawbacks

None known

## Rationale and Alternatives

The following designs have been considered:

### Using CRLs instead of OCSP

#### Background about CRLs

A CRL is a list of X509 certs that mark other certs as "revoked", i.e. invalid.

Technically, a *revocation cert* is a cert signed by a recognized CA,
which certifies that another cert, identified by serial
number/fingerprint, has been revoked as of a specific date.

The certificate validation code should obey the revocation lists and
refuse to validate/authenticate services using certs that have a
currently known revocation cert in a CRL.

In a service like CockroachDB, authn certs are presented by the client
upon establishing new connections; whereas CRLs are configured
server-side and fed into the server on a periodic basis.

In practice, CRLs are fed using two mechanisms:

- *external*: the operator has one or more revocation certs as
  separate files, or a combined file containing multiple certs. Then
  the operator "uploads" the CRL into the service. This should be done in at least two ways:

  - upon start-up, to load an initial CRL before the various other
    sub-systems in the service are fired up.

  - periodically, to refresh the CRL with new revocations while the
    service is running. This can be done by "pull" (the service uses
    e.g. HTTP to fetch a CRL over the network) or "push" (the operator
    invokes an API in the server to upload the CRL into it).

- *internal*: each CA certificate can contain a field called
  `CRLDistributionPoints`. This field is a list of URLs that point to
  CRLs related to that CA.

  Services that support `CRLDistributionPoints` should fetch the CRLs
  prior to validating certs signed by that CA.

  A particular pitfall/chalenge with this field is that there may be
  multiple intermediate CAs, each with its own
  `CRLDistributionPoints`. Some of the CA certificates may be provided
  only during the TLS connection by the client, as part of the TLS
  client cert payload. So the CRL distribution points cannot generally
  be known "ahead of time" when a server starts up.

  References:

  https://www.pixelstech.net/article/1445498968-Generate-certificate-with-cRLDistributionPoints-extension-using-OpenSSL

  http://pkiglobe.org/crl_dist_points.html

### Solution outline

- There would be a command-line flag to read the initial CRL from a network
  service or the filesystem.

  The `--cert-revocation-list` flag is provided
  with a URL to a location that provides the revocation certs. This can
  either use a path to a local file containing the CRL, or a HTTP URL to
  an external service. This is optional if the CA certs are known to
  list their CRL URLs themselves.

  If the CRL is provided externally as a collection of discrete files,
  they can be combined together into a single file via an `openssl`
  command.

- To revoke a cert, an operator would add the revocation cert to the
  list of certs. This can be either a local file (when
  `--cert-revocation-list` points to a local file), or a CRL server
  (when `--cert-revocation-list` points to a URL, or when using the
  `CRLDistributionLists` field in CA certs).

- There would be a new SQL built-in function which an operator can use
  to refresh the CRLs from the network or the configured CRL local
  file: `crdb_internal.refresh_certificate_revocation()`.

- the API endpoint `/_status/certificates/cached_revocations` would return
  cached CRL entries.

- The cert validation would work as follows.

  The broker first checks that the certs are properly signed by their
  CA. If that fails, the verification fails.

  The broker then inspects the certificate and its CA chain to collect
  all the CRL distribution URLs, and merges that with the URL configured
  for `--cert-revocation-list`.

  For each URL in this list it checks if it has an entry in the URL ->
  timestamp map. If it does not (URL not known yet), or if the timestamp
  is older than the configured refresh interval, it fetches that URL
  psynchronously and updates the cache with the results. If there is an
  error, the URL -> timestamp map is not updated and the TLS cert
  validation fails.

  If a revocation cert is found in the CRL for either the leaf cert of
  any of its CA certs in the chain, TLS cert validation also fails.

- The background cache refresh process would work as follows: it would periodically re-load
  the file / URL configured via `--cert-revocation-list`. New revocation
  certs are added to the cache. Existing revocation certs are left
  alone.

  There is a separate CRL refresh timestamp maintained by the cache
  for each CRL URL (a map URL -> timestamp). For each URL the timestamp
  is bumped forward, but only if there was no error during the CRL
  refresh. If there was an error, the refresh timestamp for that URL is
  not updated, so that the async task that monitors refreshes tries that
  URL again soon.



Rejected idea: Store the CRL in a table and query it upon every authn request.

  - The CRL becomes unavailable if the cluster is in a sad state.
  - Causes a KV lookup overhead and a hotspot upon connections.


## Unresolved questions

N/A
