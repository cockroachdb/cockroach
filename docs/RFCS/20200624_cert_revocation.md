- Feature Name: TLS client certificate revocation
- Status: draft
- Start Date: 2020-06-24
- Authors: knz
- RFC PR: [#50602](https://github.com/cockroachdb/cockroach/pull/50602)
- Cockroach Issue: [#29641](https://github.com/cockroachdb/cockroach/issues/29641)

# Summary

This RFC proposes to introduce a certificate revocation list (CRL) in
CockroachDB to validate against TLS client certificates used to
authenticate SQL client connections.

The motivation is to enable revoking certificates earlier than their
expiration date, in case a certificate was compromised or the user
account is temporarily blocked from access (e.g. account suspension).

This is also required for various compliance checklists.

The technical proposal is twofold:

- introduce a new cluster setting containing
  the revocation list: `server.user_login.certificate_revocation_list`.
  This is the main CRL store used during authentication.

- introduce two additional cluster setting to refresh
  the primary setting above periodically:
  `server.user_login.certificate_revocation_list.update_url`,
  `server.user_login.certificate_revocation_list.update_interval`.

# Motivation

see above

# Guide-level explanation

After a cluster has been set up, a security event may occur that
requires the operator to revoke a client TLS certificate. This event
could be a compromise (i.e. certificate lands in the wrong hands) or
the deployment of corporate IT / compliance processes that require
CRLs in all networked services.

Technically a CRL is a file containing one or more *revocation
certificates* encoded using a standard format (x509 of some sorts).

If the CRL is provided externally as a collection of discrete files,
they can be combined together into a single file via an `openssl` command.

Once a single-file CRL must be loaded into CockroachDB, the operator can do either of the following:

- they can set the cluster setting
  `server.user_login.certificate_revocation_list` manually to the
  contents of their CRL.

  When updating the list manually, the operator should ensure they
  have a process to update this list whenever their CRL changes.

- or, they can set up a web server in their infrastructure that
  is reachable from CockroachDB nodes, and have that server serve
  the CRL at a particular URL. Then, they can set the cluster setting
  `server.user_login.certificate_revocation_list.update_url` to
  the location of the CRL.

  The companion setting
  `server.user_login.certificate_revocation_list.update_interval`
  determines how often CockroachDB fetches a new CRL from the
  URL. This setting is set to **1 hour** by default.


Note:

- if the operator sets the `server.user_login.certificate_revocation_list` manually,
  and then configures `...update_url`, their manual configuration will be overwritten
  the first time a CRL can be fetched successfully from the URL.

- if an URL fetch is unsuccessful, a message will be logged to indicate this fact,
  and the CRL in `server.user_login.certificate_revocation_list` will be left
  unmodified.

- any changes to `server.user_login.certificate_revocation_list`
  (either manual or automatic) are logged in the cluster's event log
  table.

- when RESTOREing an entire cluster, the CRL stored in the backup will be restored.
  It is advised to ensure the CRL is refreshed at least once after a RESTORE
  before clients are allowed to try and connect to cluster nodes.

# Reference-level explanation

## Detailed design

- 3 settings as defined above.

- a background process to refresh the CRL from a URL.

A cluster setting as the main store for the CRL answers two needs:

- the setting can be modified cluster-wide even if the KV layer is
  unavailable (changes propagate by gossip)

- settings are automatically persisted across restarts.

We also have precedent for this approach:
`server.host_based_authentication.configuration` which was designed in
a similar way for the same reasons.

## Drawbacks

None known

## Rationale and Alternatives

The following designs have been considered:

- Store the CRL in a table and query it upon every authn request. Rejected:

  - The CRL becomes unavailable if the cluster is in a sad state.
  - Causes a KV lookup overhead and a hotspot upon connections.

- Store the CRL in a table and introduce an in-RAM "eventually
  consistent" cache refreshed periodically. Rejected:

  - Impossible to update the CRL while the table is unavailable.

- Store the CRL in-RAM only and refresh from an external URL. Rejected:

  - Creates a window of vulnerability if a node is restarted while the
    URL is unreachable.
  - Impossible to override the CRL if the URL becomes unreachable.
  - Too much risk that different nodes will see different CRLs. One
    node can become an attack vector because its CRL is outdated.

- Store the CRL on-disk in files, pass the directory via a command-line flag. Rejected:

  - Too much risk that different nodes will see different CRLs. One
    node can become an attack vector because its CRL is outdated.

  - Makes administration hard under orchestration.

## Unresolved questions

- Do we ping the URL from every node in the cluster periodically, or just
  from one? If just one, how to "elect" it?

