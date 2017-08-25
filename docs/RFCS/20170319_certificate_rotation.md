- Feature Name: certificate rotation
- Status: completed
- Start Date: 2017-03-18
- Authors: @mberhault
- RFC PR: [14254](https://github.com/cockroachdb/cockroach/pull/14254)
- Cockroach Issue: [1674](https://github.com/cockroachdb/cockroach/issues/1674)

# Summary

This RFC proposes changes to certificate and key management.

The main goals of this RFC are:
* support addition and use of new certificates and keys without node restart
* decouple CA from node certificate rotation
* simplify generation and use of certificates and keys

Out of scope for this RFC:
* certificate revocation
* certificate/key deployment (aka: getting the files onto the nodes/clients)
* automatic certificate renewal
* use of CSRs (certificate signing requests)

# Motivation

Anytime certificates and keys are used, we must allow for rotation,
either due to security concerns or standard expiration.

This RFC is concerned with rotation due to expiration, meaning we do not
consider certificate revocation.

Our current certificate use allows for a single CA certificate and a single
server certificate/private key pair, with node restarts being required to
update any of them.

We wish to be able to push new certificates to nodes and use them
without restart or connection termination.

# Certificate expirations

CA and node certificates need radically different lifetimes to allow new
node certificates to be rolled out rapidly without waiting for CA propagation
to all clients and nodes.

We propose the following defaults:
* 5 year expiration on CA certificates
* 1 year expiration on node certificates

This may not always be appropriate. See "unresolved questions".

## Expiration monitoring

To provide enough warning of potential problems, each node should record
and export the start/end validity timestamp for each type of certificate:
* the CA certificate
* the combined client/server certificate issued to user `node`

If two certificates of the same type are present (eg: two CA certificates), report
the timestamps for the certificate with the latest expiration date.

With such metrics, we can now alert on expiring certificates, either with a fixed lifetime
remaining or when a fraction of the lifetime has expired.

It may be preferable to monitor certificate chains rather than individual certificates
(see Known Issues below) and report validity dates for the latest valid chain.

# Certificate and key files

## Storage location

The location of certificates and keys can be specified using the `--certs-dir` command-line
flag or the corresponding `COCKROACH_CERTS_DIR` environment variable.

The flag value is a relative directory, with a default value of `~/.cockroach-certs`.

We avoid using the `cockroach-data` directory for a few reasons:
* the certs must exist before cockroach is run, making the directory less discoverable
* wiping a node would wipe certs as well

All files within the directory are examined, but sub-directories are not traversed.

## Naming scheme

We propose the following naming scheme:
`<prefix>[.<middle>].<extension>`

`<prefix>` determines the role:
* `ca`: certificate authority certificate/key.
* `node`: node combined client/server certificate/key.
* `client`: client certificate/key.

`<middle>` is required for client certificates, where `<middle>` is the name
of the client as specified in the certificate Common Name (eg: `client.marc.crt`).
For other certificate types, this may be used to differentiate between multiple versions of a similar
certificate/key. See "unresolved questions".

`<extension>` determines the type of file:
* `crt` files are certificates
* `key` files are keys

## Permissions and file types

The only check is for the key to be read-write by the current user only (maximum permissions of `-rwx------`).

We need to provide admins with a way to disable permissions checks, due both to incompatible
certificate deployment methods, and incompatible filesystems/architectures. An environment variable
`COCKROACH_SKIP_CERTS_PERMISSION_CHECKS` with a stern warning should be sufficient.

# Certificate creation and renewal

## CA certificate

Initial CA creation involves creating the certificate and private key.
* `ca.crt`: the CA certificate, valid 5 years. Provided to all nodes and clients.
* `ca.key`: the CA private key. Must be kept safe and **not** distributed to nodes and clients.

CA renewal involves creating a new certificate using either the same, or a new private key.
All valid CA certificates need to be kept as well as their corresponding keys:
* append the new certificate to the existing `ca.crt` (optionally removing expired certificates along the way).
* store the new key in a new file.

The updated `ca.crt` file must be communicated to all nodes and clients.
The `ca.key` must still be kept safe and **not** distributed to nodes and clients.

When signing node/client certificate, if multiple CA certificates are found inside `ca.crt`, the
certificate matching the private key will be used. If multiple such certificates exist, the one with
the latest expiration date will be used.

## Node/client certificate

The trusted machine holding the CA private key generates node certificates and private keys, then
pushes them to the appropriate nodes.
Keys are not kept by the trusted machine once deployed on the nodes.

Generated node/client certificates have a shorter default lifetime than CA certificates (see "Certificate Expiration" section). Furthermore, their expiration date cannot exceed the CA certificate expiration.

Upon renewal, certificates and keys are fully re-generated, with no attempt to re-use the private node/client key.
Filenames for node/client certificates and keys can remain the same as before, or be new files.

# Reloading certificate/key files

## Triggering a reload

Running nodes can be told to re-read the certificate directory by issuing a `SIGHUP` to the process.

Since we cannot control when nodes may be restarted, it is important to keep the reload process
identical to the initial load.

## Validating certificates

Node certificates must be checked for validity. Specifically:
* we must have a valid certificate/private key pair
* the certificate must currently be valid (`Not Before < time.Now() < Not After`)
* the certificate must be signed by one of the CA certificates on this node

The last condition is an attempt to avoid loading a certificate that may not be verifiable
by other nodes or clients. If we do not have the right CA, chances are someone else does not either.

We may need to set a timer for certificates that have not reached their `Not Before` date, otherwise
we would need to trigger a second refresh.

# Online certificate rotation

A good description of online key rotation in Go can be found in
[Hitless TLS Certificate Rotation in Go](https://diogomonica.com/2017/01/11/hitless-tls-certificate-rotation-in-go/)

Adding or swapping certificates can be done in multiple ways:
1. construct a new `tls.Config` object
1. modify individual fields
1. implement callbacks corresponding to individual fields

The `tls.Config` object is specified at connection time and cannot be modified after as it
is not safe for concurrent use.

A node needs to maintain two `tls.Config` objects, one for server-side connections, one for client-side connections. A new config can be constructed upon reload, then reused for all subsequent connections.

The server-side `tls.Config` object can be specified for each client connection by implementing
the `tls.Config.GetConfigForClient`. This should return the most recent `tls.Config` object.`

## Adding a new CA certificate

Root CAs for server and client certificate verification are in `tls.Config.RootCAs` and `tls.Config.ClientCAs` respectively. We should add all detected CA certificates to both pools.

## Rotating node/client certificate

The node and client certificates are set in `tls.Config.Certificates`. 
If more than one node certificate is present, the one matching the requested `ServerName` is presented.
We should set only one certificate in `tls.Config.Certificates`.

# Additional interfaces

## Command line

We will need to modify all commands that use certs to make use of the new directory structure.

We will also need:
* modification to `cert create-ca` to use an existing key.
* `cert list` to list all CA certs and node/client cert/key pairs.

## Admin UI / debug pages

We want at least barebones listing of all certs on a given node, including validity
dates, certificate chain (corresponding CA for a node cert), and valid hosts.

Soon-to-expire certificates (or chains) must be reported prominently on the admin UI and
available through external metrics.

# Future work

## Packaging certificates for other languages/libraries

Separate `.crt` and `.key` files are expected by libpq, but other libraries/languages may have different ways of specifying/packaging them.

We need to:
* augment our per-language examples to include secure mode. see [docs/631](https://github.com/cockroachdb/docs/issues/631)
* document how to use public tools (ie: openssl) to convert certificates.
* provide multiple cert/key output modes for the `cockroach cert` commands.

## Alternate reload methods

Some additional methods to trigger a reload can later be introduced:
* a timer based on certificate expiration
* regular timer
* admin UI endpoint

## Client certificate monitoring

We have no way of knowing which certificate authority a client has, so we cannot monitor for
clients not yet aware of a new CA certificate.

We could examine client certificates and report soon-to-expire ones. This will not help
with CA knowledge, but would provide better visibility into user authentication issues.

# Unresolved questions

## `tls.Config` fields

We need to verify that the proposed CA and node cert rotation mechanisms work, especially through
grpc.

Since everything uses `tls.Config`, implementing `tls.Config.GetConfigForClient` to rotate
the config on the server should be sufficient,

However, we need to ensure that all client-side connections are able to use the new config when
initiating a connection.

## Renegotiation and certificate rotation

Renegotiation may cause new certificates to be presented. We need to make sure this will not
cause issues.

The `tls.Config` comments also mention this happening in TLS 1.3:
```
GetClientCertificate may be called multiple times for the same
connection if renegotiation occurs or if TLS 1.3 is in use.
```

## Multi-certificate DER files and postgres clients

The Go `lib/pq` will use all certificates found in `ca.crt`, but this may not be the
case of other libraries.
It may be safer to keep a single CA certificate per file.

## Allow use of multiple certs directories

Instead of the `--certs-dir` flag being a single directory, we could allow specification of
multiple directories. This would be useful to separate CA certificates from other certs.

## File permissions

Is checking for `-rwx------` on keys sufficient? A more stringent check would be similar
to what the ssh client does (strict directory/file permissions).

## Multiple versions of the same certificate

Should we allow multiple versions of the same certificate? eg: multiple files matching `ca.*.crt` or `node.*.crt`? If so, how do we handle them?

## Certificate lifetime

We need to pick some defaults for certificate lifetimes.

The proposed ones may be inappropriate for most users: security-minded admins or those with
a good certificate-rotation process in place may want much shorter periods while casual
users may want to never be bothered by certificates.

# Drawbacks

## Monitoring certificates

Simply because both a CA certificate and a node certificate are valid does not mean they
are both correct. Consider the following scenario:
* we receive an alert for CA certificate expiring soon
* we lost the CA private key so generate a new cert/key pair
* we push the new CA certificate to all nodes
* alerts no longer fire

In this case, as long as the old CA is still valid, we can verify other node certificates.
However, as soon as the old CA expires, we will be unable to verify node certificates due
to the key change.

We could improve monitoring by analyzing the lifetime data for each certificate chain
on each node. This would notice that the old chain expires when the old CA expires, and the
new chain contains only a CA certificate, no node certificate.

If we record/export chain information by CA cert serial number (or public key), we can ensure
that all nodes have certificate chains rooted at the same CA.

## Multiple viable CA certs when generating node/client certs

When generating new node or clients certs, we may automatically detect multiple
valid CA certs in the certificate directory.
If two certs have the same key pair, we can pick the one expiring the latest.
If the keys differ, we want to throw an error and either ask the user to remove one, or
add an additional flag to force the cert (this partially defeats the point of automatic
file detection).

## Delegated users

Dropping specific cert flags means that the postgres URL will be built automatically from the
requested username.

For example, running the following command: `cockroach sql -u foo -certs-dir=~/.cockroach-certs` will
generate the URL: `postgresl://foo@localhost:26257/?sslcert=foo.client.crt&sslkey=foo.client.key&...`

If delegation is allowed (user `root` can act as user `foo`), the command must be run with the
fully-specified postgres URL `postgresl://foo@localhost:26257/?sslcert=root.client.crt&sslkey=root.client.key&...`

Delegation remains doable, but with an extra hoop to jump through.

## Determination of secure mode

With default values for certificate and key locations, secure mode is now less explicit, relying
on the detection of certificates in the default directory. This could be misleading to users.

# Alternative solutions

## Certificate/key file discovery

We have three major options to specify certificate and key files:

### Full file specification

This is the current method, all files are specified by their own flags.

Drawbacks:
* tedious. this can be alleviated by having default file names
* does not support multiple files. If renewal is done using new file pairs, these flags would need to be changed.
* using multiple CA certificates inside a single `ca.crt` file requires finding the certificate matching the key. This can be partially avoided by always putting the newest certificate first.

Advantages:
* simple code: we use the files as specified, relying on the standard library for mismatches.
* allows separate storage locations for certs (eg: CA in `/etc/...`, node certs in user directory).

### Globs

Command-line flags for filename globs (either per pair, or per file). Optionally, allow specification
or a certs directory, with globs matching files within the directory.

Drawbacks:
* how do we deal with multiple matching certificates? (eg: `node.old.crt` and `node.new.crt`)?
* if a glob matches multiple types of certs (eg: `*.crt` glob matches CA/node/client certs), do we just fail?
* same problem with multi-cert `ca.crt` files.
* if using a shared directory, does not allow separate storage locations.

Advantages:
* reasonably easy to code, especially if requiring single matches.
* if specifying per file-type globs, can handle separate storage locations.

### Automatically-determine files

Automatically determine file types (key vs cert) and cert usage (CA vs node vs client) by analyzing
all files in the certs directory.
Certificates can be determine by looking at `IsCA` or `ExtendedUsage`. The keys can be matched
to certificates by comparing algorithms and public keys.

Drawbacks:
* does not allow for separate storage location. We could allow specification of multiple certs directories.
* complex code: parses/analyses all certificates and keys. Need to make sure we mimic the standard library behavior to avoid surprises. Also need to evolve with the standard library (eg: key type support).
* unusual specification of certs/keys. Everyone else specifies files directly.
* "too much magic"

Advantages:
* full validation can provide user-friendly error messages on improper files (still obscure without decent knowledge of certficates).
* support for multiple ways of generating/deploying certificates and keys.
