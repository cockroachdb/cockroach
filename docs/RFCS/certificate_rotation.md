- Feature Name: certificate rotation
- Status: draft
- Start Date: 2017-03-18
- Authors: @mberhault
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [1674](https://github.com/cockroachdb/cockroach/issues/1674)

# Summary

This RFC proposes changes to certificate and key management.

The main goals of this RFC are:
* support addition and use of new certs without node restart
* decouple CA from node certificate rotation
* simplify generation and use of certificates and keys

Out of scope for this RFC:
* certificate revocation
* certificate/key deployment
* automatic certificate renewal

# Motivation

Anytime certificates and keys are used, we must allow for rotation,
either due to security concerns or standard expiration.

This RFC is concerned with rotation due to expiration, meaning we do not
consider certificate revocation.

Our current certificate use allows for a single CA certificate  and a single
server certificate/private key pair, with node restarts being required to
update any of them.

We wish to be able to push new certificates to nodes and use them
without restart or connection termination.

# Certificate expirations

CA and node certificates need radically different lifetimes to allow new
node certificates to be rolled out rapidly without waiting for CA propagation
to all clients and nodes.

We propose:
* 5 year expiration on CA certificates
* 1 year expiration on node certificates

To provide enough warning of potential problems, each node should record
and export the start/end validity timestamp for each type of certificate:
* the CA certificate (has contraint `CA:TRUE`)
* the combined client/server certificate issued to user `node`

If two certificates of the same type are present (eg: two CA certificates), report
the timestamps for the certificate with the latest expiration date.

With such metrics, we can now alert on expiring certificates, either with a fixed lifetime
remaining or when a fraction of the lifetime has expired.

It may be preferable to monitor certificate chains rather than individual certificates
(see Known Issues below) and report validity dates for the latest valid chain.

# Certificate creation and renewal

## CA certificate

Initial CA creation involves creating the certificate and private key.
* `ca.cert`: the CA certificate, valid 5 years. Provided to all nodes and clients.
* `ca.key`: the CA private key. Must be kept safe and **not** distributed to nodes and clients.

CA renewal involves creating a new certificate using the same private key.
We have two options:
* add the new certificate to `ca.cert` and provide the expanded file to all nodes and clients.
* create a new certificate (eg: `ca.new.cert`) and provide it to all nodes and clients.

The key **should not** be regenerated on certificate renewal. If it is (human error or lost key),
all client and node certificates will need to be regenerated and signed with the new key.

## Node certificate

The trusted machine holding the CA private key generates node certificates and private keys, then
pushes them to the appropriate nodes.
Keys are not kept by the trusted machine once deployed on the nodes.

Upon renewal, filenames should differ allowing multiple sets of certificate/private key files
to exist simultaneously.

# Reloading certificate/key files

The current method for providing certificates and private keys requires naming them specifically
using flags or environment variables:
* `--ca-cert`, `--ca-key` for CA certificate and key
* `--cert`, `--key` for node or client certificate and key

We can use a single file for CA certificates (see CA certificate renewal above) but we require
multiple files for node and client certificates due to changing keys.

We thus need to be able to specify a storage location containing an arbitrary number of
certificates and keys.

We propose a single directory containing all relevant certificates and private keys for
the current node or client.

## Directory

The directory can be specified by command-line flag `--certs-dir` or environment variable
`COCKROACH_CERTS_DIR`, with a default value of `cockroach-certs`.

The permissions on the directory cannot be more permissive than `drwx------`.
Directory ownership will not be checked (too much incompatibility across filesystems and
architectures), relying instead on permissions to list the directory contents.

## Files

File requirements are as follows:
* all files must be in the `COCKROACH_CERTS_DIR` directory, sub-directories are ignored
* all files must be plain files, no sym-links allowed
* maximum permissions for certificates: `-rwxr-xr-x`
* maximum permissions for keys: `-rwx------`
* all files are PEM-encoded

File types and functions are determined automatically into categories by
checking the following PEM or certificate information:

| `pem.Block.Type` | `IsCA` | `ExtendedKeyUsage` | `Subject.CommonName` | Category |
|---|---|---|---|---|
| `CERTIFICATE` | `TRUE` | `CertSign | ContentCommitment` | Any | CA Certificate |
| `CERTIFICATE` | `FALSE` | `ServerAuth | ClientAuth` | `Node` | Node certificate |
| `CERTIFICATE` | `FALSE` | `ClientAuth` | not `Node` | Client certificate |
| `PRIVATE KEY` | | | | Private Key |

Any other combination (eg: `ExtendedKeyUsage = ServerAuth | ClientAuth` and `User = foo`) is invalid.

Certificates and private keys can be matched by checking public-key components:
* `rsa.PublicKey.N.Cmp(rsa.PrivateKey.N) == 0` for RSA keys.
* `ecdsa.PublicKey.X.Cmp(ecdsa.PrivateKey.X) == 0 && ecdsa.PublicKey.Y.Cmp(ecdsa.PrivateKey.Y) == 0` for ecdsa keys.

Details of PEM parsing and key checking can be found in [tls.X509KeyPair](https://golang.org/src/crypto/tls/tls.go?s=5915:5986#L183)

CA certificates can be added to the certificate pool with little extra validation.

Node certificates must be checked for validity. Specifically:
* we must have a valid certificate/private key pair
* the certificate must currently be valid (`Not Before < time.Now() < Not After`)
* the certificate must be signed by one of the CA certificates on this node

The last condition is an attempt to avoid loading a certificate that may not be verifiable
by other nodes or clients. If we do not have the right CA, chances are someone else does no either.

We may need to set a timer for certificates that have not reached their `Not Before` date, otherwise
we would need to trigger a second refresh.

## Triggering a reload

Running nodes can be told to re-read the certificate directory by issuing a `SIGHUP` to the process.

Since we cannot control when nodes may be restarted, it is important to keep the reload process
identical to the initial load.

# Online certificate rotation

A good description of online key rotation in Go can be found in
[Hitless TLS Certificate Rotation in Go](https://diogomonica.com/2017/01/11/hitless-tls-certificate-rotation-in-go/)

## Adding a new CA certificate

All TLS connections are given a [tls.Config](https://golang.org/pkg/crypto/tls/#Config) object
containing a [*x509.CertPool](https://golang.org/pkg/crypto/x509/#CertPool) to verify
server certificates (`tls.Config.RootCAs`) and client certificates (`tls.Config.ClientCAs`).

Adding a new certificate to the pool can be done as long as we have access to the `tls.Config` or
the `x509.CertPool`. Neither is safe for concurrent use so we will need a small wrapper to allow
safe modification of the pool while serving connections.

## Rotating node certificate

Node certificates are tricker than CA certificates as we must pick exactly one to present.

The `tls.Config` object allows us to specify node certificates in two ways:
* set the certificate in `tls.Config.Certificates []Certificate`
* set a per-connection callback in `tls.Config.GetCertificate func(*ClientHelloInfo) (*Certificate, error)`

Modifying either will change which certificate is presented at session initiation or renegotiation.
The `Certificates` object is not safe for concurrent use, making `GetCertificate` preferable.

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

# Unresolved questions

## `tls.Config` fields

We need to verify that the proposed CA and node cert rotation mechanisms work, especially through
grpc.
We may be able to modify the `tls.Config.Certificates` field, but this would require a wrapper
to allow concurrent use.
On the other hand, using the `tls.Config.GetCertificate` callback mechanism **seems** to work
(tested superficially by mberhault on 2017/03/18).

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

For example, running the following command: `cockroach sql -u foo -certs-dir=cockroach-certs` will
generate the URL: `postgresl://foo@localhost:26257/?sslcert=foo.client.crt&sslkey=foo.client.key&...`

If delegation is allowed (user `root` can act as user `foo`), the command must be run with the
fully-specified postgres URL `postgresl://foo@localhost:26257/?sslcert=root.client.crt&sslkey=root.client.key&...`

Delegation remains doable, but with an extra hoop to jump through.

## Determination of secure mode

With default values for certificate and key locations, secure mode is now less explicit, relying
on the detection of certificates in the default directory. This could be misleading to users.

## Client knowledge of certificate authority

We have no way of knowing which certificate authority a client has, so we cannot monitor for
clients not yet aware of a new CA certificate.

We could examine client certificates and report soon-to-expire ones. This will not help
with CA knowledge, but would provide better visibility into user authentication issues.

## Filesystem permissions/ownership

The directory and file structure assumes that the permission checks are sufficient to ensure
that the files cannot be modified by another user. This may not be the case if the certificate
directory is on a filesystem or architecture where user permission semantics differ.
Should this be the case, the explicit file-listing method is similarly compromised (worse because
we do not currently check file permissions).
