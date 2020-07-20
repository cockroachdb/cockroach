- Feature Name: Password modernization
- Status: in-progress
- Start Date: 2020-07-20
- Authors: knz
- RFC PR: [#51599](https://github.com/cockroachdb/cockroach/pull/51599)
- Cockroach Issue:
  [#51601](https://github.com/cockroachdb/cockroach/issues/51601)
  [#50757](https://github.com/cockroachdb/cockroach/issues/50757)
  [#42519](https://github.com/cockroachdb/cockroach/issues/42519)


# Summary

This RFC proposes to modernize support for password authentication, so
as to reach some more PostgreSQL compatibility and enable more secure
deployments using password authentication.

There are 3 goals attained by this RFC:

1. define a way for CockroachDB nodes to recognize multiple password
   algorithms side-by-side. This makes it possible to use the
   facilities presented here in a backward-compatible manner with
   clusters created in previous version.

2. make it possible for a management layer (eg CC console) to
   configure account passwords without revealing the cleartext
   password to the CockroachDB server. This makes it impossible for
   attackers to discover user credentials by compromising the server where
   CockroachDB is running.

3. make it possible for SQL clients to authenticate without revealing
   the cleartext password to the CockroachDB server and also
   making password authentication resistant to MITM and replay attacks.


Goal 1 is achieved by adding a "password algorithm" column to
`system.users`, and making the pgwire component aware of this.

Goal 2 is achieved by making the WITH PASSWORD clause in the SQL dialect
able to recognize "hashed passwords" - the encoded form of passwords
that hides client secrets from the server. This is a feature that was
introduced in PostgreSQL 9.6 (released 2016) and we intend to match it.

Goal 3 is achieved by supporting the standard SCRAM-SHA-256
authentication algorithm, which has been introduced in PostgreSQL 10
and is now rather commonly supported in pg client drivers.

A side-benefit of achieving goal 3 is that it lowers the computational
cost of password verification server-side. As of this day, CockroachDB's
legacy implementation uses a CPU-intensive algorithm and it is possible
to overload a server with unsuccessful password authentication attempts.

Table of contents:

- [Motivation](#motivation)
- [Background](#background)
  - [Password authentication, general concepts](#password-authentication-general-concepts)
  - [Decomposition of the algorithm into methods](#decomposition-of-the-algorithm-into-methods)
  - [PostgreSQL authentication methods](#postgresql-authentication-methods)
  - [Separate storage and authentication methods](#separate-storage-and-authentication-methods)
  - [CockroachDB's password methods](#cockroahcdb-s-password-methods)
  - [Security considerations](#security-considerations)
- [Guide-level explanation](#guide-level-explanation)
  - [Storage method configuration](#storage-method-configuration)
  - [Version gating](#version-gating)
  - [Default storage method selection](#default-storage-method-selection)
  - [Storage method selection in WITH PASSWORD](#storage-method-selection-in-with-password)
  - [Mixed TLS client cert and password auth](#mixed-tls-client-cert-and-password-authn)
- [Reference-level explanation](#reference-level-explanation)
  - [Changes to `system.users`](#changes-to-system-users)
  - [Changes to SQL parsing](#changes-to-sql-parsing)
  - [Changes to user creation](#changes-to-user-creation)
  - [Changes to HBA configuration parsing](#changes-to-HBA-configuration-parsing)
  - [Changes to pgwire authn methods `password` and `cert-password`](#changes-to-pgwire-auth-methods-password-and-cert-password)
  - [New pgwire authn methods: `scram-sha-256` and `cert-or-scram-sha-256`](#new-pgwire-authn-methods-scram-sha-256-and-cert-or-scram-sha-256)
  - [Drawbacks](#drawbacks)
  - [Rationale and Alternatives](#rationale-and-alternatives)
  - [Unresolved questions](#unresolved-questions)

# Motivation

The primary motivation for this work is to enhance the security
of CockroachCloud clusters.

# Background

This section presents background knowledge necessary to understand the rest of the RFC.

Table of contents:

- [Password authentication, general concepts](#password-authentication-general-concepts)
- [Decomposition of the algorithm into methods](#decomposition-of-the-algorithm-into-methods)
- [PostgreSQL authentication methods](#postgresql-authentication-methods)
- [Separate storage and authentication methods](#separate-storage-and-authentication-methods)
- [CockroachDB's password methods](#cockroahcdb-s-password-methods)
- [Security considerations](#security-considerations)


## Password authentication, general concepts

At a high-level, passwords work like this:

1. a password is chosen.

2. the server stores a **server-side secret** that represents the password.

   The server-side secret is typically another string other than
   the password itself (derived from it). It is computed
   so that even if an attacker gets hold of the server-side secret,
   it cannot discover the password itself and use it to authenticate.

   For example, this could be a
   [hash](https://en.wikipedia.org/wiki/Hash_function) of the
   password. There are other ways to generate the server-side secrets
   besides a simple hash.

3. When the client connects, the server sends a **password
   challenge**, which is a message that tells the client *how it should
   present its knowledge of the password to the server*.

4. The client computes then sends a **password response** to the server.
   This could be the password itself or some other string that
   *proves that the client knows the password*, for example a hash.

5. The server then **validates** the response against its own
   stored server-side secret. If the response is OK, then
   authentication succeeds and the SQL client can log in.

For example, here is the legacy "MD5" method implemented in PostgreSQL:

1. the client-side secret is the password string itself.
2. the server-side secret is the result of hashing the password with a
   single round of [MD5](https://en.wikipedia.org/wiki/MD5).
3. the challenge is "send me the password as MD5 please"
4. the client driver computes a MD5 hash of the user-provided password
   string and sends that to the server.
5. the server compares the provided MD5 hash to that it had stored
   previously. Authn succeeds iff they are equal.

Note: Generally, pg's MD5 method is now considered obsolete. It is
only provided here for illustration purposes. (There used to be just a
little protection against an attacker getting hold of the password
hashes server-side, e.g. during a server compromise. However this
protection does not exist any more, since it has become
computationally cheap to break a MD5 function. This method also offers
no protection against MITM and replay attacks.)

## Decomposition of the algorithm into methods

From the example above, one recognizes that a **password algorithm**
is generally formed by the following two components:

- **Storage method**, used once when a password is initially configured:
  - Server-side **StoreFn**: a function to **convert a password to a server-side secret**

- **Authentication method**, used every time a client connects:
  - Server-side **ChallengeFn**: a function to **produce a challenge from a server-side secret**
  - Client-side: **ResponseFn**: a function to **compute a password response to a server-provided challenge**.
  - Server-side **ValidateFn**: a function to **validate a password response from a client**

## PostgreSQL authentication methods

As a reminder, here are the password-related authn methods supported by PostgreSQL:

- `password`: server challenges client to send plaintext, client sends password in cleartext
  - ChallengeFn: "send plaintext plz"
  - ResponseFn: send plaintext password
  - ValidateFn: compute MD5 hash server-side, compare MD5 hashes

- `md5`: server challenges client to send MD5 hash, client sends MD5 hash of password
  - ChallengeFn: "send MD5 plz"
  - ResponseFn: compute MD5 hash client-side and send it
  - ValidateFn: compare MD5 hashes

- `scram-sha-256`: server sends
  [SCRAM](https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism)
  challenge, client computes SCRAM response, using hash function
  SHA-256.
  - ChallengeFn: SCRAM, server-side
  - ResponseFn: SCRAM, client-response
  - ValidateFn: SCRAM validation

At the time of this writing we can find the following support:

| Driver         | `password` support | `md5` support | `scram-sha-256` support |
|----------------|--------------------|---------------|-------------------------|
| libpq (C)      | yes                | yes           | yes                     |
| Go lib/pq      | yes                | yes           | yes                     |
| Python psycopg | yes                | yes           | yes                     |
| pgJDBC         | yes                | yes           | yes                     |
| Go pgx         | yes                | ???           | unsure                  |

In summary, most pg-compatible client drivers know about all 3
password authn methods supported by PostgreSQL.

## Separate storage and authentication methods

The storage and authentication methods are typically configured
separately from each other.

In PostgreSQL, the storage method is determined by the server configuration option `password_encryption`. 

The authentication method is determined *separately* by the [HBA configuration] rule that matches every client connection.

[HBA configuration]: https://dr-knz.net/authentication-in-postgresql-and-cockroachdb.html

For example, pg's authn could be implemented as follows:

- StoreFn, storage `password_encryption=md5`: compute MD5 hash
- HBA rule chooses `md5` authn method:
  - ChallengeFn: "please send MD5 hash please"
  - ResponseFn: compute MD5 hash of password, send MD5 hash to server
  - ValidateFn: compare provided MD5  hash to stored MD5 hash

But notice that it could *also* be implemented as follows:

- StoreFn, storage `password_encryption=md5`: compute MD5 hash
- HBA rule chooses `password` (cleartext) authn method:
  - ChallengeFn: "please send cleartext password please"
  - ResponseFn: send cleartext password to server
  - ValidateFn: server computes MD5 hash of provided password server-side, then compare computed MD5  hash to stored MD5 hash

Or also as follows:

- StoreFn, storage `password_encryption=password`: store plaintext password
- HBA rule chooses `md5` authn method:
  - ChallengeFn: "please send MD5 hash please"
  - ResponseFn: compute MD5 hash of password, send MD5 hash to server
  - ValidateFn: server computes MD5 hash of *stored* password, then compares computed hash with provided hash.

All these combinations happen to be possible.

PostgreSQL supports the following combinations of storage/authn methods:

| PostgreSQL `password_encryption` | Storage method                                 | Supported authentication methods |
|----------------------------------|------------------------------------------------|----------------------------------|
| `password`                       | Password stored as-is, in cleartext            | `password`, `md5`                |
| `md5`, `on`,                     | MD5 hash of password stored                    | `password`, `md5`                |
| `scram-sha-256` (since v10)      | Stores SCRAM hash, salt, iterations parameters | `password`, `scram-sha-256`      |

It is thus possible for a single storage method to support multiple
authn methods. Certain combinations are impossible because of the
definition of the password algorithm. For example, it is impossible to
implement the `md5` authn method if the `scram-sha-256` storage method
was used.

## CockroachDB's password methods

CockroachDB currently supports a single *storage method* for password,
derived from the BCrypt algorithm. It is separate from and incompatible
with all the storage methods supported by PostgreSQL.
We'll call it `bcrypt-crdb` in the rest of the RFC.

It also only supports the `password` *authn method*.

## Security considerations

- Storage methods:
  - `password` offers no security against attackers who get hold of stored passwords.
  - `md5` *used* to offer some security, offers none today.
  - `scram-sha-256` offers good security against attackers who get hold of stored password.
  - CockroachDB's `bcrypt-crdb` storage method also offers good security in that attack scenario.

- Authn methods:

  - `password` provides no security over non-encrypted links; it's
    somewhat more secure over TLS encrypted session, as long as the
    client validates the server's certificate to prevent MITM attacks.
    If the session is MITMed, `password` offers no security against replay attacks.

    Meanwhile, `password` offers no security against an attacker who
    can compromise the server and thus discover the plaintext password
    server-side.

  - `md5` has the same authn-level security as `password`. It is also
    considered obsolete.

  - `scram-sha-256` offers protection against [credential
    stuffing](https://en.wikipedia.org/wiki/Credential_stuffing) and
    replay even over non-encrypted links, or MITMed TLS sessions.
    
    The algorithm is also cheaper server-side, as it moves the cost
    of the hashing function to the client.

In general, our goal is to **support scram-sha-256 for
authentication** which offers generally better protection at a lower
server-side resource cost.

However, SCRAM authn in turn *constrains the storage method* and
CockroachDB's current `bcrypt-crdb` method cannot support SCRAM
authn. Therefore, we must also support a new storage method.

# Guide-level explanation

The previous section explains how passwords are implemented using
separate **storage** and **authentication** methods and explains which
combinations are supported in PostgreSQL.

In CockroachDB, we aim to support the following:

| `format` column in `system.users` (NEW) | Storage method                                                | Supported authentication methods |
|-----------------------------------------|---------------------------------------------------------------|----------------------------------|
| NULL or `bcrypt-crdb`                   | Bcrypt-10 of (password + SHA256(""))                          | `password`                       |
| `scram-sha-256`                         | Stores SCRAM hash, salt, iterations parameters (NEW, like pg) | `password`, `scram-sha-256`      |

The first row in this table corresponds to the storage method
implemented in CockroachDB up to and until v20.2. This method was not
previously given a name in the CockroachDB source code. This RFC
proposes to call it `bcrypt-crdb`.

(Side note: the strange formula "Bcrypt-10 of (password + SHA256(""))"
is due to an implementation mistake: the intention was to implement
"Bcrypt-10 of SHA256(password)" but that wasn't done right the first
time around, and never fixed afterwards. Also a fix would raise an
annoying migration question. In any case, the mistake doesn't make the
algorithm significantly weaker.)

This storage method only supports authn method `password`, which
corresponds to cleartext passwords. It is not possible to support
`md5` or `scram-sha-256` authn with this storage method.

Therefore, in order to teach CockroachDB to support `scram-sha-256`
authn, we must also teach it a different storage method.

Table of contents:

- [Storage method configuration](#storage-method-configuration)
- [Version gating](#version-gating)
- [Default storage method selection](#default-storage-method-selection)
- [Storage method selection in WITH PASSWORD](#storage-method-selection-in-with-password)
- [Mixed TLS client cert and password auth](#mixed-tls-client-cert-and-password-authn)

## Storage method configuration

It's possible for different user accounts to use a different password storage method.

This is particularly likely in the following scenario:

- some user accounts are already created in v20.2, with method `bcrypt-crdb`
- the CockroachDB version is upgraded, offering a new pw storage method
- new user accounts get created with the new method

Because it's not possible to convert password details stored with one method
to another method, we must support multiple methods side-by-side.

We do this by adding a new column `format` to `system.users`:

- its DEFAULT value is `bcrypt-crdb`. The default makes it
  backward-compatible with all the rows already stored in the table.
- the new value `scram-sha-256` indicates that the `hashedPassword`
  field stores SCRAM-SHA-256 parameters.

During authentication, the authn code verifies that the stored values
in `system.users` are compatible with the authn method requested by
the HBA configuration.

This achieves [Goal 1 outlined in the summary](#summary).

## Version gating

In a mixed version X/Y config, where version X does not yet support the
new storage method, we must prevent the creation of user accounts
using the new method.

Otherwise, it would be possible for version X nodes to mistakenly
use the hashed details of the new method using the `bcrypt-crdb`
algorithm, resulting in a potentially insecure situation.

For this, we introduce a cluster version to gate usage of the new method.

## Default storage method selection

When a client provides a password in plaintext using `CREATE ROLE WITH
PASSWORD`, the server must choose a storage method to compute
`hashedPassword` in `system.users`.

This is done by introducing a cluster setting
`server.user_login.password_encryption`. This `password_encryption`
setting has the same role as `password_encryption` in PostgreSQL: it
determines the storage method to use for all new accounts, when
the client configures an initial password in cleartext.

In CockroachDB, the only two allowed values are `bcrypt-crdb` and
`scram-sha-256`.

We introduce a cluster setting as opposed to hard-coding the algorithm
so as to facilitate tests and the generation of `system.users` tables
compatible with previous versions of CockroachDB.

The default value for the cluster setting is `bcrypt-crdb` until the
version introducing the feature is finalized. After that (or for new
clusters), it is upgraded to `scram-sha-256`.

## Storage method selection in WITH PASSWORD

We want to make it possible to create password-based accounts without
passing the cleartext password to the server.

This is used e.g. in the CC management console to make the server
blind to the cleartext password.

For this we extend the SQL dialect as follows:

- (baseline, already implemented:) `CREATE USER/ROLE WITH PASSWORD
  'xxxx'`: the server understands `xxxx` as a cleartext password.

  The server computes `system.users.hashedPassword` using the storage
  method configured server-side via the setting
  `server.user_login.password_encryption`.

  The current value of `server.user_login.password_encryption` is stored
  in the (new) `system.users.format` column.

  Note: the Bcrypt cost for `bcrypt-crdb` remains at 10 like the
  default in the Go library.
  For `scram-sha-256` we choose the same default as PostgreSQL: 4096 iterations.
  Some more reading on this topic: https://security.stackexchange.com/questions/3959/recommended-of-iterations-when-using-pkbdf2-sha256

- `CREATE USER/ROLE WITH PASSWORD 'xxxx' ENCRYPTION 'yyyy'`: the
  server understand `xxxx` as the direct `hashedPassword` value as
  computed by the storage method `yyyy` client-side.

  It stores `xxxx`
  directly into `system.users.hashedPassword` and `yyyy` into the
  (new) `system.users.format` column.

  Note: PostgreSQL has a different way to detect the storage algorithm,
  by peeking into the string `xxxx`. This is discussed further in the
  section [Rationales and Alternatives](#utility-of-the-format-column) below.

Here is a v20.1-compatible example:

- assume the current server configuration has `server.user_login.password_encryption = bcrypt-crdb`.
- client sends `CREATE USER foo WITH PASSWORD 'abc'` (cleartext password).
- server detects method `bcrypt-crdb` from cluster setting; auto-gens random salt and computes Bcrypt-10 hash

- server stores `$2a$10$fGgWYzxv4UTXVTNzQTHEa.kX3pMNNE.mxxoSk1ZTF9MPZlLOkHxbK` into `system.users.hashedPassword`. 
- server stored `bcrypt-crdb` into `system.users.format`.

Here's an example using the new option, in a way compatible with v20.1 servers:

- client auto-generates random salt and computes Bcrypt-10 hash of password.
- client sends `CREATE USER foo WITH PASSWORD '$2a$10$fGgWYzxv4UTXVTNzQTHEa.kX3pMNNE.mxxoSk1ZTF9MPZlLOkHxbK' ENCRYPTION 'bcrypt-crdb` to server.

  Note: this string contains the bcrypt cost (10) and the salt
  (`fGgWYzxv4UTXVTNzQTHEa.`) as a prefix. This is how the server
  learns the salt from the management layer.

- server verifies that `$2a$10$...` has the right format given the `bcrypt-crdb` method (see below how this is done).
- server stores client-provided `$2a$10$fGgWYzxv4UTXVTNzQTHEa.kX3pMNNE.mxxoSk1ZTF9MPZlLOkHxbK` directly into `system.users.hashedPassword`. 
- server stores client-provided `bcrypt-crdb` into `system.users.format`.

This achieves [Goal 2 outlined in the summary](#summary).

How the hash format is checked in the latter case:

- For `bcrypt-crdb` the syntax must be:

  - a `$2a$` prefix
  - an integer value after that, followed by `$`, indicating a valid BCrypt cost.

    Note 1: even though the Go library has a minimum bound of 4, we will
    enforce the valid minimum bound provided by clients to be 10. We
    enforce the same upper bound as the Go library, that is 31.

    Note 2: is is perfectly OK for different accounts to use different
    costs.  The cost is read from `system.users` during password
    verification (together with the salt) and used as input to re-hash
    the incoming password from the client.

  - 53 valid Radix-64 characters (22 characters for the salt, 31 for the hash)

- For `scram-sha-256` we use the same constraints as postgres. Minimum SCRAM
  iteration count is 4096.

## Mixed TLS client cert and password authn

CockroachDB supports its own custom `cert-password` authn method,
incompatible with PostgreSQL, which enables a client to authenticate
using EITHER a valid TLS client cert or a valid cleartext password.

This RFC proposes to complement `cert-password` with another
authn method `cert-or-scram-sha-256` which requires either
a valid TLS client cert or a valid SCRAM-SHA-256 exchange.

This new method would be the default in the HBA config of
newly-created clusters; clusters upgraded from previous versions need
to change their HBA config manually.

# Reference-level explanation

Table of contents:

- [Changes to `system.users`](#changes-to-system-users)
- [Changes to SQL parsing](#changes-to-sql-parsing)
- [Changes to user creation](#changes-to-user-creation)
- [Changes to HBA configuration parsing](#changes-to-HBA-configuration-parsing)
- [Changes to pgwire authn methods `password` and `cert-password`](#changes-to-pgwire-auth-methods-password-and-cert-password)
- [New pgwire authn methods: `scram-sha-256` and `cert-or-scram-sha-256`](#new-pgwire-authn-methods-scram-sha-256-and-cert-or-scram-sha-256)
- [Drawbacks](#drawbacks)
- [Rationale and Alternatives](#rationale-and-alternatives)
- [Unresolved questions](#unresolved-questions)


## Changes to `system.users`

A migration adds a new column `format STRING DEFAULT 'bcrypt-crdb'` to `system.users`.

## Changes to SQL parsing

`CREATE/ALTER USER/ROLE` must recognize a new `WITH` clause `ENCRYPTION` alongside `PASSWORD`.

## Changes to user creation

Currently user creation takes the cleartext password provided using WITH PASSWORD, then applies `bcrypt-crdb` unconditionally.

This needs to be changed as follows:

- if the new `ENCRYPTION` clause is *not* provided, then:
  - read the storage method from the new cluster setting `server.user_login.password_encryption`
  - use that to compute `system.users.hashedPassword`
  - store the method's name in `system.users.format`

- if the `ENCRYPTION` clause is provided, then:
  - validate the syntactic structure of the client-provided PASSWORD string against the method
    (e.g. `bcrypt-crdb` requires a `$2a$10$` prefix followed by 53 valid Radix-64 characters)
  - store the provided PASSWORD and ENCRYPTION strings directly into
    `system.users.hashedPassword` and `system.users.format`.

In both cases, any other value than `bcrypt-crdb` is refused for the
storage method if the cluster version is not recent enough (i.e. we
prevent using the new methods until the entire cluster is upgraded).

## Changes to HBA configuration parsing

Currently the code only recognizes the `password` authn method name
(alongside the other non-password authn methods also recognized).

The HBA config parser must be extended to support the `scram-sha-256`
and `cert-or-scram-sha-256` method names.

However the new authn names must not be usable until the cluster version
is bumped: we don't want "old version" nodes discovering an HBA config
which they don't understand.

## Changes to pgwire authn methods `password` and `cert-password`

When a client connects, it may encounter a HBA rule that says `password` or `cert-password`, i.e.
the HBA config is requesting cleartext authn (as a fallback in the case of `cert-password`).

Inside the `pgwire` module, this delegates authn to a method that currently does the following:

1. challenges client to send cleartext password
2. reads `system.users.hashedPassword`
3. decodes Bcrypt parameters from the current value of `hashedPassword`
4. applies Bcrypt on the client-provided cleartext password, using the current parameters
5. compares the resulting hashes.

This needs to be changed as follows:

1. challenges client to send cleartext password
2. reads `system.users.hashedPassword` and also `system.users.format`
3. if `format` is `bcrypt-crdb`, then proceed as before.
4. if `format` is `scram-sha-256`, then:
   - decode the SCRAM parameters from `hashedPassword`.
   - hash the client-provided cleartext password using the encoded salt and iteration counter.
   - compare the resulting hashes.

## New pgwire authn methods: `scram-sha-256` and `cert-or-scram-sha-256`

When a client connects, it may encounter a HBA rule that says `scram-sha-256`, i.e.
the HBA config is requesting SCRAM-SHA-256 authn.
(or as a fallback if `cert-or-scram-sha-256` is provided)

We add a new SCRAM method which does the following:

1. challenges client to initiate SCRAM exchange
2. reads `system.users.hashedPassword` and also `system.users.format`
3. if `format` is NOT `scram-sha-256`, fake a SCRAM exchange and finish with a "password authn invalid" error
4. if `format` is `scram-sha-256`, then:
   - decode the SCRAM parameters from `hashedPassword`.
   - proceed with the SCRAM challenge/response exchange.

This achieves [Goal 3 outlined in the summary](#summary).


## Drawbacks

TBD

## Rationale and Alternatives

### Utility of the `format` column

The current implementation guarantees that `hashedPassword` always
starts with `$2a$10$...`, i.e.  the encoding of the "password hash"
embeds the Bcrypt version identifier (`2a`) and the cost
(10).

Similarly, PostgreSQL's pw storage methods prefix passwords with either the
string `md5` (MD5 method) or `scram-sha-256:` (for SCRAM).

Conceptually, we could thus derive the method name from the shape
of the value stored in `hashedPassword`: if the prefix is `$2a$`, then
use `bcrypt-crdb`; if it's `scram-sha-256:` then use SCRAM.

This approach was rejected because there are multiple ways to
implement BCrypt that result in a `$2a$` prefix, i.e. the prefix
is not sufficient to decide which implementation was used.

For example, CockroachDB's current implementation is non-standard and
does a precomputation on the password before passing it to BCrypt. If
we were to add a standard implementation later (e.g. `bcrypt` instead of `bcrypt-crdb`), the shape of the resulting hash would also start with `$2a$`.

So it's useful/necessary to distinguish the method using a separate
column on `system.users`.

### Implement SCRAM-BCRYPT-CRDB on top of existing `hashedPassword` values

Due to the specifics of how the `hashedPassword` values are computed
in CockroachDB, it is theoretically possible to define a Bcrypt-based
SCRAM challenge/response handshake without defining a storage method.

This makes it possible to reach SCRAM-level security and protections
without the need for a new storage method.

This authn method would however use different parameters than
SCRAM-SHA-256: it would use BCrypt as hash function, and would need the client
to append CockroachDB's magic string to passwords before hashing.
Its "standard" name would be SCRAM-BCRYPT-CRDB and would be incompatible
with SCRAM-SHA-256.

Therefore it would need dedicated support in client drivers.

CockroachDB does not (yet) provide its own drivers, and we can't count
on pg-compatible drivers implementors to do this work for us.
Therefore this approach is unworkable from the perspective of client
compatibility.

### Opportunity to upgrade existing passwords

Discussion from Ben:

For existing clusters, it is possible to transparently upgrade old
passwords to the new storage format whenever a user logs in with the
`password` authn method. This would make it possible to upgrade most
passwords from `bcrypt-crdb` to `scram-sha-256` without requiring users to
reset their passwords.

Do we want to take advantage of this opportunity? It would give us a
chance to upgrade the security of existing stored passwords (assuming
the iteration count we choose for `scram-sha-256` is high enough) and
give us the performance benefits of the SCRAM protocol (which is both
lower CPU costs on our side and, in a well-implemented client, faster
connection pool initialization on the client side). On the other hand,
it's rather magical and could expose users to bugs in their drivers'
SCRAM implementations.

In the past I've seen this kind of transparent upgrade used to
automate periodic increases to the work factor. But for us it would be
a one-shot opportunity (the SCRAM protocol doesn't give you the same
opportunity for login-time re-hashing). Now that I've written this up
I think it's probably not a good idea but wanted to note the
possibility here for the record.

## Unresolved questions

TBD
