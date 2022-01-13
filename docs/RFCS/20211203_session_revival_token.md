- Feature Name: Token-Based Authentication for SQL Session Revival
- Status: draft
- Start Date: 2021-12-03
- Authors: Rafi Shamim
- RFC PR: https://github.com/cockroachdb/cockroach/pull/74640
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/74643


## Summary

We are introducing a new way to authenticate and create a SQL session. This is intended to be used by SQL Proxy in order to re-authenticate a previously-established session (technically, authenticate the SQL Proxy itself) as part of the session migration project of CockroachDB Serverless. The new authentication mechanism is inspired by [JSON Web Tokens (JWTs)](https://datatracker.ietf.org/doc/html/rfc7519), so SQL Proxy will be able to ask for a token from a SQL node, and then later use it to start a new session for the user described in that token. This will allow sessions to be moved without major disruption [if a SQL node is shutting down](https://cockroachlabs.atlassian.net/browse/CC-5387) or if load can be shifted [from busy SQL nodes to lightly loaded nodes](https://cockroachlabs.atlassian.net/browse/CC-5385).


## Motivation

There are two motivations for the new authentication mechanism. For CockroachDB Serverless, we want the following:



* Gracefully transfer sessions to another SQL node whenever a node is shutting down. Currently, when a SQL node is scaled down, existing connections to that node will be terminated forcefully, which is a poor user experience. We want to transfer these sessions to a live SQL node so that from the user’s perspective, the connection remains usable without interruption.
* Connection load balancing between SQL nodes. Currently, when there is an increase in load, more SQL nodes are created. However, sessions that caused the increase in load will still be connected to the old nodes. We want to be able to transfer sessions from one SQL node to another for better resource utilization.

These use cases are addressed by “session migration.” In v21.2, the groundwork was laid for session migration with the addition of the builtin functions `crdb_internal.serialize_session` and `crdb_internal.deserialize_session` ([#68792](https://github.com/cockroachdb/cockroach/pull/68792)). These functions, respectively, save the state of the current session, and restore the saved state into the current session. The challenge is that a session can only be restored if the user associated with the current session matches the user defined in the saved session state. This means that in order to restore a session, SQL Proxy must first create a new session for that user, and this new session must be created without any input from the user.

The focus of this RFC is to revive a session that had been authenticated for a SQL client previously. Therefore, the goal here is to authenticate the SQL Proxy as operating on behalf of the client, rather than as the client. This is a more narrow scope than the general problem of token authentication for SQL client apps. We do not exclude that this RFC will inform a solution to the more general problem but we wish to not let this RFC be burdened by it.


### Non-goals



* An authentication flow that can be leveraged to implement Oauth2 authentication in the future.

The authors acknowledge that there are various customer requests for authentication-related features that are conceptually related to the change proposed here. However, we are not considering them in-scope here and we expect that the user-facing services will be implemented in other ways.


## How would we explain this to a person outside your team?

The change proposed here makes it possible to move a SQL session from one SQL node to another securely in the CRDB-Serverless architecture. What this means is that it empowers the "SQL Proxy" component to orchestrate SQL session migration, without introducing security weaknesses for malicious users outside of the CC architecture.

In particular, it makes it possible for the SQL proxy to respawn a SQL session on a new server for an existing SQL user account, without knowing the access credentials of that user, as long as that user had a valid session already open previously on another server. 

The design is such that it remains impossible for malicious users, or even a bug in the SQL Proxy, to spawn a valid SQL session for an arbitrary user without knowing that user's credentials.

Note that the purpose of this design is NOT to create a new authentication method visible to end-user apps. In other words:



* We do not intend to document this as a feature usable by end-users.
* The mechanisms implemented here will not have cross-version compatibility guarantees for the benefit of app developers.


## Technical Design

The proposed solution to this is to allow SQL Proxy to obtain a token from the SQL node for each open SQL session before that node shuts down. This token can then be presented later, during the establishment of a connection to a different SQL node, in order to initialize a session for the same user. The token is cryptographically signed with a ed25519 signing cert, so the new SQL node can verify the signature and trust that the token originated from some other SQL node. The technical changes are in the following places.


#### Add a new builtin: `crdb_internal.create_session_token`

This function has a return value of type BYTES and returns the SessionToken proto serialized to bytes.

The SessionToken itself is a proto defined as follows.


```
message SessionToken {
  // Payload is defined as a separate type so that it's easier to sign.
  message Payload {
    // The SQL user who can use this token to authenticate.
    string user = 1;
    // The algorithm used to sign the payload. Can be either Ed25519 or RSA.
    string algorithm = 2
    // The time that this token is no longer considered valid.
    // Default is 10 minutes from now.
    google.protobuf.Timestamp expires_at = 3;
    // The time that this token was created.
    google.protobuf.Timestamp issued_at = 4;
  }

  // The payload is serialized to bytes so that the bytes that get signed
  // are deterministic, independent of the proto version.
  bytes payload_bytes = 1;
  // The signature of the payload, signed using the new signing key.
  bytes signature = 2;
}
```


One may notice that the structure of the token is similar to a [JSON Web Token](https://datatracker.ietf.org/doc/html/rfc7519#section-3.1), with some differences. For example, we use protocol buffers so that new fields can easily be added and remain backwards compatible, and we do not base64-encode the result.


#### Update pgwire code to look out for the new token

We will add a CockroachDB-specific “StartupMessage” parameter name in the [pgwire/server.go](https://github.com/cockroachdb/cockroach/blob/b4ab627436afd8d9ed23330cd4026aa435b5b65d/pkg/sql/pgwire/server.go#L731) code. We’ll name it **<code>session_revival_token</code>**.

Before reaching the password authentication code, the server will see the token, and if it’s valid, will bypass the regular session initialization and [authentication code](https://github.com/cockroachdb/cockroach/blob/86383ca90f7e0604773496c03e35850d9089b394/pkg/sql/pgwire/conn.go#L645).

To validate the token, the bytes are unmarshalled into a SessionToken struct. The SessionToken is considered valid if: (1) the signature in the token is the correct signature for the payload (verified by using the public key of the signing cert), (2) the user in the payload matches the user field in the session parameters, and (3) the expires_at time has not yet passed. If the token is valid, then the authentication continues as if a correct password were presented. These three checks are because: (1) the signature prevents forgery of a token, (2) checking the user matches is a light bit of defensive programming to make sure SQL Proxy uses the correct token for a user, and (3) enforcing an expiration time prevents the creation of a token by brute-force.


#### Deploy new signing key


In order to sign the token, we will introduce a new signing key (either [Ed25519](https://pkg.go.dev/crypto/ed25519) or RSA depending on what the Intrusion team decides) that is unique per tenant, and is shared by all SQL nodes of the same tenant. The advantage of Ed25519 is that it is fast and generates small signatures. The signing key will be generated by the Intrusion team. Unlike the existing tenant-client cert, this new signing cert doesn't require any DNS or IP information baked into the certificate and the certificate can be self-signed. This makes it easier to rotate if a tenant pod becomes compromised. When creating a tenant SQL node, Intrusion will need to put the key into the `certs-dir` of the SQL node (similar to how the tenant certs are added now). The cert will be named `tenant-signing.<TENANT_ID>.key`. The `crdb_internal.create_session_token` function will fail if this key is not present, and the token creation will be able to detect the algorithm used by the cert and call the according signature methods. A `cockroach mt cert create-tenant-signing` command will be added for testing purposes.


#### Update SQL Proxy to block the new StartupMessage

It should remain impossible for external client apps to revive SQL sessions using this new protocol. In other words, this feature should be restricted for use by SQL Proxy.

To achieve this, we will change the SQL proxy code to block the new status parameter if it is provided by a SQL client app when establishing a new connection.

If we made this particular design user-facing, a malicious user could take one token for a valid session then flood our infrastructure with thousands of sessions opened with that token in a very short time. We have designed passwords and TLS authn to ensure this flood is impossible and it is important to not re-introduce it inadvertently by making this new mechanism user-facing.


#### Configuration in the sqlServer

Since the mechanism should only be available in CRDB-Serverless deployments, we will make it configurable so that it is not enabled by default on dedicated or self-hosted clusters. A new field will be added to `sqlServerArgs` named **<code>sessionRevivalTokenEnabled</code>** which defaults to false, and it will control if the SQL node looks for the <strong><code>session_revival_token</code></strong> parameter. The field will be set to true only in the <code>server.StartTenant</code> function, so the functionality only is enabled in multitenant clusters.


### Drawbacks

One drawback of this design is that it requires the token to be passed in as a CockroachDB-specific StartupMessage. This means that 3rd party drivers won’t be able to use this flow unless we fork them. Currently, that is not a use case we need to support, but if it does become one later, we can add in other ways for the SQL server to receive the token.

Another drawback is that it needs to handle the case where the signing key is rotated or revoked. In the case of rotation, we can introduce a grace period so that multiple signing keys are present for a 10-minute grace period. Every signature verification attempt needs to first try the new key, and then try the old key if that fails. If the signing key is **revoked**, then all tokens signed with that key will become invalid.


### Rationale and Alternatives

**Separate pgwire message type**

An alternative design is to introduce an entirely new message type in our pgwire protocol implementation that contains the token. However, this is a more fundamental divergence from the standard protocol. This would be a risky change, since pgwire messages only use a single byte for the message type identifier. It would be quite possible for a future version of Postgres to introduce a new message type that clashes with our custom message type.

**Coupling session deserialization to token-based authentication**

During the discussion of the design, the question arose if the authentication token should also include the serialized session state from `crdb_internal.serialize_session()`. Then, the only way to authenticate with the token would also require that a previous session be restored. This RFC proposes to **decouple** authentication from session restoration. The reason is that we want to use smaller primitives which can later be used as building blocks for other features. For example, allowing SQL Proxy to make a new session might help with a possible future use case that adds connection pooling to SQL Proxy.

**Setup an external auth broker**

If we were to build Oauth2 support into CRDB, we would be able to leverage an external auth service. 

The downsides to this option would be that our internal teams would need to configure and maintain an external auth system, as well as building support for dynamically updating the users database for that auth system.

**Make SQL Proxy generate the token**

We could add the new signing cert to SQL Proxy instead, and have it be in charge of making the token. Essentially, this would change the proposal to something more like “allow a user to login just because SQL Proxy says it can.” This would mean that the SQL node needs to trust that SQL Proxy would only do this if the user has already authenticated in some other way before. The tokens could not be created outside of the CRDB-Serverless product (e.g. in self-hosted clusters.) However, it also means that a compromised SQL Proxy instance could be used to generate any number of tokens, and we would have no way to audit the impact of such an attack. Because of these risks, we aren’t implementing this solution.


### Possible future extensions

**Long-lived tokens**

Currently, we only require short-lived tokens. If in the future, we need to have longer-lived tokens, the SQL Proxy could be updated to append additional expiry periods to the token (and sign them), in such a way that would be trusted by the SQL node when the session is resumed.

**One-time-use tokens**

In this design, a token can be used any number of times to authenticate a new session. It might be useful as a form of defensive programming to only allow each token to be used once, so that SQL Proxy doesn’t accidentally misuse them. If we decide later that this would be useful, the token could be updated to include a session identifier, and the token validation logic could verify that there are no active sessions with that ID. This would increase the latency of validating a session.

**Combine session token and deserialize_session**

As described in the motivation, sessions that are created using this token, will then use the deserialize_session builtin to restore state from a previously serialized session. This RFC keeps these two steps separate, but in the future, we may want to consolidate them if that leads to a reduction in code complexity.

**Add a tenant read-only cluster setting for opting into session revival**

Once the [multitenant cluster settings proposal](https://github.com/cockroachdb/cockroach/pull/73349) is completed, we should add a tenant read-only cluster setting that allows users to opt into this new behavior. This will be needed once the multitenant architecture is rolled out to self-hosted/dedicated clusters so that the new authentication mechanism is not enabled on these clusters unintentionally. 


## Unresolved questions

* N/A
