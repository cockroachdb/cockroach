- Feature Name: SSO Authentication
- Status: draft
- Start Date: 2022-05-24
- Authors: Cameron Nunez
- RFC PR: #82540

# Summary

This RFC proposes introducing Console-issued accept tokens as a CRDB login 
method. This change would enable single sign-on (SSO) for CRDB, allowing users
to login automatically and have a more consolidated login experience for CRDB and 
CC Console. 

This RFC seeks to achieve the following goals:
* Users are able to create sessions without using "something you know" authentication.
* Human users are able to use CC-issued, short-lived credentials to access CRDB.
* Human users, having only entered login credentials into their identity provider, are able to 
create an active CRDB session with a CRDB Cloud cluster.
* Service accounts hosted in the customer’s identity provider are able to create an active CRDB 
  session while only entering their credentials into their identity provider.
* Service accounts hosted in CC Console must be able to create an active CRDB session.

The following are out of scope:
* Authorization e.g. user permissions propagation from CC to CRDB
* Just-in-time provisioning of SQL users at login time

# Motivation

Customers are currently unable to enforce their enterprise authentication policies and 
are unable to ensure a single point of access across the product for their users.

As support for CC SSO grows, it is crucial that we provide SSO for CRDB as well 
to prevent any confusion for organization admins regarding seeing
different authentication methods across the product. 

# Background

Currently, clients can authenticate themselves to the server in the following ways:
* Password 
* SCRAM 
  * A password hashing protocol that offers multiple security benefits 
  over password.
  * Recommended over password, assuming clients can support it (many do not).
* Certificate
  * A certificate/key pair that is signed by a certificate authority is used to 
  authenticate the client.
  * Not supported for CRDB Cloud.
* SQL Session Revival Token
  * Token-based authentication used by SQL Proxy in order to re-authenticate a
    previously-established session.
* Trust (and Reject)
  * Always allows access or never allow access.
  * Usually paired with restrictions such as requiring requests come 
  from specific IP addresses or unix sockets.
* Kerberos / GSSAPI
    * Not supported for CockroachDB Cloud.

This RFC proposes to add a new token-based authentication method that will enable signed access 
tokens to be passed into the password field, 
and this new method will achieve the aforementioned goals.

# Technical Design

The intention is to adhere closely to the OAuth 2.0 and OpenID Connect (OIDC) protocols, where CRDB
CLI plays the role of the OIDC client and CC Console plays the role of the OIDC server.

The token credentials will consist of an access token, an ID token, and a refresh token. These 
tokens have finite lifetimes, and if a user needs long-lived access, refresh tokens 
can be used to request a new set of token credentials. When issued, the access token and the ID token are assigned a random 
lifetime value ranging between 60 and 90 minutes (note: they are assigned the exact same random 
value). Access tokens cannot be revoked 
and are valid until their expiry. Other details about the tokens are included in the table below.

| Token              | Description                                                                                                                                     | Type    | Usage | TTL          | Lifetime      |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------|-------|--------------|---------------|
| Access             | Used to gain CRDB access (human users).                                                                                                         | signed  | many  | short-lived  | 60–90 minutes |
| Refresh            | Used to obtain a new set of tokens when the current access token has expired (human users).                                                     | opaque  | once  | long-lived   | 90 days       |
| Service            | Used to retrieve an access token (service accounts).                                                                                            | signed  | many  | long-lived   | 30 days       |
| ID                 | Contains information about the user. Needed to follow the OIDC protocol, which we wish to adhere to for easier integration with DB Console SSO. | signed  | many  | short-lived  | 60–90 minutes |
| Authorization Code | Used to retrieve a set of token credentials.                                                                                                    | opaque  | once  | short-lived  | 10 minutes    |


Access tokens will be JWTs (JSON Web Tokens), and so the tokens contain three segments: header, 
payload, and signature. CC Console will sign the access tokens using asymmetric encryption
(RS256). Below are the claims in the 
access token:


| Claim      | JWT Part | Format | Description                                                                   |
|------------|----------|--------|-------------------------------------------------------------------------------|
| `typ`      | Header   | string | The type of token. Always "JWT".                                              |
| `alg`      | Header   | string | The algorithm that was used to sign the token. By default, "RS256".           |
| `kid`      | Header   | string | The thumbprint for the public key that can be used to validate the signature. |
| `aud`      | Payload  | string | The intended recipient of the token - the audience.                           |
| `iss`      | Payload  | string | The issuer of the token.                                                      |
| `iat`      | Payload  | int    | The time when this token was issued.                                          |
| `exp`      | Payload  | int    | The expiration time on or after which the token must not be accepted.         |
| `ccid`     | Payload  | string | The user's CockroachDB Cloud user ID.                                         |
| `username` | Payload  | string | The user's SQL username.                                                      |


ID tokens are also JWTs and are the key extension that OIDC adds to OAuth 2.0. Following the 
OIDC protocol will allow us to more easily integrate with DB Console's existing OIDC 
functionality, and the use of the token will only really be relevant for the integration. An ID 
token can be sent alongside or in lieu of an access token. ID tokens share nearly all the same 
claims that access tokens have but contain additional claims that carry information about the user 
(e.g. an `email` claim that specifies the user's email address), and so the information in ID tokens
allows for verification that a user is who they state to be. ID tokens for a given user will always 
have the same `ccid` claim (which specifies the user's CockroachDB Cloud user ID), so the `ccid` 
claim will be used to reliably identity users.

The human user login flow will follow the 
[OAuth 2.0 Authorization Code Flow](https://oauth.net/2/grant-types/authorization-code/); it will 
look like the following:
1. The CLI redirects the user to a browser to complete the login request.
2. The user will be asked by CC Console if they wish to allow Cockroach CLI to access 
   requested data.
3. The user accepts and is redirected back along with an authorization code.
4. The CLI sends the authorization code to CC Console in a request for token credentials.
5. CC Console generates a set of token credentials specific to the user (and to the
   cluster involved).
6. CC Console responds with the credentials and they are passed like a password in the pgurl.
7. CRDB recognizes that the credential is token-based and validates the 
user, depending on whether the login request succeeds.

CC Console will derive a unique SQL username from the user's email address and will store this 
username on the access token and the ID token. The username is generated earlier on at CC user 
creation time and is stored on the CC user object. When token credentials are minted, the 
username is stored in the credentials. These usernames will start with a prefix `sso_` in order to 
distinguish them from other SQL usernames. Eventually, we will want to support the ability to 
change these usernames if the customer wishes, but it is not a priority considering that the 
customer has presumably created these users already. 

When an access token is presented to CRDB, token validation will include a check to make sure that 
the audience field matches what is expected. The audience field will include 
the client ID in addition to the cluster ID, so that the intended audience is one particular 
cluster. This will prevent a single access token from being used on multiple clusters.

**Add CC API endpoints to mint token credentials (for human users)**

A new CC API, `GenerateAuthorizationCode`, will be added to allow CC Console to give 
authorization for token requests. While the user is redirected to a browser to complete the 
login attempt, CRDB CLI listens on `localhost` for an authorization code, a token that is required 
in order to get token credentials. CC Console will verify that the redirect URI is valid for 
the client. Assuming the check succeeds, it generates the authorization code and redirects the user
back to the redirect URI.

Regarding client credentials, we plan to add a default client ID and client secret in the binary so 
that CLI can use them for requests to CC Console. **Note that this approach requires that we do 
not grant these 
credentials high-level permissions going forward.**

For the new endpoint, `/oauth2/authorize`, a small webpage will be set up to operate it. 
If the user is not already logged in, the user will be prompted to 
log-in and the endpoint will use standard CC Console session cookies for authorization.

Request parameters:
* `client_id` the client's ID.
* `redirect_uri` an authorized redirect URI for the given `client_id`.
* `state` an opaque value used by the client to maintain state between the request and
    callback.

Response elements:
* `authorization_code` the token to be exchanged for an access code.
* `expires_in` the number of seconds until the authorization code expires.
* `state` an opaque value used by the client to maintain state between the request and
  callback. CC Console includes this value when redirecting the user back to the CLI.

After receiving the authorization code, the CLI will call to another CC API, 
`GenerateTokens`, which generates token credentials in exchange for the 
authorization code, client credentials, and cluster ID.
The validity of request arguments is checked, and it is checked 
whether the authorization code was created for the given client ID. CC Console uses the 
specified cluster ID in order to set a proper audience field in the access token and ID token.
Assuming the checks succeed, 
the API will return a set of token credentials. Communication with the `/oauth2/token` endpoint 
utilizes TLS.

Request parameters:
* `authorization_code` the token to be exchanged for an access code.
* `client_id` the client's ID.
* `client_secret` the client secret.
* `cluster_id` the cluster ID.

Response elements:
* `access_token` the JWT that allows the user to gain access to CRDB.
* `refresh_token` the token that can be used to retrieve new token credentials.
* `expires_in` the remaining lifetime of the access token in seconds.

**Add a CC API endpoint to refresh login tokens (for human users)**

`RefreshTokens` will be added to allow human users to retrieve a new
set of token credentials after expiration. We can have an endpoint `/oauth2/refresh` which the 
user can use to submit a request. It is important to note that the refresh tokens are single-use. 
Old refresh tokens should be securely deleted after acquiring a new one. The response of the API 
will be identical to `GenerateTokens`.

Request parameters:
* `client_id` the client's identification.
* `client_secret` the client secret.
* `refresh_token` the user sets this to the refresh token they previously
  received.
* `expires_in` the number of seconds until the access token expires.

A successful response of this API is identical to that of `GenerateTokens`.

**Update SQL authentication code to identify and validate access tokens** 

The token gets to the DB by being passed like a password in the pgurl, which also ensures support 
for different drivers since the token will be treated like a plaintext password. The server 
will look at the form of authentication, see that it is a token, and proceed 
to verify the token. If the token is valid, SQL authentication proceeds as if a correct password were provided.

Token validation will look like the following:
1) Check that the `alg` (algorithm) claim matches the expected algorithm which was used to sign
   the token.
2) Check that the signature in the token is the correct signature for the payload - verified using
   the public key provided by CC Console.
3) Check that the issuer from the `iss` claim matches the expected issuer (CC Console).
4) Check that the `aud` (audience) claim contains the expected `client_id` and `cluster_id` values.
5) Check that the `exp` (expiration) claim time has not passed.

An initial validation key will be baked into the image. The cluster will dynamically update its 
current key during token validation by updating its configuration when (1) the given token has an 
unknown key ID specified in the `kid` Claim and additionally (2) the key set has not been recently 
updated. Assuming these conditions are satisfied, a new key will be acquired which will become the 
current key, and the old current key will become the previous key, which will be kept around. 
There will also be an option to override default keys with keys of choice. 

**Add new HBA rules and update HBA configuration parsing**

The HBA configuration parser will be extended to support new methods names related to 
token-based authentication including `jwt`, `jwt-or-password`, and `jwt-or-cert-password`. When a 
client connects, the client may encounter one of these rules, which means that the HBA configuration
is requesting/allows token-based authn.  These new method names must not be usable until the 
cluster version has been bumped in order to ensure that nodes can understand the HBA config. Note 
that a `jwt-or-scram-sha-256` method is NOT included because in that particular case clients would 
not be sending a cleartext form of authentication.

## Service Accounts / Non-interactive users

Service accounts (non-interative users) will trade for SQL access tokens directly. 

Instead of a refresh token flow, non-interactive users will rely on a long-lived token flow. 
The users will use a long-lived signed token that they can use many times to retrieve an access 
token.

The service account user login flow looks like the following - **for service accounts hosted in 
the customer's identity provider (IdP)**:
1. The user requests a token from their IdP.
2. The customer's IdP issues a long-lived signed token to the user.
3. The user sends the token to CC Console in the request for an access token.
4. CC Console checks the trust relationship on the app registration and validates the 
   IdP-issued token.
5. Assuming the checks succeed, CC Console issues an access token to the user.
6. The user gains access to CRDB using the CC-issued access token.

**Add a CC API endpoint to generate tokens for service accounts**

A new API `GenerateServiceTokens` will generate an access token in exchange for client credentials
and the user's service token. Service accounts will directly send requests using this API.
The validity of request arguments is checked. CC Console uses the specified cluster ID in order 
to set a proper audience field in the access token. Assuming the checks succeed,
the API will return an access token.
Endpoint: `/oauth2/service-token`

Request parameters:
* `service_token` the token to be exchanged for an access code.
* `cluster_id` the cluster ID of the cluster that the user is trying to access.

Response elements:
* `access_token` the JWT that allows the user to gain access to CRDB.
* `expires_in` the remaining lifetime of the access token in seconds.


**Create a new CC console screen for managing service account credentials**

In order to support service accounts that are not hosted in the customer's IdP but hosted in CC, a 
new CC Console screen will be created where users can interactively manage 
long-lived proof-of-identity credentials needed by service accounts that are 
hosted in CC. The lifetime for these credentials is 30 days. This UI will be authorized using 
standard CC Console session cookies and will enable the 
user to:
* Manage the service account users
* Issue the long-lived proof-of-identity credentials
* Revoke the long-lived proof-of-identity credentials

The components of these credentials will include:
* `client_id` the client ID.
* `client_secret` the client secret.
* `cluster_ids` the list of cluster IDs of clusters that the user is permitted to access.

## Drawbacks

This approach requires that we add more urgency to minimize CC downtime, since any downtime 
would result in login failures.

In the case of a transient network disruption between a driver and CRDB, the CRDB SDK should either (1) 
continually keep an active access token or (2) refresh the access token in the case of the event. 
Unfortunately, most PostgreSQL clients cannot handle the refresh pattern, so users would have to add
in tools or libraries provided by us in order to use any non-CRL DB clients. 

Once a SQL connection is made, the connection will persist even if the 
credential expires, which can be seen as problematic.

## Alternatives

**No Refresh Tokens**

Instead of having a refresh process, we could do the same long-lived token flow that we propose for
service accounts for human users as well. One other option is that we could have an error 
`err: access token expired` trigger an access token refresh.

**Certificate-based authentication**

Instead of token-based authentication, we could implement a certificate centered login flow. 
Certificates are similar in function to signed tokens and so share similar advantages and 
limitations. In this case, certificates are referring to X.509 certs which are most 
commonly used for TLS connections. Certificates are usually valid for an extensive period of time, but we could reduce 
the lifetime if we wished to remove the need for revocation. Otherwise, in order for 
certificates to be revocable, a list of revoked certificates would need to be pushed by CC 
Console to CRDB or CRDB would need to make a call to CC during validation to ensure the certificate 
is still valid. The login flow would change in that CC would send a certificate instead of a 
signed token to the CLI, and would be proceeded by the following: 
1) The user performs mTLS handshake when connecting to CRDB.
2) During the handshake, CRDB obtains the cert.
3) CRDB validates the certificate.
4) CRDB extracts information from the cert and ensures that it matches a user.
5) The user is granted access.


Advantages:
* Fewer changes to pgwire protocol given that CRDB already supports certificates.
* Immunity to replay attacks.

**Opaque Accept Tokens**

We could use SQL accept tokens that are opaque instead of signed. These tokens would
require reaching out to CC (the token issuer) in order to verify tokens.

Drawbacks:
* Higher authentication latency since every login requires consultation with CC.

Advantages:
* Smaller size - JWTs are quite large.
* Less CPU needed for verification.
* Token revocation becomes easy.
* Increased security (token only understood by CC, the issuer).

**Opaque Tokens (as Time-Limited "Passwords")**

We could use opaque tokens that are effectively auto-generated, short-lived passwords that would be 
sent from CC to CRDB. Such tokens would be verifiable at the cluster level without a network call.

Drawbacks:
* For every token refresh, CC would need to reach into CRDB to issue a token change.
* Assuming no significant changes to the login infrastructure, this change 
would imply that for each database, there would be only a single active 
credential for a user at any given time.

**Device Authorization Flow**

Instead of having the authorization code flow during which the user is authenticated directly 
in CC during the login flow, we could support the device authorization flow so that the CLI can 
simply use the client ID to initiate the authorization process and get tokens. Instead of needing to
log into CC, the user will be asked to go to a link to authorize the CLI. 

**Avoiding Uptime Burden on CC**

If we want to avoid adding high uptime requirements for CC Console, we could consider splitting out 
the login flow from the existing Console code base, and use extra availability strategies to 
make it more redundant and durable than Console currently is. 
