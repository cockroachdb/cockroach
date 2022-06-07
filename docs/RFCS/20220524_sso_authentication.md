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

The token credentials will consist of an access token, an ID token, and a refresh token. These 
tokens have finite lifetimes, and if a user needs long-lived access, refresh tokens 
can be used to request a new set of token credentials. Old refresh tokens should be securely deleted 
after acquiring a new one. When issued, the access token and the ID token are assigned a random 
lifetime value ranging between 60 and 90 minutes. Access tokens cannot be revoked 
and are valid until their expiry. Other details about the tokens are included in the table below.

| Token              | Description                                                      | Type   | Usage | TTL          | Lifetime      |
|--------------------|------------------------------------------------------------------|--------|-------|--------------|---------------|
| Access             | Used to gain CRDB access (human users).                          | signed | many  | short-lived  | 60–90 minutes |
| Service Access     | Used to gain CRDB access (service accounts).                     | signed | many  | long-lived   | 60 days       |
| Refresh            | Used to retrieve another set of token credentials (human users). | opaque | once  | long-lived   | 90 days       |
| ID                 | Contains information about the user.                             | signed | many  | short-lived  | 60–90 minutes |
| Authorization Code | Used to get token credentials.                                   | opaque | once  | short-lived  | 10 minutes    |


Access tokens will be JWTs (JSON Web Tokens), and so the tokens contain three segments: header, 
payload, and signature. CC Console will sign the access tokens using asymmetric encryption
(RS256). Below are the claims in the 
access token:


| Claim      | JWT Part | Format | Description                                                                      |
|------------|----------|--------|----------------------------------------------------------------------------------|
| `typ`      | Header   | string | The type of token. Always "JWT".                                                 |
| `alg`      | Header   | string | The algorithm that was used to sign the token. By default, "RS256".              |
| `kid`      | Header   | string | The thumbprint for the public key that can be used to validate the signature.    |
| `aud`      | Payload  | string | The intended recipient of the token - the audience.                              |
| `idp`      | Payload  | string | The identity provider that authenticated the subject of the token.               |
| `iss`      | Payload  | string | The issuer of the token.                                                         |
| `iat`      | Payload  | int    | The time when this token was issued.                                             |
| `nbf`      | Payload  | int    | The "not before" time - the time before which the token must not be accepted.    |
| `exp`      | Payload  | int    | The expiration time on or after which the token must not be accepted.            |
| `username` | Payload  | string | The unique SQL username derived from the user's email address.                   |
| `name`     | Payload  | string | The identifier for the subject of the token.                                     |


ID tokens will also be JWTs. The claims of these tokens overlap with that of access tokens, but 
contain more user specific information like the user's email. They contain more identity information
and can be verified to prove that it has not been tampered with.

The human user login flow will look like the following:
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

When a user first gets access, CC Console will derive a unique SQL username from the user's email 
address and will store this username on the access token and the ID token. These usernames will 
start with a prefix `sso_` in order to  distinguish them from other SQL usernames. CRDB will store 
the username from the ID token and additionally will store the refresh token.

When an access token is presented to CRDB, it will check to make sure that the audience field (an 
identifier belonging to a cluster that both the cluster itself and CC knows) matches the value 
it is expecting when doing token validation. This will prevent a single 
access token from being used on multiple clusters.

**Add CC API endpoints to mint token credentials**

The plan is to follow the OIDC specification when creating new endpoints for this feature, where 
the CLI plays the role of the OIDC client. **It 
is important to note that this choice enables us to integrate with the existing OIDC 
functionality used by DB Console.**

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

For the new endpoint, `\authorize`, a small webpage will be set up to operate it. 
Regarding authorization, the endpoint will use standard CC Console session cookies.

Request parameters:
* `client_id` the client's identification.
* `redirect_uri` an authorized redirect URI for the given `client_id`.
* `state` an opaque value used by the client to maintain state between the request and
    callback.

Response elements:
* `authorization_code` the token to be exchanged for an access code.
* `expires_in` the number of seconds until the authorization code expires.
* `state` an opaque value used by the client to maintain state between the request and
  callback. CC Console includes this value when redirecting the user back to the SQL shell.

After receiving the authorization code, the CLI will call to another CC API, 
`GenerateTokens`, which generates token credentials in exchange for the 
authorization code, client credentials, and cluster ID.
The validity of request arguments is checked, and it is checked 
whether the authorization code was created for the given client ID. CC Console uses the 
specified cluster ID in order to set a proper audience field in the access token and ID token.
Assuming the checks succeed, 
the API will return a set of token credentials. Communication with the `\token` endpoint utilizes TLS.

Request parameters:
* `authorization_code` the token to be exchanged for an access code.
* `client_id` the client's ID.
* `client_secret` the client secret.
* `cluster_id` the cluster ID.

Response elements:
* `id_token` the JWT containing the user and scope information.
* `access_token` the JWT that allows the user to gain access to CRDB.
* `refresh_token` the token that can be used to retrieve new token credentials.
* `expires_in` the remaining lifetime of the access token in seconds.

**Add a CC API endpoint to refresh login tokens**

`RefreshTokens` will be added to allow human users to retrieve a new
set of token credentials after expiration. We can have an endpoint `\refresh` which the user can
use to submit a request. It is important to note that the refresh tokens are single-use. The 
`jti` claim on the refresh token will ensure the token can only be used once.
The response of the API will be identical to `GenerateTokens`.

Request parameters:
* `client_id` the client's identification.
* `client_secret` the client secret.
* `refresh_token` the user sets this to the refresh token they previously
  received.
* `expires_in` the number of seconds until the access token expires. While configurable, the
  minimum and maximum allowed values will be enforced.

A successful response of this API is identical to that of `GenerateTokens`.

**Update pgwire code to identify and validate access tokens** 

The changes needed for the new login method will take inspiration from the existing token-based 
authentication for SQL session revival. The server will look at the form of authentication, see 
that it is a token, and, if it is valid, will bypass the password authentication code.

Token validation will look like the following:
1) Check that the issuer from the `iss` claim matches the expected issuer (CC Console).
2) Check that the `aud` (audience) claim contains the expected `client_id` and `cluster_id` values.
3) Check that the `alg` (algorithm) claim matches the expected algorithm which was used to sign 
   the token.
4) Check that the signature in the token is the correct signature for the payload - verified using 
   the 
  public key provided by CC Console.
5) Check that the `nbf` (not before) claim time has passed.
6) Check that the `exp` (expiration) claim time has not passed

If the token is valid, SQL authentication continues as if a correct password were provided.

The server will be able to obtain a public key by looking it up at a specific CC Console URL.

## Service Accounts / Non-interactive users

Service accounts (non-interative users) will trade for SQL access tokens directly. These access 
tokens will be long-lived, as opposed to the short-lived tokens generated for human users.

Instead of a refresh token flow, non-interactive users will rely on a long-lived token flow.

**Create a new CC console screen for managing service account credentials**

A new CC Console screen will be created where users can interactively manage 
proof-of-identity credentials
needed by service accounts to get service access tokens. This UI will be authorized using 
standard CC Console session cookies and will enable the user to:
* Manage the service account users
* Issue credentials
* Revoke credentials 

The components of these credentials will include:
* `authorization_code` the token to be exchanged for an access code.
* `client_id` the client ID assigned to the service account.
* `client_secret` the client secret assigned to the service account.
* `cluster_id` the cluster ID of the cluster that the service account will access.

**Add a CC API endpoint to generate tokens for service accounts**

A new API `GenerateServiceTokens` will generate token credentials (including a long-lived 
service access 
token) in exchange for the authorization code, client credentials, and cluster ID. 
Service accounts will directly send requests using this API.
The validity of request arguments is checked, and it is checked whether the authorization code was 
created for the given client ID. CC Console uses the specified cluster ID in order to set a proper 
audience field in the service access token and ID token. Assuming the checks succeed,
the API will return a set of token credentials.  Endpoint: `\servicetoken` 

Request parameters:
* `authorization_code` the token to be exchanged for an access code.
* `client_id` the client's ID.
* `client_secret` the client secret.
* `cluster_id` the cluster ID.

Response elements:
* `id_token` the JWT containing the user and scope information.
* `access_token` the JWT that allows the user to gain access to CRDB.
* `expires_in` the remaining lifetime of the access token in seconds.

## Drawbacks

This approach requires that we add more urgency to minimize CC downtime, since any downtime 
would result in login failures.

In the case of a transient network disruption, the CRDB SDK should either (1) 
continually keep an active access token or (2) refresh the access token 
in the case of the event. Unfortunately, most PostgreSQL clients cannot handle 
the refresh pattern, so users would have to add in tools or libraries provided 
by us in order to use any non-CRL DB clients. 

Once a SQL connection is made, the connection will persist even if the 
credential expires, which can be seen as problematic.

## Alternatives

**Auto-generated, short-lived passwords**

We could use auto-generated passwords that would be 
sent from CC to CRDB. We can use passwords as long as they are complex enough 
that they cannot be feasibly remembered. 

Drawbacks:
* For every refresh, CC would need to reach into CRDB to issue a password change.
* Assuming no significant changes to the password infrastructure, this change 
would imply that for each database, there would be only a single active 
credential for a user at any given time.

Advantages:
* Builds on existing password infrastructure.


**Opaque accept tokens**

We could use SQL accept tokens that are opaque instead of signed.

Drawbacks:
* Increases time to login since every login requires consultation with CC.

Advantages:
* Less CPU needed for verification. 
* Token revocation becomes easy.
* Increased security (token only understood by CC, the issuer).

**Long-lived tokens**

Instead of having a refresh process, we could do the same long-lived token flow that we propose for 
service accounts for human users as well.

Drawbacks:
* Access tokens with long TTL adds security risk.

Advantages:
* Unifies the login flows for human users and service account users.

**CLI using long polling**

Instead of having CLI listen on `localhost` and get authorization before sending a token 
request, we could simply have CLI send a token request and wait, using long polling, for the 
request to succeed.

Drawbacks:

Advantages:
