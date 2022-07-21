- Feature Name: SSO Authentication
- Status: in-progress
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
  * Recommended over password, assuming clients can support it.
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

The intention is to adhere closely to the OAuth 2.0 and OpenID Connect (OIDC) protocols. The
OAuth2.0 flow has the following roles:
* **Resource Server**, the server hosting the protected resources.
  * CRDB.
* **Resource Owner**, the entity that can grant access to a protected resource.
  * The user.
* **Authorization Server**, the server that authenticates the resource owner and issues access
  tokens after receiving proper authorization.
  * CC Console.
* **Client**: the application requesting access to the resource server on behalf of the
  resource owner.
  * CRDB CLI if the user is interactive.
  * The user itself if the user is non-interactive (service account).

The token credentials will consist of an access token, an ID token, and a refresh token. These
tokens have finite lifetimes, and if a user needs long-lived access, refresh tokens
can be used to request a new set of token credentials. The access token and the ID token both
have a lifetime value of 60 minutes. However, we expect to eventually make the TTL configurable
(bounded by minimum and maximum allowed values). Access tokens cannot be revoked and are valid until
their expiration. In addition to allowing users to refresh their access once they run over the TTL
of the access token, refresh tokens make it so that users will not have to continually re-authorize. Other
details about the tokens are included in the table below.

| Token              | Description                                                                                                                                     | Users                           | Type   | Usage | TTL          | Lifetime   |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|--------|-------|--------------|------------|
| Access             | Used to gain CRDB access.                                                                                                                       | All                             | signed | many  | short-lived  | 60 minutes |
| Refresh            | Used to obtain a new set of tokens when the current access token has expired.                                                                   | Humans                          | opaque | once  | long-lived   | 90 days    |
| ID                 | Contains information about the user. Needed to follow the OIDC protocol, which we wish to adhere to for easier integration with DB Console SSO. | Humans                          | signed | many  | short-lived  | 60 minutes |
| Authorization Code | Used to retrieve a set of token credentials.                                                                                                    | Humans                          | opaque | once  | short-lived  | 10 minutes |


Access tokens will be JWTs (JSON Web Tokens), and so the tokens contain three segments: header,
payload, and signature. The header of the token contains information about the key and encryption 
method used to sign the token.

Example:
```
{
  "typ": "JWT",
  "alg": "RS256",
  "kid": "iBfK1Acqbhey1fpxHxdZpohN1Zu"
}
```
This is Base64 encoded to form the first section of the JWT.

The payload contains all the important data pertaining to the user that is attempting to gain 
access to CRDB. 

Example:
```
{
  "iss":       "https://cockroachlabs.cloud",
  "sub":       "jdoe",
  "aud":       "d34e1413-9b31-4a6f-90e1-c194f871845f",
  "iat":       1627566690,
  "exp":       1627653090,
}
```
An example of an encoded JWT to be passed to the server:
```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImlCZksxQWNxYmhleTFmcHhIeGRacG9oTjFadSJ9.eyJpc3MiOiJodHRwczovL2NvY2tyb2FjaGxhYnMuY2xvdWQiLCJzdWIiOiIxMjM0NTY3ODkwIiwiYXVkIjoiZDM0ZTE0MTMtOWIzMS00YTZmLTkwZTEtYzE5NGY4NzE4NDVmIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE2Mjc2NTMwOTB9.FDrf30sOSaGju0K7asCQMW5z-mv-BJ6cv8rTT1H_K5I
```

Below are the claims in the access token in detail:


| Claim      | JWT Part | Format | Description                                                                   |
|------------|----------|--------|-------------------------------------------------------------------------------|
| `typ`      | Header   | string | The type of token. Always "JWT".                                              |
| `alg`      | Header   | string | The algorithm that was used to sign the token. By default, "RS256".           |
| `kid`      | Header   | string | The thumbprint for the public key that can be used to validate the signature. |
| `aud`      | Payload  | string | The intended recipient of the token - the audience.                           |
| `iss`      | Payload  | string | The issuer of the token.                                                      |
| `iat`      | Payload  | int    | The time when this token was issued.                                          |
| `exp`      | Payload  | int    | The expiration time on or after which the token must not be accepted.         |
| `sub`      | Payload  | string | The subject identifier (the user's SQL username).                             |

ID tokens are also JWTs and are the key extension that OIDC adds to OAuth 2.0. Following the
OIDC protocol will allow us to more easily integrate with DB Console's existing OIDC
functionality, and the use of the token will only really be relevant for the integration. An ID
token can be sent alongside or in lieu of an access token. ID tokens share nearly all the same
claims that access tokens have but may contain additional claims that carry information about the
user, and so the information in ID tokens allows for verification that a user is who they state to
be. ID tokens for a given user will always have the same `sub` claim, so the `sub` claim will be used to reliably 
identity users.

The human user login flow will follow the
[OAuth 2.0 Authorization Code Flow](https://oauth.net/2/grant-types/authorization-code/);
it will look like the following:
1. The CLI initially either:

    (1) redirects the user to a browser to complete the login request or

    (2) detects it cannot open a browser and instead displays a URL for the user to go to.
2. The user will be asked by CC Console if they wish to allow Cockroach CLI to access
   requested data.
   * **note**: the user will not have to continually re-authorize the CLI - because of the refresh flow.
3. The user accepts. If the user was initially directed to a browser, the user is redirected to 
   `localhost` (which the CLI is listening on) back along with an authorization code. If the user 
   initially went to the URL manually, the user copies and pastes the authorization code into the 
   CLI.
   * **note**: if the redirection to `localhost` fails, the user can try logging in again with a 
     `--no-redirect` flag so that the user will be able to copy and paste the authorization code 
     instead.
4. The CLI sends the authorization code to CC Console in a request for token credentials.
5. CC Console responds with the token credentials and they are [presented as a password](#update-sql-authentication-flow-to-identify-and-validate-access-tokens).
6. CRDB recognizes that the credential is token-based and validates the
   user, depending on whether the login request succeeds.

[Details about what 
this flow looks like for non-interactive users is outlined later in this RFC](#service-accounts).

When token credentials are minted, a SQL username is included in the credentials as the 
`username` claim. The username is generated earlier on at CC user creation time and is derived 
from the user's email. Eventually, we will want to add the ability for customers to 
specify SQL usernames of their choice, but that is not a priority at the moment given that 
presumably customers have already created these users.

When an access token is presented to CRDB, token validation will include a check to make sure that
the audience field matches what is expected. The audience field contains the cluster ID, so that 
the intended audience is one particular cluster. This will prevent a single access token from being 
used on multiple clusters.

#### Add CC API endpoints to mint token credentials (for human users)

A new CC API, `GenerateAuthorizationCode`, will be added to allow CC Console to give authorization for 
token requests. It generates the authorization code and, with a response, either
(1) the user is redirected back to the redirect URI along with the code or (2) the page displays 
the authorization code for the user to copy and paste into the CLI, depending on whether the CLI was
able to open a browser at the beginning of the flow.

Regarding client credentials, we plan to add a default client ID and client secret in the binary 
so that CLI can use them for requests to CC Console. **Note that this approach requires that we do 
not grant these credentials high-level permissions going forward**.

For the new endpoint, `/oauth2/authorize`, a small webpage will be set up to operate it. If the user 
is not already logged in, the user will be prompted to log in and the endpoint will use standard CC 
Console session cookies for authorization.

Request parameters:
* `client_id` the client's ID.
* `redirect_uri` an authorized redirect URI for the given `client_id`.
* `state` an opaque value used by the client to maintain state between the request and callback.

Response elements:
* `authorization_code` the token to be exchanged for an access code.
* `expires_in` the number of seconds until the authorization code expires.
* `state` an opaque value used by the client to maintain state between the request and callback. 
CC Console includes this value when redirecting the user back to the CLI.

After receiving the authorization code, the CLI will call to another CC API, `GenerateTokens`, which 
generates token credentials in exchange for the authorization code, client credentials, and cluster
ID. The validity of request arguments is checked, and it is checked whether the authorization code 
was created for the given client ID. CC Console uses the specified cluster ID in order to set a 
proper audience field in the access token and ID token. Assuming the checks succeed, the API will 
return a set of token credentials. Communication with the `/oauth2/token` endpoint utilizes TLS.

Request parameters:
* `authorization_code` the token to be exchanged for an access code.
* `client_id` the client's ID.
* `client_secret` the client secret.
* `cluster_id` the cluster ID.
* `scope` (optional) a space-separated list of scopes that determine what claims are included in
  the issued tokens. If `openid` is included, an ID token will be included in the response.

Response elements:
* `access_token` the JWT that allows the user to gain access to CRDB.
* `id_token` the JWT that can be used to authenticate a user (issued only if the `scope` 
  parameter in the request included the `openid` scope).
* `refresh_token` the token that can be used to retrieve new token credentials.
* `expires_in` the remaining lifetime of the access token in seconds.

#### Add a CC API endpoint to refresh login tokens (for human users)

`RefreshTokens` will be added to allow human users to retrieve a new
set of token credentials after expiration. We can have an endpoint `/oauth2/refresh` which the
user can use to submit a request. It is important to note that the refresh tokens are single-use.
They will have an `accepted_at` date to indicate if they were used and when. If the `accepted_at`
field is populated, the server will no longer accept that refresh token.

Request parameters:
* `client_id` the client's ID.
* `refresh_token` the user sets this to the refresh token they previously
  received.
* `expires_in` the number of seconds until the access token expires.

A successful response of this API is identical to that of `GenerateTokens`.

#### Update SQL authentication flow to identify and validate access tokens

The access token will be presented (Base64 encoded) to the server as a password.

In the connection string, the JWT will be sent through
the password field and the intent to use a JWT instead of a password will be communicated 
in the `options` field. The credential can be safely passed in this way because the TLS handshake occurs before the pg
status parameters are sent by the client.

`postgres://<username>:<jwt>@....?--options=jwt=true`

e.g.

```
postgres://jdoe:eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImlCZksxQWNxYmhleTFmcHhIeGRacG9oTjFadSJ9.eyJpc3MiOiJodHRwczovL2NvY2tyb2FjaGxhYnMuY2xvdWQiLCJzdWIiOiIxMjM0NTY3ODkwIiwiYXVkIjoiZDM0ZTE0MTMtOWIzMS00YTZmLTkwZTEtYzE5NGY4NzE4NDVmIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE2Mjc2NTMwOTB9.FDrf30sOSaGju0K7asCQMW5z-mv-BJ6cv8rTT1H_K5I@localhost:26257/postgres?--options=jwt=true
```
The token can be consumed in any way that the server consumes passwords, such as being read from the pgpass file.

After the token is sent and identified by the server, the server proceeds to verify the token. If
the token is valid, authentication proceeds as if a correct password were provided.

Token validation will look like the following:
1) Check that the `alg` (algorithm) claim matches the expected algorithm which was used to sign
   the token.
2) Check that the signature in the token is the correct signature for the payload - verified using
   the public key provided by CC Console.
3) Check that the issuer from the `iss` claim matches the expected issuer (CC Console). 
   * **note**: to support self-hosted, we can allow other acceptable issuers.
4) Check that the `aud` (audience) claim is the expected `cluster_id` value.
5) Check that the `exp` (expiration) claim time has not passed.

CC Console will use the
[JSON Web Key (JWK) specification](https://openid.net/specs/draft-jones-json-web-key-03.html).
CC Console will sign tokens with a public/private key pair using
[RS256 (RSA Digital Signature Algorithm with SHA-256)](https://ldapwiki.com/wiki/RS256).
The private signing key will rotate once a year, and in the case of an emergency, could be rotated
immediately. 

There will be two public keys available to the server (a newer key and older key), and CC will only issue
tokens using the older key set to give more leeway for rotations. The public keys will be stored in a new cluster setting 
(under the tenant read-only cluster setting class) in the 
[JSON Web Key Set (JWKS) format](https://auth0.com/docs/secure/tokens/json-web-tokens/json-web-key-sets). 
If CRDB attempts to use expired verification keys to verify the signature on a token, CRDB will reject the token.

## Service Accounts

Service accounts (non-interactive users) will trade for SQL access tokens directly. There will be
slightly different flows depending on if the service account is rooted in (1) the
customer's identity provider (IdP) or (2) CC Console.

#### Add CC API endpoints to generate tokens

A new API geared towards CC-rooted service accounts, `GenerateTokensService`, will generate
access tokens. Service accounts will need an API key to access the API, and admins will be
able to
[manage the API keys for this API in the console access management UI](https://www.cockroachlabs.com/docs/cockroachcloud/console-access-management.html#api-access).
CC Console uses the specified cluster ID in order to set a proper audience field in the access token.
Assuming the checks succeed, the API will return an access token. Endpoint: `/oauth2/servicetoken`

Request parameters:
* `api_key` used to identify the application that's calling the API and check whether it has
  been granted access to call the API.
* `cluster_id` the cluster ID of the cluster that the user is trying to access.

Response elements:
* `access_token` the JWT that allows the user to gain access to CRDB.
* `expires_in` the remaining lifetime of the access token in seconds.

There will be another API endpoint that is nearly identical for service accounts rooted in the
customer's IdP but takes a token issued by the customer's IdP, `customer_service_token`, instead
of an API key.

For service accounts hosted in the customer's IdP, the service account user login flow looks like
the following:
1. The user requests a token from their IdP.
2. The customer's IdP issues a token to the user.
3. The user sends the token to CC Console in the request for an access token.
4. CC Console checks the trust relationship on the app registration and validates the
   IdP-issued token.
5. Assuming the checks succeed, CC Console issues an access token to the user.
6. The user gains access to CRDB using the CC-issued access token.

## Supporting Various Setups 

Given that we follow the OAuth 2.0/OIDC standard in this proposal, it should be straightforward
getting customers setup to use this feature in ways other than the dynamic described in this
RFC (CC-hosted CRDB and the credential issuer being CC).

The proposal in this RFC is NOT in conflict with supporting:
1. CC customers who want to use their own infrastructure as the credential issuer.
2. Self-hosted customers who want to use their own infrastructure as the credential issuer.
3. Self-hosted customers who want to use CC as the credential issuer.

If the customer elects to use CC as the credential issuer, their instances would need to trust CC. 
If the customer chooses to use their own infrastructure, they would be able to set the signing key 
in the same way CC does, and we could provide a software microservice that can run in the customer's 
infrastructure which would convert their SSO login operations into credentials recognizable to CRDB.

Supporting self-hosted customers in using CC-issued tokens would help them integrate with their CC 
clusters and may provide an incentive to transition to CC-hosted CRDB. Supporting self-hosted 
customers in using their own tokens may allow some customers to move off of 
Kerberos. It should be considered, however, that investing resources in supporting self-hosted 
customers may not necessarily help regarding encouraging such customers to migrate to CC.

## Drawbacks

This approach requires that we add more urgency to minimize CC downtime, since any downtime
would result in login failures.

In the case of a transient network disruption between a driver and CRDB, the CRDB SDK should
either (1) continually keep an active access token or (2) refresh the access token in the case
of the event. Unfortunately, most PostgreSQL clients cannot handle the refresh pattern, so users
would have to add in tools or libraries provided by us in order to use any non-CRL DB clients.

Once a SQL connection is made, the connection will persist even if the
credential expires, which can be seen as problematic.

## Alternatives

**No Refresh Tokens**

If we do not mind having users continually involved in re-auth, we could
remove refresh tokens entirely. It will likely be more frustrating for the user to continually
re-authorize the CLI, but it would provide more protection against leaked access tokens.

**Embed Access Tokens in Certificates**

Instead of passing JWTs through the pg password field, we could have a TLS
client certificate hold an access token as a special payload. The token would be not contained as a 
cleartext string; it would be encrypted. This alternative makes an assumption, however, that in mTLS the client verifies the server's identity before the client 
presents a client cert.

The cert would be issued by CC and would expire at the same time as the JWT. The client would 
write the cert in the standard pg location (e.g. `$HOME/.postgres/<user>.{crt,key}`) and the 
server, after setting up a TLS session, can detect if a JWT exists in the client cert and, if a 
JWT is detected, validate the JWT and subsequently complete the authentication without initiating 
the HBA mechanism. It is important to note that this would NOT put a TLS client cert authentication 
requirement on Serverless, since the SQL Proxy could terminate TLS, extract the access token, and 
forward it to SQL pod which would handle authentication.

**Signed Tokens that are not JWTs**

JWTs are commonly used with OAuth 2.0, but there is no direct relationship between the two, so our 
signed access tokens do not need to necessarily be JWTs. It is required in the OIDC spec, 
however, that ID tokens are JWTs, but we could still pursue this alternative if we wanted to 
simply adhere to OAuth 2.0 only. The main drawback is that JWTs will be a familiar standard for 
users, but the advantage is that we could have smaller sized tokens given that JWTs are quite large.

**Opaque Access Tokens**

We could use SQL access tokens that are opaque instead of signed. These tokens would
require reaching out to CC (the token issuer) in order to verify tokens.

Drawbacks:
* Higher network latency since every login requires consultation with CC.

Advantages:
* Smaller size - JWTs are large.
* Less CPU needed for verification.
* Token revocation becomes easy.
* Reduces risk of data leakages and compliance violations (not possible for a client
  to access or leak any data from the token).

**Opaque Tokens (as Time-Limited "Passwords")**

We could use opaque tokens that are effectively auto-generated, short-lived passwords that would be
sent from CC to CRDB. Such tokens would be verifiable at the cluster level without a network call.

Drawbacks:
* For every access token issued, CC would need to reach into CRDB to issue a token change.
* Assuming no significant changes to the login infrastructure, this change
  would imply that for each database, there would be only a single active
  credential for a user at any given time.

Advantages:
* Less changes needed on the CRDB side.

**Certificate-based Authentication**

Instead of token-based authentication, we could implement a certificate centered login flow.
Certificates are similar in function to signed tokens and so share similar advantages and
limitations. In this case, certificates are referring to X.509 certs which are most
commonly used for TLS connections. Certificates are usually valid for an
extensive period of time, but we could reduce the lifetime if we wished to remove the need for
revocation. The login flow would happen in the following way:
1) The user performs mTLS handshake when connecting to CRDB.
2) During the handshake, CRDB obtains the cert.
3) CRDB validates the certificate.
4) CRDB extracts information from the cert and ensures that it matches a user.
5) The user is granted access.

Advantages:
* CRDB already supports this form of authn.

Drawbacks:
* Requires a more intricate technical design.
* Trickier to integrate it with DB Console.
* Still requires a refresh flow.

**Device Authorization Flow**

Instead of having the authorization code flow during which the user is authenticated directly in CC
during the login flow, we could support the device authorization flow so that the CLI can simply 
use the client ID to initiate the authorization process and get tokens. 
Instead of needing to log into CC, the user will be asked to go to a link to authorize the CLI.

**Avoiding Uptime Burden on CC**

If we want to avoid adding high uptime requirements for CC Console, we could consider splitting out
the login flow from the existing Console code base, and use extra availability strategies to
make it more redundant and durable than Console currently is. However, removing the CC control
plane would mean that service accounts hosted in CC could not log in.

We could use a different credential issuer (e.g. use Auth0 to issue credentials). However,
any
code we have to facilitate the login process would have to be tightly coupled to the chosen
vendor. It would mean we would not be able to modify the token encryption method.
It would also be trickier to allow users to login as other users and to enforce
username uniqueness constraints. 
