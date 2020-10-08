- Feature Name: Session Authentication for GRPC API
- Status: In Progress
- Start Date: 2017-7-11
- Authors: Matt Tracy
- RFC PR: #16829
- Cockroach Issue: #6307

# Summary
The Cockroach Server currently provides a number of HTTP endpoints (the Admin UI
and /debug endpoints) which return data about the cluster; however, these
endpoints are not secured behind with any sort of authorization or
authentication mechanism. This RPC proposes an authentication method which will
require each incoming request to be associated with a *login session*. A login
session will be created using a username/password combination. Incoming requests
which are not associated with a valid session will be rejected.

Changes to the CockroachDB server will be the addition of a sessions table, the
addition of a new RPC endpoint to allow the Admin UI to create a new session,
and a modification of our current RPC gateway server to enforce the new session
requirement for incoming requests.  Also included in this proposal is a method
for preventing CSRF (Cross-site request forgery) attacks.

The AdminUI will be modified to require that users create a new session by
"signing in" before the UI becomes usable. Outgoing requests will be modified
slightly to utilize the new CSRF mitigation method.

This RPC does not propose any new *authorization* mechanisms; it only
authenticates sessions, without enforcing any specific permissions on different
users.

# Motivation
Long-term goals for the UI include many scenarios in which users can actually
modify CockroachDB settings through the UI. Proper authentication and
authorization are a hard requirement for this; not all users of the system
should have access to modify it, and any modifications made must be auditable.

Even for the read-only scenarios currently enabled by the Admin UI, we are not
properly enforcing permissions; for example, all Admin UI users can see the
entire schema of the cluster, while database users can be restricted to seeing a
subset of databases and tables.

# Detailed design

The design has the following components:

+ A "web_sessions" table which holds information about currently valid user
sessions.
+ A "login" RPC accessible from the Admin UI over HTTP.  This RPC is called with
a username/password pair, which are validated by the server; if valid, a new
session is created and a cookie with a session ID is returned with the response.
+ All Admin UI methods, other than the new login request, will be modified to
require a valid session cookie be sent with incoming http requests. This will be
done at the service level before dispatching a specific method; the session's
username will be added to the method context if the session is valid.
+ The Admin UI will be modified to require a logged-in session before allowing
users to navigate to the current pages. If the user is not logged in, a dialog
will be displayed for the user to input a username and password.

+ A CSRF mitigation method will be added to both the client and the server.
    + The server gives the client a cookie containing a cryptographically random
      value. This is done when static assets are loaded.
    + HTTP Requests from the Admin UI will be augmented to read this cookie and
      use it to write a custom HTTP header.
    + The server verifies the custom header is present and matches the cookie on
      all incoming requests.

## Backend Changes

### Session Table
A new metadata table will be created to hold system sessions:

```sql
CREATE TABLE system.web_sessions {
    id              SERIAL      PRIMARY KEY,
    "hashedSecret"  BYTES       NOT NULL,
    username        STRING      NOT NULL,
    "createdAt"     TIMESTAMP   NOT NULL DEFAULT now(),
    "expiresAt"     TIMESTAMP   NOT NULL,
    "revokedAt"     TIMESTAMP,
    "lastUsedAt"    TIMESTAMP   NOT NULL DEFAULT now(),
    "auditInfo"     STRING,
    INDEX(expiresAt),
    INDEX(createdAt),
}
```

`id` is the identifier of the session, which is of type SERIAL (equivalent to
INT DEFAULT unique_rowid()).

`hashedSecret` is the hash of a cryptographically random byte array generated
and shared only with the original creator of the session. The server does not
store the original bytes generated, but rather hashes the generated value and
stores the hash. The server requires incoming requests to have the original
version of the secret for the provided session id. This allows the session's id
to be used in auditing logs which are readable by non-root users; if we did not
require this secret, any user with access to auditing logs would be able to
impersonate sessions.

`username` stores the username used to create a session.

Each session records four important timestamps:
+ `createdAt` indicates when the session was created. When a new session is
created, this is set to the current time.
+ `expiresAt` indicates when the session will expire. When a new session is
created, this is set to (current time + `server.web_session_timeout`);
web_session_timeout is a new server setting (a duration) added as part of this
RFC.
+ `revokedAt` is a possibly null timestamp indicating when the session was
explicitly revoked. If it has not been revoked, this field is null. When a new
session is created, this field is null.
+ `lastUsedAt` is a field being used to track whether a session is active. It
will be periodically refreshed if a session continues to be used; this can help
give an impression of whether a session is still being actively used after it is
created. When a new session is created, this is set to the current time.

The `auditInfo` string field contains an optional JSON-formatted string
object containing other information relevant for auditing purposes. Examples of
the type of information that may be included:
+ The IP address used to create the session originally.
+ If the session is revoked, the revoking username and session ID.

Secondary indexes are added on the following fields:
+ `expiresAt`, which quickly allows an admin to query for possibly active
sessions.
+ `createdAt`, which allows querying sessions in order of creation. This should
be useful for auditing purposes.

#### Creation of new table.

The new table will be added using a backwards-compatible migration; the same
fashion used for the jobs and settings tables. This is trivially possible
because no existing code is interacting with these tables.

The migration system, along with instructions for adding a new table, have an
entry point in `pkg/migrations/migrations.go`.

### Session Creation
Sessions will be created by calling a new HTTP endpoint "UserLogin". This new
method will be on its own new service "Authentication" separate from existing
services (Status/Admin); this is for reasons that will be explained in the
Session Enforcement session.

```protobuf
message UserLoginRequest {
    string username = 1;
    string password = 2;
}

message UserLoginResponse {
    // No information to return.
}

message UserLogoutRequest {
    // No information needed.
}

message UserLogoutResponse {
    // No information to return.
}

service Authentication {
  rpc UserLogin(UserLoginRequest) returns (UserLoginResponse) {
    // ...
  }

  rpc UserLogout(UserLogoutRequest) returns (UserLogoutResponse) {
    // ...
  }
}
```

When called, UserLogin will check the provided `username`/`password` pair against
the `system.users` table, which stores usernames along with hashed versions of
passwords.

If the username/password is valid, a new entry is inserted into the
`web_sessions` table and a 200 "OK" response is returned to the client. If the
username or password is invalid, a 401 Unauthorized response is returned.

For successful login responses a new cookie "session" is created containing the
ID and secret of the newly created session. This is used to associate future
requests from the client with the session.

Cookie headers can be added to the response using GRPC's SetHeader method. These
are attached as grpc metadata, which is then converted by GRPC Gateway into the
appropriate HTTP response headers.

The "session" cookie is marked as "httponly" and is thus not accessible from
javascript; this is a defense-in-depth measure to prevent session exfiltration
by XSS attacks. The cookie is also marked as "secure", which prevents the cookie
from being sent over an unsecured http connection.

The UserLogin method, when called successfully, will revoke the current session
by setting its `revokedAt` field to the current time. It will then return the
appropriate headers to delete the "session"

### Session Enforcement

Session enforcement is handled by adding a new muxing wrapper around the
existing "gwMux" used by grpc gateway services. Notably, the new wrapper will
only be added for the existing services; it will *not* be added for the new
Authentication service, because the UserLogin method must be accessible without
a session.

The new mux will enforce that all incoming connections have a valid session token.
A valid request will pass all of the following checks:

1. The incoming request has a "session" cookie.
2. The value of the id in the "session" cookie contains the ID of a session in
the session table. This is confirmed by performing a SELECT from the session
table.
3. The value of the secret in the "session" cookie matches the "hashedSecret"
from the session retrieved from the session table. This is confirmed by
hashing the secret in the cookie and comparing.
3. The session's `revokedAt` timestamp is null.
4. The session's `expiresAt` timestamp is in the future.

If any of the above criteria are not true, the incoming request is rejected with
a 401 Unauthorized message.

If the the session *does* pass the above criteria, then the username and session
ID are added to the context.Context before passing the call to gwMux. These
values can later be accessed in specific methods by accessing them from the
context.

If the session's `lastUsedAt` field is older than new system setting
`server.web_session_last_used_refresh`, the session record will be updated
to set its `lastUsedAt` value to the current time.

### CSRF Enforcement

CSRF is enforced using the "Double-submit cookie" method. This has two parts:

+ When the client sends a request to the server, it must read the value from a
cookie "csrf-token" (if present) and writes that value to an HTTP header on the
request "x-csrf-token".
+ When the server receives any request, it ensures that the value of the
"csrf-token" cookie sent with that request is the same as the value in
"x-csrf-token" header.

Any request that does not have a matching "csrf-token" cookie and "x-csrf-token"
header is rejected with a 401 Unauthorized error.

In order for this to work, the "csrf-token" cookie needs to be created at some
point. This is done on initial page load, when the server retrieves the static
assets (accomplished by wrapping the http.FileServer currently used with an
outer handler that sets the cookie. The outer handler will only set the cookie
for the main entry point of the website, `index.html`). The cookie contains a
cryptographically random generated string. The generated value does not need to
be stored anywhere on the server side. The cookie should have the `secure`
attribute.

CSRF involves an attacker's third-party website constructing a request to your
website, where the request contains some sort of malicious action. If the user
is logged in to your website, their valid session cookie will be sent with the
request and the action will be authorized. "Double-cookie" prevents this by
requiring the requester to set the "x-csrf-token" header based on a
domain-specific cookie; the third-party website cannot access that cookie, so it
cannot properly set the header. Simply explained, "Double cookie" works by
requiring the sender to prove that it can read cookies from a trusted origin.

The random value must be set on the server because it must be cryptographically
random; the client application is javascript and does not have reliable access
to a reliable random source, and attackers could possibly guess any random
values generated.

CSRF protection will be added to all GRPC gateway services, including the new
Authentication service (which does not itself require authentication, but will
require CSRF protection).

### "Debug" pages

A small number of "debug" pages are not part of the Admin UI application, but
are instead provided as HTML generated directly from Go. These endpoints can
also be moved underneath the session enforcement wrapper to require a valid
login session.

However, we will need to redirect users to the Admin UI in order for them to
actually create a session; therefore, the wrapper for these methods will not
return a "401" error response, but rather a redirection to the Admin UI.

Because these pages are being migrated to the Admin UI proper, there is no
pressing need to improve the user experience beyond the redirect.

### Insecure Mode

If the cluster is running explicitly in "insecure" mode, the following changes
will apply:

+ All requests will be acceptable. Where session validation would normally
occur, an empty session ID and the "root" user will be added to the request
context.
+ CSRF Token validation will not be applied.

Insecure mode is trivially indicated to the client by serving from "http"
addresses instead of "https". The client can thus check javascript variable
`window.location.protocol` and adjust its behavior accordingly.

## Frontend Changes

### Logged-in State

The front-end will be modified to have a "logged in" and "logged out" state.
The logged-in state will be equivalent to the current behavior of the UI. The
"logged out" state displays a full-screen prompt for the user to enter
credentials.

This mode will be enforced with the following mechanism:

+ A new "logged in" value will be added to the Admin UI state.
+ If the "logged in" value is not present, the top-level "Layout" element
will display the full-screen login dialogue instead of the currently requested
route component.
+ Upon successful login, the username used to log in is added to the "logged in"
value in the Admin UI state. *The username is also recorded to LocalStorage*;
this is necessary because javascript explicitly will not have access to the
session cookie, and thus will not be able to recognize that it is logged in if
the session is resumed later.
+ If the "logged in" value is present, the top-level "Layout" element will
render the components of the currently requested route.
+ If any request comes back with a "401 Unauthorized", it is safe to assume that
the user's session is no longer valid, and the "logged in" Admin UI state will
thus be cleared.

### Login Dialog

The Login Dialog is a full-screen component that prompts the user for a username
and password.

This component will be displayed by the top-level "Layout" element if the
"logged in" value is not set in the Admin UI State. While the dialog is
displayed, no other navigation controls are accessible.

Upon successful login, the route requested by the user before seeing the login
prompt will be rendered.

### Logout button

After login, all pages will display a "log out" option in the top right corner
of the screen.

### CSRF Support

In order to perform CSRF properly, all outgoing requests will be need to read
the value of "csrf-token" cookie and send it back to the server in the
"x-csrf-token" HTTP header. This can be added in a central location at the method
`timeoutFetch`.

# Drawbacks

The primary drawback of this login system is that it requires a session lookup
for every request. If that proves expensive, the cost of this could be
significantly mitigated by adding a short-term cache for sessions on each node.

# Alternatives

### Stateless Sessions

One alternatives considered was a "Stateless Session", where there would be
no sessions table, but instead session information would be encoded using a
"Javascript Web Token" (JWT). This is a signed object returned to the user
instead of a session token; the object contains the username, csrf token,
and session id. Using JWT, servers would not need to consult a sessions table
for incoming requests, but instead would simply need to verify the signature
on the token.

The major issue with JWT is that it does not provide a way to revoke login
sessions; to do this, we would need to store a blocklist of revoked session IDs,
which removes much of the advantage of not having the sessions table in the
first place.

JWT would also require all machines on the cluster to maintain a shared secret
for signing the tokens.

### HTTP Basic Auth

The simplest option may be HTTP Basic authorization, wherein the browser allows
the user to "log in", and sends a username/password combination with every
request to the server. This requires no implementation on our part beyond
verifying username/password, and in combination with HTTPS there is very little
risk of being compromised by an attacker.

However, this has two main drawbacks:

+ It does not allow us to track actions at the session level, which is desirable
from the perspective of auditing.
+ It does not allow us to provide a custom "login" dialog, a "logout" button, or
persistent client sessions.

# Unresolved Questions

### 401 Unauthorized

The "401 Unauthorized" response seems to be the most semantically correct HTTP
code to return for unauthenticated attempts to access API methods. However,
returning a 401 from a request seems to cause browsers to display
username/password dialog for HTTP Basic auth, which we do not want to happen.

We can try returning 401 responses *without* the WWW-Authenticate method, but
that seems to be in [violation of HTTP
standards](https://tools.ietf.org/html/rfc7235#section-3.1). We could try
sending a custom value in the WWW-authenticate field (such as "custom"), but
it's also not clear that this will prevent the browser from preventing a
login dialog pop-up.

If 401 proves to be problematic in this way, we will instead send 403 Forbidden
in all cases where 401 is used in this RFC.
