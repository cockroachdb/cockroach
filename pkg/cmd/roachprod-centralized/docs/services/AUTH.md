# Authentication & Authorization Service

This document provides an implementation guide for the authentication and authorization system in roachprod-centralized.

**Related Documentation:**
- [Architecture](../ARCHITECTURE.md)
- [API Reference](../API.md)
- [Metrics](../METRICS.md)

## Table of Contents

- [Overview](#overview)
- [Authentication Implementations](#authentication-implementations)
- [Authentication Flows](#authentication-flows)
- [Architecture](#architecture)
- [Key Components](#key-components)
- [Permissions System](#permissions-system)
- [SCIM Provisioning](#scim-provisioning)
- [Extending the System](#extending-the-system)
- [Configuration](#configuration)
- [Bootstrap Configuration](#bootstrap-configuration)

---

## Overview

The authentication system uses opaque bearer tokens for all API authentication, with two primary flows:

1. **Human Flow** - Users authenticate via Okta Device Authorization Grant and exchange for an opaque API token
2. **Machine-to-Machine Flow** - Service accounts with admin-issued tokens for CI/automation

Key design decisions:
- **Opaque tokens over JWTs** - Enables instant revocation without token blacklists
- **Hash-at-rest** - Tokens stored as SHA-256 hashes, never in plaintext
- **Dynamic permission resolution** - Permissions computed at request time from group memberships
- **SCIM 2.0** - User and group provisioning via Okta SCIM

---

## Authentication Implementations

The API supports three authentication implementations, configured via `AUTH_TYPE`:

| Implementation | Config Value | Use Case | Description |
|----------------|--------------|----------|-------------|
| **Bearer** | `bearer` | Production | Opaque tokens with database-backed users, groups, and permissions |
| **JWT** | `jwt` | Google IAP | Google ID tokens validated via OIDC; grants wildcard permissions |
| **Disabled** | `AUTH_DISABLED=true` | Development | Bypasses authentication; always returns admin principal |

### Controllers by Authentication Type

Not all controllers are available in every authentication mode. Controllers that manage database-backed entities (users, groups, tokens, service accounts) are only available with bearer authentication:

| Controller | Bearer | JWT / Disabled | Description |
|------------|--------|----------------|-------------|
| `health` | ✓ | ✓ | Health checks |
| `clusters` | ✓ | ✓ | Cluster management |
| `public-dns` | ✓ | ✓ | DNS synchronization |
| `tasks` | ✓ | ✓ | Background task management |
| `auth` (WhoAmI) | ✓ | ✓ | Returns current principal info |
| `auth` (Bearer) | ✓ | ✗ | Okta token exchange, self-service token management |
| `service-accounts` | ✓ | ✗ | Service account CRUD and token minting |
| `admin` | ✓ | ✗ | User and group administration |
| `scim` | ✓ | ✗ | SCIM 2.0 user/group provisioning |

**Why are some controllers disabled?**

With `jwt` or `disabled` authentication:
- Principals are not stored in the database—they're derived from JWT claims or created on-the-fly
- Creating service accounts or tokens would be pointless since SA tokens require bearer authentication to validate
- User/group management via admin or SCIM has no effect since permissions aren't database-driven

### Bearer Authentication (Production)

The bearer authenticator validates opaque tokens against the database:

1. Hash the provided token with SHA-256
2. Look up the hash in `api_tokens` table
3. Load associated user or service account
4. Resolve permissions from groups (users) or `service_account_permissions` (orphan SAs)
5. Return fully-populated Principal

**Enabled endpoints:**
- `POST /v1/auth/okta/exchange` - Exchange Okta ID token for opaque bearer token
- `GET /v1/auth/tokens` - List own tokens
- `DELETE /v1/auth/tokens/:id` - Revoke own token
- All service-accounts, admin, and SCIM endpoints

### JWT Authentication (Google IAP)

The JWT authenticator validates Google ID tokens:

1. Validate token signature and claims via Google's `idtoken` library
2. Verify issuer and audience match configuration
3. Extract email and name from claims
4. Return Principal with wildcard (`*`) permissions

**Use case:** When the API runs behind Google Cloud Identity-Aware Proxy (IAP).

**Limitations:**
- No database-backed user management
- No fine-grained permissions: all authenticated users have full access
- No token revocation capabilities

### Disabled Authentication (Development)

The disabled authenticator bypasses all authentication:

1. Always succeeds
2. Returns a default "dev@localhost" principal with wildcard permissions

**Use case:** Local development and testing only. Never use in production.

---

## Authentication Flows

### Human Flow (Okta Exchange)

```
CLI                  Okta                 Backend
 |                    |                      |
 |-- Device Flow ---->|                      |
 |<-- ID Token -------|                      |
 |                    |                      |
 |----- POST /api/v1/auth/okta/exchange ---->|
 |                    |     Validate token   |
 |                    |     Lookup user      |
 |                    |     Issue opaque token
 |<------------ rp$user$1$... ---------------|
```

The exchange endpoint validates the Okta token using OIDC discovery (`.well-known/openid-configuration`), looks up the user by `okta_user_id`, and issues an opaque token.

**Entry point:** [controllers/auth/controller.go](../../controllers/auth/controller.go) `OktaExchange()`

### Service Account Flow

Administrators create service accounts, assign permissions, and mint tokens:

```go
// 1. Create SA
POST /api/v1/service-accounts

// 2. Assign permissions
POST /api/v1/service-accounts/:id/permissions

// 3. Optional: Add IP allowlist
POST /api/v1/service-accounts/:id/origins

// 4. Mint token (returned once, stored as hash)
POST /api/v1/service-accounts/:id/tokens
```

**Entry point:** [controllers/service-accounts/service_accounts.go](../../controllers/service-accounts/service_accounts.go)

### Service Account Types

Service accounts come in two types, determined by the `delegated_from` field:

| Type | `delegated_from` | Permission Source | Use Case |
|------|------------------|-------------------|----------|
| **Orphan** | `NULL` | `service_account_permissions` table | CI/CD automation, monitoring, SCIM provisioning |
| **Delegated** | User UUID | Inherited from user's group permissions | User-created automation that acts on their behalf |

#### Delegated Service Accounts (Default)

Delegated SAs inherit permissions from a user principal. They have no entries in `service_account_permissions`; instead, their permissions are resolved at authentication time from the delegated user's group memberships.

**When to use:** When a user wants to create automation that acts with their own permissions. The SA automatically gains/loses permissions as the user's group memberships change.

**Creating a delegated SA:**
```bash
# Omit "orphan" field or set to false (default behavior)
curl -X POST /api/v1/service-accounts \
  -H "Authorization: Bearer $USER_TOKEN" \
  -d '{"name": "my-automation", "description": "Runs with my permissions"}'

# Response includes delegated_from = creating user's ID
{
  "id": "...",
  "name": "my-automation",
  "delegated_from": "11111111-1111-1111-1111-111111111111",  // User's ID
  ...
}
```

**Permission resolution:**
```sql
-- Delegated SA: permissions come from the delegated user's groups
SELECT gp.permission FROM group_permissions gp
JOIN groups g ON g.display_name = gp.group_name
JOIN group_members gm ON gm.group_id = g.id
WHERE gm.user_id = :delegated_from;
```

**Characteristics:**
- `delegated_from = <user_uuid>` (the user whose permissions are inherited)
- Cannot have explicit permissions added via the API (returns HTTP 403)
- Permissions automatically update when the delegated user's group memberships change
- Can create other delegated SAs (they inherit the same `delegated_from` user)

#### Orphan Service Accounts

Orphan SAs have their own explicit permissions stored in the `service_account_permissions` table. They are independent entities that don't inherit from any user.

**When to use:** For system-level automation that needs specific, controlled permissions independent of any user. Examples: SCIM provisioning, CI/CD pipelines, monitoring systems.

**Creating an orphan SA:**
```bash
# Set "orphan": true explicitly
curl -X POST /api/v1/service-accounts \
  -H "Authorization: Bearer $USER_TOKEN" \
  -d '{"name": "ci-automation", "description": "CI pipeline", "orphan": true}'

# Response has no delegated_from field
{
  "id": "...",
  "name": "ci-automation",
  ...
}

# Then grant explicit permissions
curl -X POST /api/v1/service-accounts/:id/permissions \
  -d '{"scope": "gcp-my-project", "permission": "clusters:create"}'
```

**Permission resolution:**
```sql
-- Orphan SA: permissions come from service_account_permissions
SELECT permission FROM service_account_permissions
WHERE service_account_id = :sa_id;
```

**Characteristics:**
- `delegated_from = NULL`
- Permissions are explicitly granted via `POST /service-accounts/:id/permissions`
- Can be created by users or delegated service accounts
- Cannot create delegated service accounts (no user context to delegate from)

#### Creation Rules

| Creator Principal | Can Create Orphan? | Can Create Delegated? | Delegated From |
|-------------------|--------------------|-----------------------|----------------|
| User | Yes | Yes | Creator's user ID |
| Delegated SA | Yes | Yes | SA's `delegated_from` user ID |
| Orphan SA | Yes | **No** (403 Forbidden) | N/A |

The restriction on orphan SAs creating delegated SAs prevents privilege escalation—orphan SAs have no user context to delegate from.

#### Permission Modification Rules

| SA Type | Add/Remove Permissions | Reason |
|---------|------------------------|--------|
| Orphan | Allowed | Permissions are explicit |
| Delegated | **Forbidden** (403) | Permissions are inherited from user |

**Error codes:**
- `ErrSACreationNotAllowedFromOrphanSA`: Orphan SA tried to create a delegated SA
- `ErrNonOrphanSAPermissionModification`: Attempted to modify permissions on a delegated SA

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Controllers                             │
│  auth/  │  service-accounts/  │  admin/  │  scim/               │
└────┬────┴──────────┬──────────┴────┬─────┴────┬─────────────────┘
     │               │               │          │
     └───────────────┴───────┬───────┴──────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    Auth Middleware                              │
│  authn_authz.go: AuthMiddleware() + AuthzMiddleware()           │
│  ─ Extracts token from Authorization header                     │
│  ─ Calls authenticator.Authenticate()                           │
│  ─ Checks authorization requirements                            │
│  ─ Stores Principal in context                                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    IAuthenticator Interface                     │
│  auth/interface.go                                              │
│  ├─ bearer/bearer.go    (production - opaque tokens)            │
│  ├─ jwt/jwt.go          (Google IAP integration)                │
│  └─ disabled/disabled.go (development bypass)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    Auth Service                                 │
│  services/auth/service.go                                       │
│  ├─ authenticate.go  (token validation, principal loading)      │
│  ├─ tokens.go        (token CRUD, revocation)                   │
│  ├─ users.go         (user management)                          │
│  ├─ groups.go        (group management)                         │
│  ├─ service_accounts.go (SA management)                         │
│  └─ okta.go          (Okta token validation)                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    Auth Repository                              │
│  repositories/auth/repository.go (interface)                    │
│  repositories/auth/cockroachdb/  (implementation)               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Components

### Principal (`auth/interface.go`)

The `Principal` struct represents an authenticated user or service account:

```go
type Principal struct {
    Token            TokenInfo          // Token metadata (ID, type, expiry)
    UserID           *uuid.UUID         // Set for user tokens
    ServiceAccountID *uuid.UUID         // Set for SA tokens
    User             *authmodels.User   // Full user (bearer auth)
    ServiceAccount   *authmodels.ServiceAccount
    Permissions      []authmodels.Permission  // Resolved permissions
    Claims           map[string]interface{}   // JWT claims (if JWT auth)
}
```

### IAuthenticator Interface (`auth/interface.go`)

All authenticators implement this interface:

```go
type IAuthenticator interface {
    // Authenticate validates token and returns Principal
    Authenticate(ctx context.Context, token string, clientIP string) (*Principal, error)

    // Authorize checks if Principal has required permissions
    Authorize(ctx context.Context, principal *Principal, req *AuthorizationRequirement, endpoint string) error
}
```

### AuthorizationRequirement (`auth/interface.go`)

Defines what permissions are needed for an endpoint:

```go
type AuthorizationRequirement struct {
    RequiredPermissions []string  // ALL required (AND logic)
    AnyOf              []string   // ANY required (OR logic)
}
```

### Bearer Authenticator (`auth/bearer/bearer.go`)

The production authenticator that:
1. Calls `authService.AuthenticateToken()` to validate token
2. Returns fully-loaded Principal with permissions
3. Records authentication metrics

### Token Format

Tokens use a structured format: `rp$<type>$<version>$<random>`

| Component | Description |
|-----------|-------------|
| `rp` | Application prefix |
| `<type>` | `user` or `sa` |
| `<version>` | Format version (`1`) |
| `<random>` | 43 chars of base62 entropy |

Token suffix (`rp$user$1$****<last8>`) is stored for audit logging.

---

## Permissions System

### Permission Format

Permissions follow: `<service>:<resource>:<action>:<scope>`

**Auth service permissions:**
```go
auth:scim:manage-user
auth:service-accounts:create
auth:service-accounts:view:all
auth:service-accounts:view:own
auth:tokens:view:all
auth:tokens:revoke:own
// ... defined in services/auth/types/types.go
```

**Cluster service permissions:**
```go
clusters:create
clusters:view:all
clusters:view:own
// ... defined in services/clusters/types/types.go
```

### Permission Resolution

**For users** (in `services/auth/authenticate.go`):
1. Load user from `users` table
2. Join `group_members` → `groups` → `group_permissions`
3. Aggregate all matching permissions

**For orphan service accounts** (`delegated_from = NULL`):
1. Load SA from `service_accounts` table
2. Load from `service_account_permissions` table

**For delegated service accounts** (`delegated_from = user_id`):
1. Load SA from `service_accounts` table
2. Load the delegated user's permissions via group memberships
3. SA inherits all permissions from the delegated user's groups

### Checking Permissions

```go
// Simple check (any scope)
if principal.HasPermission("clusters:create") { ... }

// Scoped check (specific scope/environment)
if principal.HasPermissionScoped("clusters:create", "gcp-my-project") { ... }

// Any of multiple permissions (OR)
if principal.HasAnyPermission([]string{"clusters:view:all", "clusters:view:own"}) { ... }
```

### Authorization Boundary (Controller vs Service)

Authorization is intentionally split across two layers:

1. Controller layer (coarse gate):
   - Declare required permission family on the route with `AuthorizationRequirement`.
   - Keep controller checks generic and easy to audit.
2. Service layer (fine-grained decision):
   - Enforce scope/environment constraints via `HasPermissionScoped(...)`.
   - Enforce ownership/resource checks using trusted persisted data.
   - Apply final authorization decision before mutations.

#### Practical rules when adding endpoints

1. Route declaration should express coarse intent only:
   - Example: `clusters:create`, `clusters:view:all|own`, `clusters:update:all|own`.
2. Service implementation must enforce resource-specific constraints:
   - For create: validate scopes against provider environments from server config.
   - For update/delete/read: load resource first, then authorize using stored resource attributes.
3. Do not authorize from untrusted request fields alone:
   - Request payload can be malformed or malicious.
   - Authorization-critical checks should use trusted state (database + server config).

### Controller Handler Registration

Permissions are declared on controller handlers:

```go
&controllers.ControllerHandler{
    Method: "POST",
    Path:   "/api/v1/clusters",
    Func:   ctrl.Create,
    Authorization: &auth.AuthorizationRequirement{
        AnyOf: []string{
            clustermodels.PermissionCreate,
        },
    },
}
```

---

## SCIM Provisioning

### SCIM Controller (`controllers/scim/`)

Implements SCIM 2.0 for Users and Groups:

| Endpoint | Controller Method |
|----------|------------------|
| GET/POST/PUT/PATCH/DELETE `/scim/v2/Users` | `scim.go` |
| GET/POST/PUT/PATCH/DELETE `/scim/v2/Groups` | `groups.go` |
| GET `/scim/v2/ServiceProviderConfig` | Discovery |
| GET `/scim/v2/Schemas` | Discovery |
| GET `/scim/v2/ResourceTypes` | Discovery |

### User Lifecycle

| SCIM Event | Backend Action |
|------------|----------------|
| User created | Insert into `users`, `active=true` |
| User deactivated | Set `active=false`, revoke all tokens |
| User reactivated | Set `active=true` |
| User deleted | Hard delete from `users` |

### Group → Permission Mapping

Groups are mapped to permissions via `group_permissions` table:

```sql
-- Okta group "Division-Engineering" gets clusters:create for all GCP projects
INSERT INTO group_permissions (group_name, scope, permission)
VALUES ('Division-Engineering', 'gcp-engineering', 'clusters:create');
```

The `group_name` column matches the `display_name` of groups in the `groups` table.

---

## Extending the System

### Adding a New Permission

1. Define constant in service's `types/types.go`:
   ```go
   const PermissionNewAction = TaskServiceName + ":new-action"
   ```

2. Add to controller handler's `Authorization`:
   ```go
   Authorization: &auth.AuthorizationRequirement{
       AnyOf: []string{PermissionNewAction},
   },
   ```

3. Add group permission mapping in database (if users need it)

### Adding a New Authenticator

1. Implement `IAuthenticator` interface in `auth/<type>/<type>.go`
2. Register in app factory when building the API
3. Update `AuthenticationType` enum if adding new type

### Adding SCIM Resource Type

1. Create new controller in `controllers/scim/`
2. Implement SCIM CRUD operations
3. Register routes with appropriate authorization
4. Add metrics for the new resource type

---

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `AUTH_DISABLED` | Disable auth (dev only) | `false` |
| `AUTH_TYPE` | `bearer` or `jwt` | `bearer` |
| `OKTA_ISSUER` | Okta issuer URL | Required |
| `OKTA_AUDIENCE` | Okta audience | Required |
| `AUTH_TOKEN_TTL` | Default token TTL | `168h` |
| `AUTH_CLEANUP_INTERVAL` | Expired token cleanup | `24h` |
| `BOOTSTRAP_SCIM_TOKEN` | Bootstrap token for initial SCIM provisioning | (empty) |

---

## Bootstrap Configuration

When deploying with bearer authentication, you need a way to bootstrap the first service account for SCIM provisioning. The chicken-and-egg problem: you need a service account token to call the SCIM API, but you can't create service accounts without first provisioning users.

### Bootstrap Token Mechanism

The bootstrap token solves this by allowing you to pre-configure a token that creates an initial SCIM service account on first startup:

1. **Generate a token** in the correct format: `rp$sa$1$<43-chars-base62-entropy>`
2. **Configure the environment variable**: `ROACHPROD_BOOTSTRAP_SCIM_TOKEN=<token>`
3. **Start the service**: On first boot, if no service accounts exist, the bootstrap SA is created automatically
4. **Configure Okta SCIM**: Use the bootstrap token in Okta's SCIM provisioning configuration
5. **Token expires in 6 hours**: The bootstrap token has a short TTL for security

### Token Format Requirements

The bootstrap token must:
- Start with the prefix `rp$sa$1$` (service account token, version 1)
- Have at least 43 characters of base62 entropy after the prefix
- Total minimum length: 51 characters

**Generate a valid token:**
```bash
# Using openssl to generate entropy
TOKEN="rp\$sa\$1\$$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 43)"
echo "$TOKEN"
```

### Bootstrap Service Account Permissions

The bootstrap service account is created as an orphan SA with these permissions:
- `auth:scim:manage-user` - Manage users and groups via SCIM
- `auth:service-accounts:create` - Create additional service accounts
- `auth:service-accounts:view:all` - View any service account
- `auth:service-accounts:update:all` - Update any service account
- `auth:service-accounts:delete:all` - Delete any service account
- `auth:service-accounts:mint:all` - Mint tokens for any service account
- `auth:tokens:view:all` - View all tokens
- `auth:tokens:revoke:own` - Revoke own tokens

### Security Considerations

1. **Short TTL (6 hours)**: The bootstrap token expires quickly to limit exposure
2. **One-time creation**: The bootstrap SA is only created if no service accounts exist
3. **Audit logging**: Bootstrap SA creation is logged with `bootstrap: true` flag
4. **Strong entropy**: The token requires 43+ chars of base62 (~256 bits entropy)
5. **Rotate after setup**: After Okta SCIM is configured, mint a new long-lived token for the SCIM SA and revoke the bootstrap token

### Production Setup Workflow

```bash
# 1. Generate bootstrap token
export BOOTSTRAP_TOKEN="rp\$sa\$1\$$(openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 43)"

# 2. Start the service with bootstrap token
export ROACHPROD_BOOTSTRAP_SCIM_TOKEN="$BOOTSTRAP_TOKEN"
export ROACHPROD_API_AUTHENTICATION_TYPE=bearer
export ROACHPROD_API_AUTHENTICATION_BEARER_OKTA_ISSUER="https://your-org.okta.com"
roachprod-centralized api

# 3. Configure Okta SCIM with the bootstrap token
#    - Add SCIM app in Okta
#    - Set API endpoint: https://your-api/scim/v2
#    - Set Bearer token: $BOOTSTRAP_TOKEN
#    - Enable user/group provisioning

# 4. (Optional) Mint a permanent token for SCIM and revoke bootstrap
#    The bootstrap token expires in 6 hours anyway, but you can:
#    - Create a new long-lived SCIM service account
#    - Update Okta SCIM with the new token
```

### Troubleshooting Bootstrap

**Token format error:**
```
Error: bootstrap SCIM token must start with prefix "rp$sa$1$"
```
Ensure the token format is correct. Common issues:
- Shell escaping: use `\$` or single quotes
- Missing prefix components

**Token entropy error:**
```
Error: bootstrap SCIM token must have at least 43 characters of entropy
```
The random portion after `rp$sa$1$` must be at least 43 characters.

**Bootstrap skipped:**
```
Log: service accounts already exist, skipping bootstrap
```
This is expected on subsequent startups. The bootstrap only runs once when no SAs exist.

---

*For database schema details, see the migrations in `repositories/auth/cockroachdb/migrations_definition.go`*
