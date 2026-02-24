# Environments

Environments are named collections of variables that provide configuration and
credentials to provisioning templates. They decouple infrastructure definitions
(templates) from deployment-specific values (project IDs, regions, secrets).

**Related Documentation:**
- [Provisionings Guide](PROVISIONINGS.md) - How provisionings consume environments
- [Template Authoring Guide](TEMPLATES.md) - How templates reference environment variables

## Table of Contents

- [Quick Start](#quick-start)
- [Concepts](#concepts)
- [CLI Reference](#cli-reference)
- [Variable Types](#variable-types)
- [Secret Management](#secret-management)
- [How Variables Reach OpenTofu](#how-variables-reach-opentofu)
- [Validation Rules](#validation-rules)
- [Permissions](#permissions)
- [Limitations](#limitations)

## Quick Start

```bash
# Create an environment
roachprod env create gcp-ephemeral --description "Ephemeral GCP resources"

# Add plaintext variables (non-sensitive config)
roachprod env var set gcp-ephemeral gcp_project my-gcp-project-id
roachprod env var set gcp-ephemeral region us-central1

# Add a secret (auto-written to GCP Secret Manager)
roachprod env var set gcp-ephemeral ssh_private_key "$(cat ~/.ssh/id_ed25519)" --secret

# Add a template_secret (sensitive template variable)
roachprod env var set gcp-ephemeral db_password "hunter2" --type template_secret

# Or reference an existing GCP secret directly
roachprod env var set gcp-ephemeral ssh_private_key \
  "gcp:projects/my-project/secrets/ssh-key/versions/latest" --secret

# List variables (secrets are masked)
roachprod env var list gcp-ephemeral

# Use the environment in a provisioning
roachprod prov create --type gce-instance --env gcp-ephemeral
```

## Concepts

An **environment** bundles all the variables needed to deploy a template in a
specific context. For example:

- `gcp-ephemeral` — GCP project for short-lived test resources
- `aws-staging` — AWS account for staging deployments
- `gcp-production` — GCP project for production infrastructure

Each environment has:
- A unique **name** (immutable after creation)
- An **owner** (set automatically from the creating principal)
- A set of **variables** (key-value pairs with types)

When a provisioning is created, the system resolves the environment's variables
and makes them available to the OpenTofu template. Secret references are
fetched from their providers (e.g., GCP Secret Manager) at resolution time.

## CLI Reference

All commands are under `roachprod environment` (alias: `roachprod env`).

### `env list`

List all environments visible to the current user.

```bash
roachprod env list
roachprod env list -o json
```

### `env create <name>`

Create a new environment. The current user becomes the owner.

```bash
roachprod env create gcp-ephemeral
roachprod env create gcp-ephemeral --description "Ephemeral GCP resources"
```

### `env delete <name>`

Delete an environment. Fails if provisionings reference it (FK constraint).

```bash
roachprod env delete gcp-ephemeral
```

### `env var list <env>`

List all variables for an environment. Secret values are masked as `********`.

```bash
roachprod env var list gcp-ephemeral
roachprod env var list gcp-ephemeral -o json
```

### `env var get <env> <key>`

Show a single variable. Secret values are masked.

```bash
roachprod env var get gcp-ephemeral gcp_project
```

### `env var set <env> <key> <value>`

Create or update a variable. If the variable already exists, it is updated
(upsert pattern).

```bash
# Plaintext (default)
roachprod env var set gcp-ephemeral gcp_project my-project

# Secret (provider credentials, raw env only)
roachprod env var set gcp-ephemeral AWS_ACCESS_KEY_ID AKIA... --secret

# Template secret (sensitive template variable, TF_VAR_* only)
roachprod env var set gcp-ephemeral db_password hunter2 --type template_secret

# Pre-existing GCP secret reference (verified, not auto-written)
roachprod env var set gcp-ephemeral ssh_key \
  "gcp:projects/my-project/secrets/ssh-key/versions/latest" --secret
```

### `env var delete <env> <key>`

Remove a variable from an environment.

```bash
roachprod env var delete gcp-ephemeral old_var
```

## Variable Types

Each variable has a type that controls how its value is delivered to OpenTofu
and whether it appears in process listings.

| Type | `-var` flag | `TF_VAR_*` env | Raw env | Use case |
|------|-----------|---------------|---------|----------|
| `plaintext` | Yes (if declared) | Yes | Yes | Non-sensitive config: project IDs, regions, instance types |
| `template_secret` | No | Yes | Yes | Sensitive template variables: passwords, certificates |
| `secret` | No | No | Yes | Provider credentials: `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS` |

**Why three types?**

- `-var` flags are visible in process listings (`ps aux`). Secrets must never
  appear there.
- `TF_VAR_*` environment variables are consumed by OpenTofu for declared
  template variables. Provider credentials (like `AWS_ACCESS_KEY_ID`) are NOT
  template variables — they're consumed by providers directly via their
  standard env var names.
- `template_secret` bridges the gap: it's a template variable (needs
  `TF_VAR_*`) but is sensitive (no `-var` flag).

### Choosing the right type

| Variable | Type | Why |
|----------|------|-----|
| `gcp_project` | `plaintext` | Non-sensitive, appears in plan output anyway |
| `region` | `plaintext` | Non-sensitive config |
| `ssh_private_key` | `template_secret` | Sensitive, consumed by template as `TF_VAR_ssh_private_key` |
| `db_password` | `template_secret` | Sensitive template variable |
| `AWS_ACCESS_KEY_ID` | `secret` | Provider credential, not a template variable |
| `GOOGLE_APPLICATION_CREDENTIALS` | `secret` | Provider credential path |

## Secret Management

### How secrets are stored

Secret-type variables (`secret` and `template_secret`) are not stored as raw
values in the database. Instead, the system stores a **reference** to a secret
manager entry.

When you set a secret variable:

1. **Raw value provided** (no prefix): The system auto-writes the value to
   GCP Secret Manager and stores the reference.
   ```
   Input:  "my-secret-password"
   Stored: "gcp:projects/my-project/secrets/env-name--var-key/versions/latest"
   ```

2. **Reference provided** (with prefix): The system verifies the reference is
   accessible and stores it as-is.
   ```
   Input:  "gcp:projects/my-project/secrets/existing-secret/versions/3"
   Stored: "gcp:projects/my-project/secrets/existing-secret/versions/3"
   ```

### GCP Secret Manager naming

When auto-writing, secrets are created with the ID `{envName}--{key}`:

```
Environment: gcp-ephemeral
Variable:    ssh_private_key
Secret ID:   gcp-ephemeral--ssh_private_key
Full ref:    gcp:projects/my-project/secrets/gcp-ephemeral--ssh_private_key/versions/latest
```

Secrets use automatic replication (geo-redundant). If the secret already
exists, a new version is added (preserving version history).

### Configuration

Auto-write requires the `Secrets.GCPProject` server config to be set:

```
ROACHPROD_SECRETS_GCP_PROJECT=my-gcp-project
```

If not set, raw secret values cannot be stored — you must provide pre-existing
GCP secret references instead.

### Resolution

When a provisioning is created, the system calls `GetEnvironmentResolved`
which fetches actual secret values from their providers. This resolved data
is never exposed via the API — it's only used internally by the task handler.

```
Stored:   "gcp:projects/my-project/secrets/env--key/versions/latest"
Resolved: "actual-secret-value-from-gcp"
```

References without a recognized prefix (e.g., no `gcp:` prefix) are treated
as literal values and returned as-is.

## How Variables Reach OpenTofu

When a provisioning runs, the system assembles variables from multiple sources
and delivers them to OpenTofu through different channels:

```
Environment Variables (resolved)
        │
        ├── plaintext vars ──────────────────┬── raw env (KEY=VALUE)
        │                                    ├── TF_VAR_KEY=VALUE
        │                                    └── -var KEY=VALUE (if declared in template)
        │
        ├── template_secret vars ────────────┬── raw env (KEY=VALUE)
        │                                    └── TF_VAR_KEY=VALUE
        │
        └── secret vars ─────────────────────└── raw env (KEY=VALUE)

User --var flags ────────────────────────────── -var KEY=VALUE

Auto-injected (identifier, prov_name, ...) ──── -var KEY=VALUE
```

**Plaintext variables** that match a declared template variable name are
automatically passed as `-var` flags. This is the primary mechanism for
non-sensitive configuration.

**Template secret variables** are delivered via `TF_VAR_*` environment
variables. OpenTofu picks these up automatically for declared variables with
matching names. They never appear as `-var` flags.

**Secret variables** are only delivered as raw environment variables (e.g.,
`AWS_ACCESS_KEY_ID=...`). They're consumed by cloud providers directly, not
by OpenTofu template variables.

## Validation Rules

### Environment names

- Length: 1-63 characters
- Allowed characters: `[a-zA-Z0-9_-]`
- Must be unique across the system
- Immutable after creation
- Rationale: embedded in GCP Secret Manager secret IDs (255-char limit)

### Variable keys

- Length: 1-190 characters
- Allowed characters: `[a-zA-Z0-9_-]`
- Must be unique within an environment
- Rationale: 255 (GCP limit) - 63 (max env name) - 2 (`--` separator) = 190

### Variable types

Must be one of: `plaintext`, `secret`, `template_secret`.

### Secret verification

When a secret reference with a recognized prefix (e.g., `gcp:...`) is
provided, the system verifies it's accessible before storing. If verification
fails, the request is rejected with a clear error.

## Permissions

| Permission | Scope | Operations |
|-----------|-------|------------|
| `environments:create` | - | Create environments |
| `environments:view:all` | All | List, get environments and variables |
| `environments:view:own` | Own | List, get own environments and variables |
| `environments:update:all` | All | Update environments, create/update/delete variables |
| `environments:update:own` | Own | Update own environments, manage own variables |
| `environments:delete:all` | All | Delete any environment |
| `environments:delete:own` | Own | Delete own environments |

Ownership is determined by the `owner` field set at creation time. Variable
mutations (create, update, delete) use the environment's `update` permissions
— there are no separate variable-level permissions.

## Limitations

### No instance isolation for secrets

The GCP Secret Manager naming convention is `{envName}--{key}` with no
instance-level prefix. If two roachprod-centralized instances share the same
GCP project and both have an environment named `prod` with a variable
`DB_PASSWORD`, they will read and write the same GCP secret.

**Current assumption:** One roachprod-centralized instance per GCP project. If
you need multiple instances sharing a project, use distinct environment names
or different GCP projects for each instance.

### No cascading delete with provisionings

Deleting an environment that has active provisionings fails with a foreign key
constraint error. Destroy all provisionings first, then delete the environment.

### Secret values never exposed via API

The API returns secret references (`gcp:projects/...`), never resolved values.
The `env var list` and `env var get` CLI commands mask secret values as
`********`. Resolved values are only used internally by the provisioning task
handler.
