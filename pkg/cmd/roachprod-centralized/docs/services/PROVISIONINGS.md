# Provisionings

The provisionings service manages infrastructure lifecycle through OpenTofu
templates. It handles creation, destruction, lifetime management, and
post-provisioning automation (hooks) for ephemeral cloud resources.

**Related Documentation:**
- [Environments Guide](ENVIRONMENTS.md) - Environment variables and secret management
- [Template Authoring Guide](TEMPLATES.md) - How to create and structure templates
- [Hooks Guide](HOOKS.md) - Post-provisioning automation (SSH, commands)

## Table of Contents

- [Quick Start](#quick-start)
- [Concepts](#concepts)
- [CLI Reference](#cli-reference)
- [Lifecycle](#lifecycle)
- [Lifetime and Garbage Collection](#lifetime-and-garbage-collection)
- [Variables](#variables)
- [Permissions](#permissions)

## Quick Start

```bash
# List available templates
roachprod prov templates

# Show template details (variables, defaults)
roachprod prov templates gce-instance

# Create a provisioning (attached mode: streams logs)
roachprod prov create \
  --type gce-instance \
  --env gcp-ephemeral \
  --var gcp_project=my-project \
  --var instance_status=RUNNING

# Create in detached mode (prints task ID, exits immediately)
roachprod prov create --type gce-instance --env gcp-ephemeral -d

# Check status
roachprod prov get <provisioning-id>

# View outputs (IPs, resource names, etc.)
roachprod prov outputs <provisioning-id>

# Extend lifetime
roachprod prov extend <provisioning-id>

# Destroy
roachprod prov destroy <provisioning-id>

# Delete the record (only after destroy completes)
roachprod prov delete <provisioning-id>
```

## Concepts

A **provisioning** is a managed lifecycle wrapper around an OpenTofu (Terraform)
deployment. It bundles:

- A **template** (OpenTofu `.tf` files + `template.yaml` metadata)
- An **environment** (cloud credentials, project IDs, secrets)
- User-provided **variables** (region, instance type, etc.)
- **Hooks** for post-provisioning automation (optional)

The system snapshots the template at creation time, so later template changes
don't affect running provisionings. Destruction always uses the same template
version that was used for creation.

## CLI Reference

All commands are under `roachprod provisioning` (alias: `roachprod prov`).

### `prov templates [name]`

List all templates, or show details for a specific template.

```bash
roachprod prov templates                    # list all
roachprod prov templates gce-instance       # show details
roachprod prov templates gce-instance -o json
```

### `prov create`

Create a new provisioning and schedule infrastructure deployment.

```bash
roachprod prov create \
  --type <template>     \  # required: template name
  --env <environment>   \  # required: environment name
  --var key=value       \  # repeatable: variable overrides
  --var-file vars.yaml  \  # YAML/JSON file with variables
  --lifetime 24h        \  # override default lifetime
  -d                       # detach: don't stream logs
```

**Variable precedence** (highest wins):
1. Auto-injected (`identifier`, `prov_name`, `environment`, `owner`)
2. `--var` flags
3. `--var-file` values
4. Environment plaintext variables matching template variable names
5. Template defaults

**Dotted paths** are supported for nested objects:

```bash
roachprod prov create --type gce-instance --env test \
  --var instance_config.zone=us-west1-a \
  --var instance_config.disk_size_gb=1024
```

### `prov list`

List provisionings with optional filters.

```bash
roachprod prov list
roachprod prov list --state provisioned
roachprod prov list --env gcp-ephemeral --owner alice@example.com
roachprod prov list -o json
```

Filters: `--state`, `--env`, `--owner`.

### `prov get <id>`

Show provisioning details.

```bash
roachprod prov get <id>
roachprod prov get <id> -o json
```

### `prov plan <id>`

Show the OpenTofu plan output (JSON).

### `prov outputs <id>`

Show terraform outputs (IPs, resource names, etc.).

```bash
roachprod prov outputs <id>         # table format
roachprod prov outputs <id> -o json
```

### `prov logs <task-id>`

Stream task logs in real-time (SSE). The task ID is printed by `create`,
`destroy`, and `setup-ssh` commands.

```bash
roachprod prov logs <task-id>
```

### `prov extend <id>`

Extend the provisioning's expiration by the configured extension duration
(default: 12 hours).

### `prov destroy <id>`

Destroy the infrastructure. Use `-d` for detached mode.

```bash
roachprod prov destroy <id>
roachprod prov destroy <id> -d
```

### `prov delete <id>`

Remove the provisioning record from the database. Only allowed when state is
`new` or `destroyed`.

### `prov setup-ssh <id>`

Trigger SSH key refresh on all machines. Fetches engineer SSH public keys from
GCP project metadata and writes them to `~/.ssh/authorized_keys` on each
machine. See the [Hooks Guide](HOOKS.md) for details.

```bash
roachprod prov setup-ssh <id>
roachprod prov setup-ssh <id> -d
```

## Lifecycle

A provisioning moves through these states:

```
                     +---------+
                     |   new   |
                     +----+----+
                          |
                     +----v----+
                     |  init   | tofu init
                     +----+----+
                          |
                     +----v----+
                     | planning| tofu plan
                     +----+----+
                          |
                     +----v--------+
                     | provisioning| tofu apply
                     +----+--------+
                          |
                     +----v--------+     +------------+
                     | provisioned |---->| destroying  | tofu destroy
                     +-------------+     +------+-----+
                                                |
                                         +------v-----+
                                         |  destroyed  |
                                         +-------------+
```

**Failure states:**
- `failed` - init, plan, or apply failed (can retry via destroy + recreate)
- `destroy_failed` - destroy failed (can retry destroy)

**Notes:**
- Post-apply hooks run between `provisioning` and `provisioned` (inline, not a
  separate task). A non-optional hook failure moves the state to `failed`.
- Destruction uses the same template snapshot as the original creation.

## Lifetime and Garbage Collection

Every provisioning has an expiration time (`ExpiresAt`). When it expires, the
GC scheduler automatically destroys the infrastructure.

**Lifetime sources** (priority order):
1. User-provided `--lifetime` flag at creation
2. Template's `default_lifetime` in `template.yaml`
3. Global default (server config, default: `12h`)

**Extension:** `roachprod prov extend <id>` adds the configured extension
duration (default: 12 hours) to the current expiration.

**GC behavior:**
- Runs every 5 minutes (configurable)
- `new` state: marked `destroyed` immediately (no infra to clean up)
- Other states: schedules a destroy task
- Prevents duplicate destroy tasks for the same provisioning

## Variables

### Auto-injected variables

These are always available to templates. Declare them in `vars.tf` to use them.

| Variable | Always injected | Description | Example |
|----------|----------------|-------------|---------|
| `identifier` | Yes | 8-char random string (letter + alphanumeric) | `ab12cd34` |
| `prov_name` | If declared | `{template}-{identifier}` | `gce-instance-ab12cd34` |
| `environment` | If declared | Environment name | `gcp-ephemeral` |
| `owner` | If declared | Creator's email | `alice@example.com` |

### Environment variable types

Variables from environments are delivered to OpenTofu in different ways
depending on their type:

| Type | `TF_VAR_*` | `-var` flag | Raw env | Use case |
|------|-----------|------------|---------|----------|
| `plaintext` | Yes | Yes | Yes | Non-sensitive config (project, region) |
| `template_secret` | Yes | No | Yes | Terraform-consumed secrets (SSH keys) |
| `secret` | No | No | Yes | Provider credentials (not for Terraform) |

`-var` flags are visible in process listings. Secrets use `TF_VAR_*` environment
variables or raw environment injection to avoid exposure.

## Permissions

| Permission | Scope | Operations |
|-----------|-------|------------|
| `provisionings:create` | - | Create provisionings |
| `provisionings:view:all` | All | List, get, plan, outputs |
| `provisionings:view:own` | Own | List, get, plan, outputs |
| `provisionings:update:all` | All | Extend lifetime, setup SSH keys |
| `provisionings:update:own` | Own | Extend lifetime, setup SSH keys |
| `provisionings:destroy:all` | All | Destroy, delete |
| `provisionings:destroy:own` | Own | Destroy, delete |

Ownership is determined by the `owner` field (email of the creating principal).
