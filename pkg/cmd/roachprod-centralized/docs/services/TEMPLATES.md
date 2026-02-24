# Template Authoring Guide

Templates are OpenTofu (Terraform) configurations packaged with metadata for
the roachprod-centralized provisioning system. This guide covers how to create,
structure, and test templates.

**Related Documentation:**
- [Provisionings Guide](PROVISIONINGS.md) - User-facing provisioning commands
- [Environments Guide](ENVIRONMENTS.md) - Environment variables and secret management
- [Hooks Guide](HOOKS.md) - Post-provisioning automation

## Table of Contents

- [Template Structure](#template-structure)
- [The Metadata File](#the-metadata-file)
- [Variables](#variables)
- [Auto-Injected Variables](#auto-injected-variables)
- [Outputs](#outputs)
- [Shared Modules](#shared-modules)
- [State Backend](#state-backend)
- [Snapshot and Versioning](#snapshot-and-versioning)
- [Example: GCE Instance](#example-gce-instance)
- [Checklist](#checklist)

## Template Structure

Templates live as subdirectories under the configured templates directory.
A directory is recognized as a deployable template if it contains a
`template.yaml` (or `template.yml`) marker file with a non-empty `name` field.

```
templates/
  gce-instance/           # deployable template
    template.yml           # metadata (required)
    vars.tf                # variable declarations
    main.tf                # resources
    output.tf              # outputs
    data.tf                # data sources, locals
    providers.tf           # provider configuration (no backend block)
  aurora-ec2/              # another template
    template.yaml
    vars.tf
    main.tf
    output.tf
    modules/               # symlinked shared modules
      aws-vpc -> ../../modules/aws-vpc
      aws-ec2-worker -> ../../modules/aws-ec2-worker
  modules/                 # shared modules (no template.yaml = not deployable)
    aws-vpc/
      main.tf
      variables.tf
    aws-ec2-worker/
      main.tf
      variables.tf
```

**File discovery rules:**
- All `.tf` files in the template directory are processed
- Hidden files (leading `.`), vim backups (`~`), and emacs backups (`#name#`)
  are ignored
- Override files (`override.tf`, `*_override.tf`) are loaded last
- Symlinks are resolved at snapshot time (the target files are archived)

**Do not** include a `backend` block in your `.tf` files. The system generates
`backend.tf` automatically (GCS or local, depending on deployment config).

## The Metadata File

`template.yaml` (or `template.yml`) is the marker file that declares a
directory as a template. It configures template identity, lifetime defaults,
SSH access, and post-provisioning hooks.

### Minimal example

```yaml
name: gce-instance
description: A GCE compute instance for testing.
```

### Full example

```yaml
name: gce-instance
description: A GCE compute instance with SSH key management.
default_lifetime: "24h"

ssh:
  private_key_var: ssh_private_key    # env var holding PEM private key
  machines: .instances[].public_ip    # jq expression on terraform outputs
  user: ubuntu                        # SSH username

hooks:
  - name: wait-ready
    type: run-command
    ssh: true
    command: test -f /.roachprod-initialized
    retry:
      interval: 10s
      timeout: 5m
    triggers: [post_apply]

  - name: setup-ssh-keys
    type: ssh-keys-setup
    ssh: true
    triggers: [post_apply, manual]
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Template identifier. Used as `template_type` when creating provisionings. |
| `description` | No | Human-readable description shown in `prov templates`. |
| `default_lifetime` | No | Default TTL for provisionings (Go duration: `12h`, `30m`). Falls back to global default. |
| `ssh` | No | SSH connection config for hooks. See [Hooks Guide](HOOKS.md). |
| `hooks` | No | Ordered list of post-provisioning hooks. See [Hooks Guide](HOOKS.md). |

## Variables

Template variables are declared in `.tf` files using standard HCL `variable`
blocks. The system parses these at template discovery time to build the schema
shown by `roachprod prov templates <name>`.

### Supported types

| HCL Type | Simplified | Notes |
|----------|-----------|-------|
| `string` | `string` | Default variable type |
| `number` | `number` | Integers and floats |
| `bool` | `bool` | `true`/`false` |
| `list(T)` | `list` | Ordered collection |
| `set(T)` | `set` | Unique unordered collection |
| `tuple([T1, T2])` | `tuple` | Fixed-length typed sequence |
| `object({...})` | `object` | Named fields with types |
| `map(T)` | `map` | String-keyed dictionary |

Complex types with `optional()` fields and nested defaults are fully supported.

### Variable sources

When a provisioning is created, variables are assembled from multiple sources.
Higher-priority sources override lower ones:

1. **Auto-injected** (always wins): `identifier`, `prov_name`, `environment`,
   `owner`
2. **User `--var` flags**: provided at creation time
3. **User `--var-file`**: YAML or JSON file
4. **Environment plaintext variables**: if the env var key matches a declared
   template variable name, the value is passed as a `-var` flag
5. **Environment `template_secret` variables**: delivered via `TF_VAR_*`
   environment variables (not visible in process listings)
6. **Template defaults**: from the `default` attribute in the `variable` block

If a variable is `required` (no default) and no source provides a value,
creation fails with a validation error.

## Auto-Injected Variables

The system automatically injects these variables into every provisioning.
Declare them in `vars.tf` to use them in your resources.

### `identifier` (always injected)

An 8-character random string. The first character is always a letter (`a-z`),
the rest are alphanumeric (`a-z0-9`). Useful for ensuring cloud resource name
uniqueness.

```hcl
variable "identifier" {
  type = string
}

resource "google_compute_instance" "worker" {
  name = "worker-${var.identifier}"
  # ...
}
```

### `prov_name` (injected if declared)

Format: `{template-type}-{identifier}` (e.g., `gce-instance-ab12cd34`).
Useful for human-readable resource names and tags.

```hcl
variable "prov_name" {
  type = string
}
```

### `environment` (injected if declared)

The environment name (e.g., `gcp-ephemeral`, `aws-staging`). Useful for
environment-aware configuration.

```hcl
variable "environment" {
  type = string
}
```

### `owner` (injected if declared)

Email of the user who created the provisioning. Useful for tagging resources.

```hcl
variable "owner" {
  type = string
}
```

## Outputs

Terraform outputs are captured after `apply` and stored on the provisioning
record. They are accessible via `roachprod prov outputs <id>` and are used
by hooks to resolve machine targets (via jq expressions on the output map).

```hcl
output "instances" {
  value = [
    {
      public_ip  = google_compute_instance.worker.network_interface[0].access_config[0].nat_ip
      private_ip = google_compute_instance.worker.network_interface[0].network_ip
    }
  ]
}
```

The `ssh.machines` jq expression in `template.yaml` operates on this output
structure. For the output above, `.instances[].public_ip` would extract the
list of public IPs.

## Shared Modules

Templates can reference shared modules using symlinks. Place reusable modules
in a `modules/` directory at the templates root (no `template.yaml` = not
deployable as standalone).

```
templates/
  modules/
    aws-vpc/
      main.tf
      variables.tf
      outputs.tf
  aurora-ec2/
    template.yaml
    main.tf
    modules/
      aws-vpc -> ../../modules/aws-vpc    # symlink
```

In your template's `.tf` files:

```hcl
module "vpc" {
  source = "./modules/aws-vpc"
  # ...
}
```

**Important:** Symlinks are resolved at snapshot time. The archived template
contains copies of the actual module files, so the snapshot is self-contained.

The system validates that all relative module `source` paths resolve to
existing directories. Broken symlinks produce warnings during template
discovery.

## State Backend

The provisioning system manages the OpenTofu state backend automatically.
**Do not** include a `backend` block in your template.

The system generates a `backend.tf` file in the working directory before
running `tofu init`. Two backends are supported:

- **GCS** (production): State stored in a GCS bucket under a per-provisioning
  prefix (`provisioning-{uuid}`). Configured via `GCS_STATE_BUCKET`.
- **Local** (development): State stored in the working directory. Used when
  no GCS bucket is configured.

State is cleaned up when a provisioning record is deleted (`prov delete`).

## Snapshot and Versioning

When a provisioning is created, the template directory is archived as a tar.gz
snapshot and stored with the provisioning record. This snapshot is used for
all subsequent operations (plan, apply, destroy), ensuring that:

- Template changes after creation don't affect running provisionings
- Destroy always uses the exact template that created the infrastructure
- Provisionings are reproducible even if the templates directory changes

The snapshot includes:
- All `.tf` files
- `template.yaml` / `template.yml`
- Resolved symlink targets (modules)
- A SHA256 checksum for integrity verification

Maximum snapshot size: 10 MB (typical templates compress to < 100 KB).

## Example: GCE Instance

A minimal template that creates a GCE compute instance:

**`template.yml`:**
```yaml
name: gce-instance
description: A GCE instance.
```

**`vars.tf`:**
```hcl
# Auto-injected variables
variable "identifier" {
  type = string
}

variable "prov_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "owner" {
  type = string
}

# Environment variables (from environment config)
variable "gcp_project" {
  type = string
}

# User-provided variables
variable "instance_config" {
  description = "Configuration for the GCE instance"
  type = object({
    zone          = optional(string, "us-central1-a")
    instance_type = optional(string, "n2-standard-4")
    disk_size_gb  = optional(number, 512)
    user          = optional(string, "testeng-worker")
  })
  default = {}
}
```

**`main.tf`:**
```hcl
resource "google_compute_instance" "worker" {
  project      = var.gcp_project
  name         = var.prov_name
  machine_type = var.instance_config.instance_type
  zone         = var.instance_config.zone

  boot_disk {
    initialize_params {
      size = var.instance_config.disk_size_gb
    }
  }

  labels = {
    owner       = replace(var.owner, "@", "_at_")
    environment = var.environment
  }
}
```

**`output.tf`:**
```hcl
output "instances" {
  value = [{
    public_ip = google_compute_instance.worker.network_interface[0].access_config[0].nat_ip
  }]
}
```

## Checklist

Before submitting a new template:

- [ ] `template.yaml` has a non-empty `name` field
- [ ] No `backend` block in any `.tf` file
- [ ] `variable "identifier"` is declared (always injected)
- [ ] All required variables have descriptions
- [ ] Sensitive variables have `sensitive = true`
- [ ] Outputs are structured for hook consumption (if hooks are used)
- [ ] Module symlinks point to valid targets
- [ ] `default_lifetime` is set if different from global default (12h)
- [ ] Resources are tagged with `owner` and `environment` for traceability
- [ ] Template name matches directory name (convention, not enforced)
