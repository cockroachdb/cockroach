# Hooks Guide

Hooks are post-provisioning actions declared in `template.yaml`. They run after
OpenTofu creates infrastructure and handle lifecycle-aware operations that
roachprod-centralized is uniquely positioned to manage: boot-readiness checks,
SSH key deployment, and one-off setup scripts.

Hooks are NOT a general-purpose configuration management tool. Complex setup
belongs in cloud-init startup scripts. Hooks handle the thin layer between
"infrastructure exists" and "infrastructure is ready to use."

**Related Documentation:**
- [Provisionings Guide](PROVISIONINGS.md) - User-facing provisioning commands
- [Environments Guide](ENVIRONMENTS.md) - Environment variables and secret management
- [Template Authoring Guide](TEMPLATES.md) - Template structure and variables

## Table of Contents

- [Overview](#overview)
- [SSH Configuration](#ssh-configuration)
- [Hook Types](#hook-types)
- [Hook Declaration](#hook-declaration)
- [Triggers and Lifecycle](#triggers-and-lifecycle)
- [Environment Variables in Hooks](#environment-variables-in-hooks)
- [Machine Targeting with jq](#machine-targeting-with-jq)
- [Script Delivery](#script-delivery)
- [Error Handling](#error-handling)
- [Validation](#validation)
- [Examples](#examples)

## Overview

A template declares hooks in `template.yaml`. When infrastructure is
provisioned, the system:

1. Reads the SSH configuration and hook declarations from template metadata
2. Resolves machine IPs from terraform outputs using jq expressions
3. Resolves environment variables (SSH keys, secrets)
4. SSHes into each machine and runs the hook's script
5. Reports success or failure per hook

Two hook types are available:

| Type | Purpose |
|------|---------|
| `run-command` | Execute a bash command on target machines |
| `ssh-keys-setup` | Write engineer SSH public keys to `~/.ssh/authorized_keys` |

## SSH Configuration

Hooks that access machines need SSH configuration. This is declared at the
template level in the `ssh` block:

```yaml
ssh:
  private_key_var: ssh_private_key     # environment variable key
  machines: .instances[].public_ip     # jq expression on terraform outputs
  user: ubuntu                         # SSH username
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| `private_key_var` | Yes | Key name in the resolved environment holding a PEM-encoded SSH private key. This is an environment variable, not a template variable. |
| `machines` | Yes | jq expression evaluated against terraform outputs to extract target IP addresses. Must return strings. |
| `user` | Yes | SSH username for remote connections. |

### SSH Connection Details

The SSH client uses `golang.org/x/crypto/ssh` with:

- **Port:** 22
- **Connect timeout:** 5 seconds
- **Retries:** 3 attempts with exponential backoff (5s, 10s, 20s)
- **Keep-alive:** 60-second interval to prevent idle disconnection
- **Host key verification:** Disabled (`InsecureIgnoreHostKey`). Provisioned
  machines are ephemeral and their host keys are not known in advance.
- **Auth failures** (wrong key, wrong user) are not retried.
- **Transient errors** (connection refused, timeout, EOF) are retried.

### Per-Hook SSH Overrides

Individual hooks can override `machines` and `user` from the template-level
SSH config:

```yaml
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: ubuntu

hooks:
  - name: setup-primary
    type: run-command
    ssh: true
    machines: .instances[0].public_ip    # only first machine
    user: admin                           # different user
    command: /opt/setup-primary.sh
    triggers: [post_apply]
```

## Hook Types

### `run-command`

Executes a bash command on each target machine. The command is delivered via
`bash -s` stdin (no files written to disk on the remote machine).

```yaml
hooks:
  - name: install-deps
    type: run-command
    ssh: true
    command: |
      sudo apt-get update
      sudo apt-get install -y curl jq
    triggers: [post_apply]
```

With retry (useful for boot-readiness checks):

```yaml
hooks:
  - name: wait-ready
    type: run-command
    ssh: true
    command: test -f /.roachprod-initialized
    retry:
      interval: 10s    # wait between attempts
      timeout: 5m      # give up after this duration
    triggers: [post_apply]
```

Without `retry`, the command runs once. A non-zero exit code is a failure.

With `retry`, the command is retried on each machine independently until
success or timeout. Each machine's retry loop is independent: if machine A
succeeds on attempt 2 and machine B needs 10 attempts, both are handled
correctly.

### `ssh-keys-setup`

Fetches engineer SSH public keys from GCP project metadata and writes the full
`~/.ssh/authorized_keys` file on each machine. The file is replaced entirely
(not appended) for idempotency.

```yaml
hooks:
  - name: setup-ssh-keys
    type: ssh-keys-setup
    ssh: true
    triggers: [post_apply, manual]
```

This hook type:
- Reads the `ssh-keys` metadata item from the configured GCP project
- Parses the `user:key_type key_data comment` format
- Filters out reserved users (`root`, `ubuntu`)
- Validates each key with `ssh.ParseAuthorizedKey`
- Sorts keys by username for consistency
- Base64-encodes the result and delivers it via the script pattern
- Creates `~/.ssh` (mode 700) and writes `authorized_keys` (mode 600)

**Server configuration required:** The `ssh-keys-setup` executor is only
registered when `SSHKeysGCPProject` is set in the server config. If not set,
templates declaring this hook type will log a warning and skip execution.

**Manual trigger:** The `manual` trigger allows refreshing keys after initial
provisioning via `roachprod prov setup-ssh <id>`.

## Hook Declaration

Full hook declaration schema:

```yaml
hooks:
  - name: hook-name           # required: identifier for logging
    type: run-command          # required: hook type
    ssh: true                  # required for SSH-dependent types
    machines: .jq.expression   # optional: override ssh.machines
    user: custom-user          # optional: override ssh.user
    command: echo hello        # for run-command type
    env:                       # optional: environment variable mappings
      HOOK_VAR: env_key        #   HOOK_VAR will be set to env_key's value
    optional: false            # optional: if true, failure is a warning
    retry:                     # optional: retry configuration
      interval: 10s
      timeout: 5m
    triggers:                  # required: when to execute
      - post_apply
      - manual
```

### Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | - | Hook identifier used in logs and error messages. |
| `type` | Yes | - | Hook type: `run-command` or `ssh-keys-setup`. |
| `ssh` | Yes* | `false` | Must be `true` for `run-command` and `ssh-keys-setup`. |
| `machines` | No | From `ssh.machines` | jq expression override for this hook's targets. |
| `user` | No | From `ssh.user` | SSH user override for this hook. |
| `command` | For `run-command` | - | Bash command to execute. Supports multiline. |
| `env` | No | `{}` | Maps hook-local variable names to environment keys. |
| `optional` | No | `false` | If `true`, failure logs a warning but doesn't fail the provisioning. |
| `retry` | No | None | Retry config (only meaningful for `run-command`). |
| `triggers` | Yes | - | List of triggers: `post_apply`, `manual`. |

*`ssh: true` is enforced at runtime for `run-command` and `ssh-keys-setup`. A
hook declaring these types without `ssh: true` will fail with a clear error.

## Triggers and Lifecycle

### `post_apply`

Runs automatically after `tofu apply` completes, inside the provisioning task.
Hooks execute in the order declared in `template.yaml`. This trigger runs
inline -- a non-optional hook failure transitions the provisioning to `failed`.

```
tofu init  ->  tofu plan  ->  tofu apply  ->  [hooks: post_apply]  ->  provisioned
                                                      |
                                                      v (if non-optional fails)
                                                    failed
```

### `manual`

Available for on-demand execution via the API or CLI. Currently used by
`ssh-keys-setup` for key refresh: `roachprod prov setup-ssh <id>`.

Manual triggers create a separate task (not inline). The provisioning must be
in `provisioned` state. A concurrency key prevents parallel execution of the
same hook type on the same provisioning.

### `on_key_change`

Defined in the model but not yet wired to any automation. Reserved for future
use (automatic SSH key refresh when GCP metadata changes).

## Environment Variables in Hooks

Hooks can receive environment variables from the resolved environment. The
`env` mapping connects hook-local variable names to environment variable keys:

```yaml
hooks:
  - name: install-certs
    type: run-command
    ssh: true
    command: |
      echo "$CA_CERT" > /etc/certs/ca.crt
      chmod 600 /etc/certs/ca.crt
    env:
      CA_CERT: ca_cert_secret      # CA_CERT will hold the value of ca_cert_secret
    triggers: [post_apply]
```

The environment variable value is base64-encoded and decoded on the remote
machine. This safely handles multiline content and special characters.

Environment variable names are validated against POSIX naming rules
(`[A-Za-z_][A-Za-z0-9_]*`). Invalid names are rejected at script build time.

### Validation at creation time

The system validates at provisioning creation that:
- `ssh.private_key_var` exists in the resolved environment
- All hook `env` mapping values exist in the resolved environment

This catches misconfigurations before any infrastructure is created.

## Machine Targeting with jq

The `machines` field (in `ssh` config or per-hook override) is a jq expression
evaluated against the provisioning's terraform outputs. It must produce a list
of strings (IP addresses).

The expression operates directly on the terraform output map. For outputs:

```json
{
  "instances": [
    {"public_ip": "1.2.3.4", "private_ip": "10.0.0.1"},
    {"public_ip": "5.6.7.8", "private_ip": "10.0.0.2"}
  ]
}
```

Common expressions:

| Expression | Result |
|-----------|--------|
| `.instances[].public_ip` | All public IPs |
| `.instances[0].public_ip` | First instance only |
| `.instances[].private_ip` | All private IPs |

Non-string values are silently skipped. An expression that returns zero strings
produces an error.

## Script Delivery

Hooks deliver scripts to remote machines via `bash -s` stdin. No files are
written to disk on the remote machine. The script structure:

```bash
#!/bin/bash
set -euo pipefail
export VAR1="$(echo 'base64_encoded' | base64 -d)"
export VAR2="$(echo 'base64_encoded' | base64 -d)"
<command>
```

For `ssh-keys-setup`, the authorized_keys content is also base64-encoded:

```bash
#!/bin/bash
set -euo pipefail
mkdir -p ~/.ssh && chmod 700 ~/.ssh
echo 'base64_encoded_keys' | base64 -d > ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

**Concurrency:** Hooks run on up to 10 machines in parallel (bounded by a
semaphore). Errors are collected per-machine and joined into a combined error.

**Context cancellation:** If the task is cancelled, SSH connections are closed
and remote processes are terminated.

## Error Handling

### Optional vs required hooks

```yaml
hooks:
  - name: nice-to-have
    type: run-command
    ssh: true
    optional: true           # failure = warning, provisioning continues
    command: echo "optional"
    triggers: [post_apply]

  - name: must-succeed
    type: run-command
    ssh: true
    optional: false          # failure = provisioning goes to 'failed'
    command: echo "required"
    triggers: [post_apply]
```

- `optional: false` (default): hook failure stops execution and fails the
  provisioning.
- `optional: true`: hook failure is logged as a warning. Execution continues
  to the next hook.

### Missing executor

If a hook type is declared in the template but the corresponding executor is
not registered on the server (e.g., `ssh-keys-setup` without
`SSHKeysGCPProject` configured), the hook is skipped with a warning. This is
not an error -- it indicates the feature is not enabled in this deployment.

### Per-machine errors

When a hook runs on multiple machines and some fail, the error message includes
the failing machine addresses:

```
machine 1.2.3.4: run command on 1.2.3.4:22: Process exited with status 1
machine 5.6.7.8: run command on 5.6.7.8:22: connection refused
```

### Retry behavior

Hooks with `retry` configured retry each machine independently. A machine that
succeeds stops retrying. A machine that exhausts the timeout reports the last
error. If 3 out of 50 machines fail after timeout, only those 3 appear in the
error.

## Validation

The system performs two levels of validation:

### At provisioning creation time

Before any infrastructure is created, `ValidateHookEnvironment` checks:
- `ssh.private_key_var` exists in the resolved environment
- All hook `env` mapping values exist in the resolved environment

Missing variables produce a user-facing error listing all missing references.

### At hook execution time

Before each hook executes, the orchestrator validates:
- SSH-dependent types (`run-command`, `ssh-keys-setup`) have `ssh: true`
- The jq machine expression returns at least one IP
- The SSH private key is present in the environment
- The hook executor is registered (warn and skip if not)

## Examples

### Boot-readiness check + SSH key deployment

```yaml
name: gce-worker
description: GCE instance with SSH key management
default_lifetime: "24h"

ssh:
  private_key_var: ssh_private_key
  machines: .instances[].public_ip
  user: ubuntu

hooks:
  # Wait for cloud-init to finish before doing anything else.
  - name: wait-ready
    type: run-command
    ssh: true
    command: test -f /var/lib/cloud/instance/boot-finished
    optional: false
    retry:
      interval: 10s
      timeout: 5m
    triggers: [post_apply]

  # Deploy engineer SSH keys so the team can access the machine.
  - name: setup-ssh-keys
    type: ssh-keys-setup
    ssh: true
    triggers: [post_apply, manual]
```

### Certificate installation with secrets

```yaml
name: secure-worker
description: Worker with TLS certificates

ssh:
  private_key_var: ssh_private_key
  machines: .instances[].public_ip
  user: admin

hooks:
  - name: install-certs
    type: run-command
    ssh: true
    command: |
      sudo mkdir -p /etc/certs
      echo "$CA_CERT" | sudo tee /etc/certs/ca.crt > /dev/null
      echo "$NODE_CERT" | sudo tee /etc/certs/node.crt > /dev/null
      sudo chmod 600 /etc/certs/*.crt
    env:
      CA_CERT: ca_certificate
      NODE_CERT: node_certificate
    optional: false
    triggers: [post_apply]
```

### Primary-only initialization

```yaml
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: ubuntu

hooks:
  # Run on all machines.
  - name: wait-ready
    type: run-command
    ssh: true
    command: test -f /.initialized
    retry:
      interval: 5s
      timeout: 3m
    triggers: [post_apply]

  # Run only on the first machine.
  - name: init-primary
    type: run-command
    ssh: true
    machines: .instances[0].public_ip    # override: first machine only
    command: /opt/init-cluster.sh
    triggers: [post_apply]
```
