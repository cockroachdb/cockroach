# Configuration UX Guidelines

Note: these are a work in progress; check with #technical-leads-council for questions/clarification.

# Cluster settings

## Appropriateness

**Guideline:** Consider appropriateness of a cluster setting versus some other configuration mechanism.

* A behavior specific to a node, such as compaction rates or threads or memory limits is not well suited to a cluster setting.

  + A CLI flag, env var, or some other form of persisted configuration may be a better fit.
* A behavior where developers or operators may require it to differ by table, application or user is not well suited to a cluster setting.

  + A session variable, role option, table storage parameter, span config or other persisted attribute may be a better fit.
* A behavior that needs to be configurable by developers working on a single application, who are not cluster administrators, is generally not a good fit for a cluster setting.

## Organization and naming for cluster settings

**Guideline:** A name is composed of three main parts, the middle of which could have sub-parts, joined by dots:

* 1) a broad, high level area prefix such as `sql.` or `kv.`
* 2) a component, feature or behavior name within that area (possibly with multiple sub-parts), and then
* 3) a suffix identifying the aspect of that behavior being configured.

Example: `sql.catalog.descriptor_lease_renewal.cross_validation.enabled`

* Identifies the `descriptor_lease_renewal.cross_validation` behavior within the `catalog` component in the `sql` area, and configures the `enabled` aspect of it.

**Guideline:** Always use a separate suffix to identify the aspect of a behavior being configured, even when it is the only aspect being configured.

* Thus a boolean should nearly always have the suffix `.enabled` as it is configuring the enabled aspect of some behavior.
* This leaves room for other aspects, such as frequency or strictness, to be added alongside it in the future.

**Guideline:** Use only ASCII lower-case letters and numbers, avoiding any special characters or punctuation other than dot to separate parts of the name and underscore to separate words within a part.

This ensures that settings names can appear as "bare" unquoted identifiers in our SQL grammar, e.g `SET … a.b_c.d = x`

## Default values

**Guideline:** Review and adjust defaults to be appropriate out of the box.

The more settings each cluster sets, the harder it is to support them, as their behaviors become increasingly unique and dependent on who set them up, what doc or guide they followed, on what version, etc which can complicate subsequent operation or support. If you see docs or customers or field teams setting a particular setting often, stop and ask why, then see if its default can be adjusted, or the behavior reworked with additional smarts/adaptiveness, to avoid the need to be setting it manually.

A setting set via automation should smell like a bug most of the time (except in CC, where we’re OK with custom defaults).

A setting can use a sentinel such as zero or ““ for its default then document that this value causes the some special case or dynamic behavior instead, for example “0 = GOPAXPROCs” or “0 = no limit”. However the definition of the default itself should use a constant, not derived from an env var, flag or runtime/compiler value that could differ between nodes (though making the constant metamorphic for testing is allowed and encouraged).

## Visibility

**Guideline:** Do not make a setting “public” unless:

* a) it *needs* to be adjusted self-service by operators AND
* b) you have put in place appropriate guardrails to minimize risk of causing performance or reliability problems AND
* c) documentation around its correct use has been written.

It is okay to add a setting without doing the above so long as it remains non-public

* Misadjusting non-public settings may risk availability and reliability
* Non-public settings should only be adjusted with the guidance of engineering

  + Such guidance could be either in the form of live ad-hoc guidance or engineer reviewed internal docs or runbooks

**Guideline:** Tread carefully around unsafe configuration. Use “unsafe” in the name AND description / help texts.

This applies to settings that are known to have potential for lead to data loss or corruption.

* Potentially avoid a cluster setting in favor of an environment variable if used only for testing/debugging

  + Needing to start the cluster in a specific configuration makes it more clear it is for the express purpose of a test.
* If a cluster setting is the answer, its name should include `unsafe`, `experimental` or some similar descriptive word in its name. This may be revisited it in future.

  + Such a setting generally should not be made public until guardrails to eliminate those risks are in place.
  + "Unsafe" or similar scary name should be kept *even if non-public* when potential for data loss is known.

# CLI configuration for server commands

In server commands (`cockroach start`), we use a combination of CLI flags and environment variables for knobs that either:

* can be different on different nodes (e.g. max memory available)
* needs to apply before a cluster starts up (e.g. join flags, logging config, security parameters)

There are two general categories described in the following sub-sections.

## User-visible CLI configuration

We generally prefer CLI command-line flags for user-visible configuration.

User-visible CLI configuration always applies according to a common schema:

* in any case, the user can override using a CLI flag on the command-line (`--my-arg` or `--my-arg=value`)
* for very few flags, if the user has not passed a value explicitly, we also provide an env var that can also be used to provide a custom value (lower priority than the explicit CLI flag), for example `COCKROACH_SOCKET_DIR` for `--socket-dir`.
* if no value was provided by the user, a default is applied in code.

**Guideline:** Ensure any addition or change to user-visible CLI configuration is documented in release notes and has a documentation follow-up project.

**Guideline**: Use descriptive names for CLI flags that **pertain to the mechanism, not the use case**. For example, we use the flag name `--clock-device` to make CockroachDB work with VMWare PTP clocks, not `--vmware-ptp-device`, because the mechanism is more generic than the use case.

**Guideline:** Don’t define CLI flags such that the user must pass PII or secrets as value: CLI flags can be inspected from other unprivileged processes on the same machine. In those use cases that require it, make the CLI flag point to a file path and load the PII/secret from there.

**Guideline**: Use env var aliases for user-visible CLI *server* flags extremely sparingly. Currently only 4 CLI flags have env var aliases, mostly for historical purposes. We should shy away from using env vars for CLI *server* configuration.

**Guideline**: Tread carefully about CockroachDB version upgrades.

A user may have built automation that embeds specific CLI flags and env vars. During an upgrade, they will use the same automation to run both previous and new version nodes. Therefore:

* any change OR removal of a CLI flag must follow a deprecation cycle that announces the deprecation/removal at least one version in advance.
* any change to an env var must also follow a deprecation cycle (but removals are OK - the env var will simply be ignored).

**Guideline:** Tread carefully around unsafe configuration. Use “unsafe” in the name AND description / help texts.

* Possibly mark the flag as “Hidden”.
* Consider not using a CLI flag at all for unsafe configuration. See the next section instead

**Guideline:** If the same aspect of a behavior must be controllable by both a cluster setting and a per-node flag/env var, the flag/env var should override the cluster setting.

* Having both is confusing and should be avoided if possible, however in cases where both exist…
* Flags and env vars as per-node, which means they are more specific and should thus override cluster-wide settings.

  + Example: a cluster setting might set the `storage.max_sync_duration` to 5s on a cluster where that is based on the most common disk configuration, while some nodes in that cluster might have different disks and/or other applications specific to those nodes sharing those specific disks and thus need to specify a different value.

## Ad-hoc back-end configuration overrides

We use environment variables for server configuration that is ad-hoc to specific deployments, that is, where the specificity is such that relatively very few users will ever need to change it. This includes:

* changes to internal node behavior that should ever be changed under instruction by a CRL employee (e.g. `COCKROACH_RAFT_ENTRY_CACHE_SIZE`);
* one-off configurations to use in emergency situations (e.g. `COCKROACH_AUTO_BALLAST`)

Like other CLI config for server commands, env vars are only used for behavior that can be different on different nodes, or need to apply before a cluster is fully initialized.

**Guideline**: Ensure that the definition of env vars in code has detailed documentation next to it that explains its impact.

**Guideline:** Don’t define env vars such that the user must pass PII or secrets as value: env vars can be inspected from other unprivileged processes on the same machine. In those use cases that require it, make the env var point to a file path and load the PII/secret from there.

# CLI configuration for client commands

In client commands (e.g. `cockroach node`, `cockroach sql`) we primarily use CLI flags for all configuration: both documented, user-visible and internal, ad-hoc configuration.

* The guidelines from the section above about CLI flags for server commands apply here, with regards to documentation and release notes, naming, PII/secrets, version upgrades and unsafe configuration.
* Additional guidelines include:

  + Experimental or internal configuration can be marked as “hidden” so they don’t show up in docs and help texts (e.g. `--experimental-dns-srv`).
  + We can also hide configuration flags that provide redundant configurability for convenience (for example `--port` is hidden because the port number can be included in `--host` or `--url`).

As a main difference from server commands, we do **provide slightly more env var aliases for CLI client configurations** that are expected to be configured the same across many invocations of client commands, and across multiple client commands. For example: `COCKROACH_HOST`, `COCKROACH_PORT`, `COCKROACH_URL`.

# Review

TBD
