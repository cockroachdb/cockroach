- Feature Name: Multi-tenant cluster settings
- Status: completed
- Start Date: 2021-11-06
- Edited: 2023-09-27
- Authors: Radu Berinde, knz, ssd
- RFC PR: [#85970](https://github.com/cockroachdb/cockroach/pull/85970), previously [#73349](https://github.com/cockroachdb/cockroach/pull/73349)
- Cockroach Issue: [#77935](https://github.com/cockroachdb/cockroach/issue/77935), [#85729](https://github.com/cockroachdb/cockroach/issues/85729)

# Summary

This RFC introduces an update to our cluster settings infrastructure aimed at
solving shortcomings in multi-tenant environments. We introduce different
*classes* of cluster settings, each with its own semantics.

# Motivation

Cluster settings are used to control various aspects of CockroachDB. Some of
them apply exclusively to the KV subsystem; some apply only to the SQL layer.
Yet others are harder to classify - for example, they may apply to an aspect of
the KV subsystem, but the SQL layer also needs to interact with the setting.

Currently all cluster settings are treated homogeneously; their
current values are stored in the `system.settings` table of the system
tenant, at the level of the storage cluster.

With cluster virtualization, the KV/storage and SQL layers are
separated. For example, KV is handled by a single shared storage
cluster; in contrast, each virtual cluster runs its own separate
instance of the SQL layer, across multiple SQL pods (that form the
"logical cluster").

As of this writing (2021) each virtual cluster has its own separate
instance of all cluster settings (and its own `system.settings`
table). Some settings are designated as `SystemOnly` to indicate that
they are only applicable to the storage layer (these settings are not
expected to be consulted by virtual cluster servers). Virtual clusters
can freely change all other settings, but only those that affect the
SQL code run inside the virtual cluster will make any difference.

Beyond the obvious usability issues, there are important functional gaps:

 - we need settings that can be read by VC server processes but which cannot be
   modified by the end-user. For example: controls for the RU accounting
   subsystem.

 - in certain cases VC code may need to consult values for cluster settings
   that apply to the storage cluster: for example
   `kv.closed_timestamp.follower_reads.enabled` applies to the KV subsystem but
   is read by the SQL code when serving queries.

# Technical design

We propose splitting the cluster settings into three *classes*:

1. System only (`system-only`)

   Settings associated with the storage layer, only usable by the
   system tenant. These settings are not visible at all from virtual
   clusters. Settings code prevents use of values for these settings
   from a VC server process.

   Example: `kv.allocator.qps_rebalance_threshold`.

2. System visible `system-visible` (previously: "Tenant read-only `system-visible`")

   These settings are visible from virtual clusters but the virtual
   clusters cannot modify the values.

   The observed value of settings in this class is:

   - by default, the value held for the setting in the system tenant
     (the storage cluster's value in the system tenant's
     `system.settings`).
   - New SQL syntax allows the system tenant to set the value for a
     specific tenant; this results in the tenant (asynchronously)
     getting the updated value. (i.e. the value for one tenant can be
     overridden away from the default)

   Examples:

   - Settings that affect the KV replication layer, should not be
     writable by tenants, but which benefit the SQL layer in tenants:
     `kv.raft.command.max_size`.

   - Settings that benefit from being overridden per tenant, but where
     inheriting the system tenant's value when not overridden is OK:
     `kv.bulk_ingest.batch_size`, `tenant_cpu_usage_allowance`.

3. Application level (`application`, previously: "Tenant writable `tenant-rw`)

   These settings are contained by each virtual cluster and can be
   modified by the virtual cluster. They can also be overridden from
   the system tenant using the same override mechanism as above.

   Example: `sql.notices.enabled`.

The difference between the three classes, and with/without override, is as follows:

| Behavior                                                                       | System only                                                                            | System visible                                                                        | Application writable                                           |
|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|----------------------------------------------------------------|
| Value lookup order                                                             | N/A on virtual clusters; in system tenant, 1) local `settings` 2) compile-time default | 1) per-VC override 2) system tenant `settings` 3) compile-time default                | 1) per-VC override 2) local `settings` 2) compile-time default |
| Can run (RE)SET CLUSTER SETTING in system tenant                               | yes                                                                                    | yes                                                                                   | yes                                                            |
| Can run (RE)SET CLUSTER SETTING in virtual cluster                             | no                                                                                     | no                                                                                    | yes                                                            |
| Can set virtual cluster override from system tenant                            | no                                                                                     | yes                                                                                   | yes                                                            |
| Value in current VC's `system.settings` is used as configuration               | only in system tenant                                                                  | no (local value always ignored)                                                       | yes, but only if there's no override                           |
| Default value when the current VC's `system.settings` does not contain a value | compile-time default                                                                   | per-VC override if any, otherwise system tenant value, otherwise compile-time default | per-VC override if any, otherwise compile-time default         |

In effect, this means there's two ways to create a "read-only" setting
in virtual clusters:

- using a "System visible" setting. In that case, the value is taken
  from the system tenant and *shared across all tenants*.
- using an "Application writable" setting, and adding an override in
  the system tenant. In that case, the value is taken from the
  override and *can be specialized per virtual cluster*.

When should one choose one over the other? The determination should be
done based on whether the configuration is system-wide or can be
meaningfully different per virtual cluster.

#### A note on the threat model

The described restrictions assume that each virtual cluster server
process is not compromised. There is no way to prevent a compromised
process from changing its own view of the cluster settings. However,
even a compromised process should never be able to learn the values
for the "System only" settings or modify settings for other virtual
cluster. It's also worth considering how a compromised VC server
process can influence future uncompromised processes.

### SQL changes

New statements for the system tenant only:

 - `ALTER VIRTUAL CLUSTER <id> SET CLUSTER SETTING <setting> = <value>`
   - Sets the value seen by a VC. For `application`, this value will override
     any setting from the VC's side, until the cluster setting is reset.

 - `ALTER VIRTUAL CLUSTER ALL SET CLUSTER SETTING <setting> = <value>`
   - Sets the value seen by all non-system VCs, except those that have a
     specific value for that VC (set with `ALTER VIRTUAL CLUSTER <id>`). For
     `application`, this value will override any setting from the VC's side,
     until the cluster setting is reset.
     Note that this statement does not affect the system tenant's settings.

 - `ALTER VIRTUAL CLUSTER <id> RESET CLUSTER SETTING <setting>`
   - Resets the VC setting override. For `system-visible`, the value reverts to
     the shared value in `system.settings` (if it is set), otherwise
     to the setting default. For `application`, the value reverts to the
     `ALL` value (if it is set), otherwise to whatever value was set
     by the VC (if it is set), otherwise the build-time default.

 - `ALTER VIRTUAL CLUSTER ALL RESET CLUSTER SETTING <setting>`
   - Resets the all-VCs setting override. For VCs that have a specific
     value set for that VC (using `ALTER VIRTUAL CLUSTER <id>`), there is
     no change. For other VCs, `system-visible` values revert to the
     value set in system tenant's `system.settings`, or build-time default if
     there's no customization; and `application` values revert to
     whatever value was set by the VC (if it is set), otherwise
     the build-time default.

 - `SHOW CLUSTER SETTING <setting> FOR VIRTUAL CLUSTER <id>`
   - Display the setting override. If there is no override, the statement
     returns NULL. (We choose to not make this statement 'peek' into
     the VC to display the customization set by the VC itself.)

 - `SHOW [ALL] CLUSTER SETTINGS FOR VIRTUAL CLUSTER <id>`
   - Display the setting overrides for the given VC. If there is no
     override, the statement returns NULL.

In all statements above, using `id=1` (the system tenant's ID) is not valid.

New semantics for existing statements for VCs:
 - `SHOW [ALL] CLUSTER SETTINGS` shows the `system-visible` and `application` settings.
   `system-visible` settings that have an override from the KV side are marked as
   such in the description.

 - `SET/RESET CLUSTER SETTING` can only be used with `application` settings.  For
   settings that have overrides from the KV side, the statement will fail
   explaining that the setting can only be changed once the KV side resets the
   override.

## Implementation

The proposed implementation is as follows:

 - We update the semantics of the existing `system.settings` table:
    - on the system tenant, this table continues to store values for
      all settings (for the system tenant only, and secondary VCs
      for `system-visible` settings)
    - on other VCs, this table stores only values for `application` settings.
      Any table rows for other types of variables are ignored (in the case that
      the VC manually inserts data into the table).

 - We add a new `system.tenant_settings` table with following schema:
   ```
   CREATE TABLE system.tenant_settings (
     tenant_id INT8 NOT NULL,
     name STRING NOT NULL,
     value STRING NOT NULL,
     value_type STRING,
     last_updated TIMESTAMP NOT NULL DEFAULT now() ON UPDATE now(),
     reason STRING,
     PRIMARY KEY (tenant_id, name)
   )
   ```
   This table is only used on the system tenant. All-VC override values
   are stored in `tenant_id=0`. This table contains no settings for the system
   VC (`tenant_id=1`), and the `tenant_id=0` values do not apply to the
   system tenant.

 - We modify the tenant connector APIs to allow "listening" for updates to
   cluster settings. Inside the tenant connector this can be implemented using a
   streaming RPC (similar to `GossipSubscription`).

 - On the system tenant we set up rangefeed on `system.tenant_settings` and keep
   all the changed settings (for all VCs) in memory. We expect that in
   practice overrides for specific VCs are rare (with most being "all
   VC" overrides). The rangefeed is used to implement the API used by the
   tenant connector to keep VCs up to date. We continue to set up the
   rangefeed on the `system.settings` table to maintain the system tenant
   settings.

 - On non-system VCs we continue to set up the rangefeed on the
   VC's `system.settings` table, and we also use the new connector
   API to listen to updates from the storage cluster. Values from the
   storage cluster which are present always override any local values.


### Upgrade

The proposed solution has very few concerns around upgrade. There will be a
migration to create the new system table, and the new connector API
implementation is only active on the new version (in a mixed-version cluster, it
can error out or use a stub no-op implementation). The new statements (around
setting per-VC values) should also error out until the new version is
active.

The settings on the system tenant will continue to work. On non-system VCs,
any locally changed settings that are now `system` or `application` will revert to
their defaults. It will be necessary to set these settings from the system
tenant (using the new statements) if any clusters rely on non-default values.

### Notes

All functions used to register cluster settings take an extra argument with the
class of the setting. We want to make an explicit (and reviewable) decision for
each existing cluster setting, and we want the authors of future settings to be
forced to think about the class.

When deciding which class is appropriate for a given setting, we will use the
following guidelines:

 - if the setting controls a user-visible aspect of SQL, it should be a
   `application` setting.

 - control settings relevant to VC-specific internal implementation (like
   VC throttling) that we want to be able to control per-VC should be
   `system-visible`, or possibly `application` with an override, depending on whether
   we want different overrides for different VCs.

 - when in doubt the first choice to consider should be `application`.

 - `system` should be used with caution - we have to be sure that there is no
   internal code running on the VC that needs to consult them.

We fully hide `system` settings from non-system VCs. The cluster settings
subsystem will not allow accessing these values from a VC process (it will
crash the VC process, at least in testing builds, to find any internal code
that incorrectly relies on them). The values of these settings are unknown to
the VC APIs for changing VC settings (i.e. if a VC attempts to read
or set such a setting, it will get the "unknown cluster setting" error).


## Alternatives

There are three possibilities in terms of the system table changes:

 - a) Add a new `system.tenant_settings` table (as described above).
    - Pro: clean schema, easier to reason about.
    - Pro: no schema changes on the existing system table.

 - b) Use the existing `system.settings` table as is. For VC-specific
   settings and overrides, encode the tenant ID in the setting name (which is
   the table PK), for example: `tenant-10/sql.notices.enabled`.
    - Pro: no migrations (schema changes) for the existing system table.
    - Pro: requires a single range feed.
    - Pro: existing SET CLUSTER SETTING (in system tenant) continues to "just"
           work.
    - Con: semantics are not as "clean"; goes against the principle of taking
           advantage of SQL schema when possible. A CHECK constraint can be used
           to enforce correct encoding.

 - c) Modify the existing `system.settings` to add a `tenant_id` column, and
   change the PK to `(tenant_id, name)`.
    - Pro: clean schema
    - Pro: requires a single range feed.
    - Con: requires migration (schema change) for the existing system table
           (with the added concern that we have code that parses the raw KVs for
           this table directly).

A previous proposal was to store `system-visible` values in each VC's
`system.settings` table and disallowing arbitrary writes to that table. While
this would be less work in the short-term, it will give us ongoing headaches
because it breaks the VC keyspace abstraction. For example, restoring a
backup will be problematic.

Another proposal was to store all VC settings on the storage side and allow the
VC to update them via the tenant connector. This is problematic for a number
of reasons, including transactionality of setting changes and opportunities for
abuse.

A previous version of the proposal included a "system visible" (or "shared
read-only") class, for system settings that the VCs can read. However, given
the support for all-VC values for `system-visible`, the functional differences
between these two classes becomes very small.
