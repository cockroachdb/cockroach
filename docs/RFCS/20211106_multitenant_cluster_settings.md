- Feature Name: Multi-tenant cluster settings
- Status: accepted
- Start Date: 2021-11-06
- Authors: Radu Berinde
- RFC PR: #73349
- Cockroach Issue: #77935

# Summary

This RFC introduces an update to our cluster settings infrastructure aimed at
solving shortcomings in multi-tenant environments. We introduce different
*classes* of cluster settings, each with its own semantics.

# Motivation

Cluster settings are used to control various aspects of CockroachDB. Some of
them apply exclusively to the KV subsystem; some apply only to the SQL layer.
Yet others are harder to classify - for example, they may apply to an aspect of
the KV subsystem, but the SQL layer also needs to interact with the setting.

Currently all cluster settings are treated homogeneously; their current values
are stored in the `system.settings` table.

In a multi-tenant deployment, the KV and SQL layers are separated. KV is handled
by a single shared host cluster; in contrast, each tenant runs its own separate
instance of the SQL layer, across multiple SQL pods (that form the tenant
"cluster").

Currently each tenant has its own separate instance of all cluster settings (and
its `system.settings` table). Some settings are designated as `SystemOnly` to
indicate that they are only applicable to the system tenant (these settings are
not expected to be consulted by the tenant code). Tenants can freely change all
other settings, but only those that affect the SQL code run by the tenant will
make any difference.

Beyond the obvious usability issues, there are important functional gaps:

 - we need settings that can be read by the tenant process but which cannot be
   modified by the end-user. For example: controls for the RU accounting
   subsystem.

 - in certain cases tenant code may need to consult values for cluster settings
   that apply to the host cluster: for example
   `kv.closed_timestamp.follower_reads.enabled` applies to the KV subsystem but
   is read by the SQL code when serving queries.

### Note on SQL settings

Many SQL features are controlled using a session setting / cluster setting pair.
The cluster setting is of the form `sql.defaults.*` and contains the default
value for the session setting. In a separate project, these cluster settings are
being deprecated in favor of database/role defaults (ALTER ROLE statement). This
doesn't affect the present proposal, other than to note that there will be much
fewer cluster settings that need to be controlled by the tenant.

# Technical design

We propose splitting the cluster settings into three *classes*:

1. System only (`system`)

   Settings associated with the host cluster, only usable by the system tenant.
   These settings are not visible at all from other tenants. Settings code
   prevents use of values for these settings from a tenant process.

   Example: `kv.allocator.qps_rebalance_threshold`.
  
2. Tenant read-only (`tenant-ro`)

   These settings are visible from non-system tenants but the tenants cannot
   modify the values.

   New SQL syntax allows the system tenant to set the value for a specific
   tenant; this results in the tenant (asynchronously) getting the updated
   value. The system tenant can also set an "all tenants" default value with a
   single command; the value applies to all tenants that don't have their own
   specific value, including future tenants.

   Examples: `kv.bulk_ingest.batch_size`, `tenant_cost_control.cpu_usage_allowance`.

3. Tenant writable (`tenant-rw`)

   These settings are per tenant and can be modified by the tenant (as well as
   the system tenant as above). The system tenant can override these settings
   with the same syntax as above (effectively converting specific settings into
   `tenant-ro`).

   Example: `sql.notices.enabled`.

#### A note on the threat model

The described restrictions assume that the SQL tenant process is not
compromised. There is no way to prevent a compromised process from changing its
own view of the cluster settings. However, even a compromised process should
never be able to learn the values for the `System` settings. It's also worth
considering how a compromised tenant process can influence future uncompromised
processes.

### SQL changes

New statements for the system tenant only (which concern only `tenant-ro` and
`tenant-rw` settings):
 - `ALTER TENANT <id> SET CLUSTER SETTING <setting> = <value>`
   - Sets the value seen by tenant. For `tenant-rw`, this value will override
     any setting from the tenant's side, until the cluster setting is reset.

 - `ALTER TENANT ALL SET CLUSTER SETTING <setting> = <value>`
   - Sets the value seen by all non-system tenants, except those that have a
     specific value for that tenant (set with `ALTER TENANT <id>`). For
     `tenant-rw`, this value will override any setting from the tenant's side,
     until the cluster setting is reset.
     Note that this statement does not affect the system tenant settings.

 - `ALTER TENANT <id> RESET CLUSTER SETTING <setting>`
   - Resets the tenant setting. For `tenant-ro`, the value reverts to the `ALL`
     value (if it is set), otherwise to the setting default. For `tenant-rw`,
     the value reverts to the `ALL` value (if it is set), otherwise to whatever
     value was set by the tenant (if it is set), otherwise the setting default.

 - `ALTER TENANT ALL RESET CLUSTER SETTING <setting>`
   - Resets the all-tenants setting. For tenants that have a specific value set
     for that tenant (using `ALTER TENANT <id>`), there is no change. For other
     tenants, `tenant-ro` values revert to the setting default and `tenant-rw`
     values revert to whatever value was set by the tenant (if it is set),
     otherwise the setting default.

 - `SHOW CLUSTER SETTING <setting> FOR TENANT <id>`
   - Display the setting override. If there is no override, the statement
     returns NULL. (We choose to not make this statement 'peek' into
     the tenant to display the customization set by the tenant itself.)

 - `SHOW [ALL] CLUSTER SETTINGS FOR TENANT <id>`
   - Display the setting overrides for the given tenant. If there is no
     override, the statement returns NULL.

In all statements above, using `id=1` (the system tenant's ID) is not valid.

New semantics for existing statements for tenants:
 - `SHOW [ALL] CLUSTER SETTINGS` shows the `tenant-ro` and `tenant-rw` settings.
   `Tenant-ro` settings that have an override from the host side are marked as
   such in the description.

 - `SET/RESET CLUSTER SETTING` can only be used with `tenant-rw` settings.  For
   settings that have overrides from the host side, the statement will fail
   explaining that the setting can only be changed once the host side resets the
   override.

## Implementation

The proposed implementation is as follows:

 - We update the semantics of the existing `system.settings` table:
    - on the system tenant, this table continues to store values for all
      settings (for the system tenant only)
    - on other tenants, this table stores only values for `tenant-rw` settings.
      Any table rows for other types of variables are ignored (in the case that
      the tenant manually inserts data into the table).

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
   This table is only used on the system tenant. All-non-system-tenant values
   are stored in `tenant_id=0`. This table contains no settings for the system
   tenant (`tenant_id=1`), and the `tenant_id=0` values do not apply to the
   system tenant.

 - We modify the tenant connector APIs to allow "listening" for updates to
   cluster settings. Inside the tenant connector this can be implemented using a
   streaming RPC (similar to `GossipSubscription`).

 - On the system tenant we set up rangefeed on `system.tenant_settings` and keep
   all the changed settings (for all tenants) in memory. We expect that in
   practice overrides for specific tenants are rare (with most being "all
   tenant" overrides). The rangefeed is used to implement the API used by the
   tenant connector to keep tenants up to date. We continue to set up the
   rangefeed on the `system.settings` table to maintain the system tenant
   settings.

 - On non-system tenants we continue to set up the rangefeed on the tenant's
   `system.settings` table, and we also use the new connector API to listen to
   updates from the host. Values from the host which are present always override
   any local values.


### Upgrade

The proposed solution has very few concerns around upgrade. There will be a
migration to create the new system table, and the new connector API
implementation is only active on the new version (in a mixed-version cluster, it
can error out or use a stub no-op implementation). The new statements (around
setting per-tenant values) should also error out until the new version is
active.

The settings on the system tenant will continue to work. On non-system tenants,
any locally changed settings that are now `system` or `tenant-rw` will revert to
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
   `tenant-rw` setting.

 - control settings relevant to tenant-specific internal implementation (like
   tenant throttling) that we want to be able to control per-tenant should be
   `tenant-ro`.

 - when in doubt the first choice to consider should be `tenant-rw`.

 - `system` should be used with caution - we have to be sure that there is no
   internal code running on the tenant that needs to consult them.

We fully hide `system` settings from non-system tenants. The cluster settings
subsystem will not allow accessing these values from a tenant process (it will
crash the tenant process, at least in testing builds, to find any internal code
that incorrectly relies on them). The values of these settings are unknown to
the tenant APIs for changing tenant settings (i.e. if a tenant attempts to read
or set such a setting, it will get the "unknown cluster setting" error).


## Alternatives

There are three possibilities in terms of the system table changes:

 - a) Add a new `system.tenant_settings` table (as described above).
    - Pro: clean schema, easier to reason about.
    - Pro: no schema changes on the existing system table.

 - b) Use the existing `system.settings` table as is. For tenant-specific
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

A previous proposal was to store `tenant-ro` values in each tenant's
`system.settings` table and disallowing arbitrary writes to that table. While
this would be less work in the short-term, it will give us ongoing headaches
because it breaks the tenant keyspace abstraction. For example, restoring a
backup will be problematic.

Another proposal was to store all tenant settings on the host side and allow the
tenant to update them via the tenant connector. This is problematic for a number
of reasons, including transactionality of setting changes and opportunities for
abuse.

A previous version of the proposal included a "system visible" (or "shared
read-only") class, for system settings that the tenants can read. However, given
the support for all-tenant values for `tenant-ro`, the functional differences
between these two classes becomes very small.
