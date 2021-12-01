- Feature Name: Multi-tenant cluster settings
- Status: draft
- Start Date: 2021-11-06
- Authors: Radu Berinde
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

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
   `kv.closed_timestamp.follower_reads_enabled` applies to the KV subsystem but
   is read by the SQL code when serving queries.

### Note on SQL settings

Many SQL features are controlled using a session setting / cluster setting pair.
The cluster setting is of the form `sql.defaults.*` and contains the default
value for the session setting. In a separate project, these cluster settings are
being deprecated in favor of database/role defaults (ALTER ROLE statement). This
doesn't affect the present proposal, other than to note that there will be much
fewer cluster settings that need to be controlled by the tenant.

# Technical design

We propose splitting the cluster settings into four *classes*:

1. System hidden

   Settings associated with the host cluster, only usable by the system tenant.
   These settings are not visible at all from other tenants. Settings code
   prevents use of values for these settings from a tenant process.

   Example: `kv.allocator.qps_rebalance_threshold`.
  
2. System visible

   Settings associated with the host cluster, visible from tenant processes. All
   tenants see the same value for these settings. Changing one of these settings
   (from the system tenant) results in all tenants (asynchronously) getting the
   updated value.

   Example: `kv.bulk_ingest.batch_size`.

3. Tenant read-only

   These settings are per-tenant, meaning that each tenant has its own separate
   value. However, the tenant cannot modify them; only the system tenant can,
   via a new statement:
   ```
     SET CLUSTER SETTING <setting> = <value> FOR TENANT <tenant>
   ```

   Example: `tenant_cpu_usage_allowance`.

4. Tenant writable

   These settings are per tenant and can be modified by the tenant (as well as
   the system tenant as above). The system tenant can override these settings
   with the same syntax as above (effectively converting specific settings into
   "tenant read-only").

   Example: `sql.notices.enabled`.

#### A note on the threat model

The described restrictions assume that the SQL tenant process is not
compromised. There is no way to prevent a compromised process from changing its
own view of the cluster settings. However, even a compromised process should
never be able to learn the values for the `System hidden` settings. It's also
worth considering how a compromised tenant process can influence future
uncompromised processes.

### SQL changes

New statements for the system tenant only:
 - `SET CLUSTER SETTING <setting> = <value> FOR TENANT <tenant>`
   - The setting is either `Tenant read-only` or `Tenant writable`. For
     `Tenant writable` settings, this value will override any setting from the
     tenant's side, until the cluster setting is reset.

 - `RESET CLUSTER SETTING <setting> FOR TENANT <tenant>`
   - Resets the tenant setting. Tenant read-only revert to the default, and
     tenant writable revert to whatever setting was set on the tenant side.

 - `SHOW CLUSTER SETTINGS <setting> FOR TENANT <tenant>`

 - `SHOW [ALL] CLUSTER SETTINGS FOR TENANT <tenant>`

New semantics for existing statements for tenants:
 - `SHOW [ALL] CLUSTER SETTINGS` shows the `System visible`, `Tenant
   read-only`, and `Tenant writable` settings. Tenant writable settings that
   have an override are marked as such in the description.

 - `SET/RESET CLUSTER SETTING` can only be used with `Tenant writable` settings.
   For settings that have overrides from the host side, the statement will fail
   explaining that the setting can only be changed once the host side resets the
   override.

## Implementation

The proposed implementation is as follows:

 - We update the semantics of the existing `system.settings` table:
    - for the system tenant, this table stores all `System` variables as well as
      `Tenant` variables for the system tenant (similar to what we do now).
    - for other tenants, this table stores only values for `Tenant writable`
      settings. Any table rows for other types of variables are ignored (in the
      case that the tenant manually inserts data into the table).

 - We add a new `system.tenant_settings` (TODO: `tenant_setting_overrides`?)
   table with the schema below. This table is only used on the system tenant and
   it stores any changed values for `Tenant read-only` settings and any
   overrides for `Tenant writable` settings. We can also implement "all tenant"
   overrides using the special `tenant_id` value 0.
```
CREATE TABLE system.tenant_settings (
  tenant_id INT8 NOT NULL,
  name STRING NOT NULL,
  value STRING NOT NULL,
  value_type STRING,
  last_updated TIMESTAMP NOT NULL DEFAULT now(),
  reason STRING,
  PRIMARY KEY (tenant_id, name)
)
```

 - We modify the tenant connector APIs to allow "listening" for updates to
   cluster settings. We already have a `SystemConfigProvider` / gossip
   subscription API with similar functionality; we can extend that or use it as
   a precedent for a similar subsystem. To implement this API, KV nodes set up
   two rangefeeds: one on the `system.settings` table and one on the
   `system.tenant_settings` table.

 - On the tenant side we continue to set up one rangefeed on the tenant's
   `system.settings` table, and we also use the new connector API to listen
   to updates from the host. Values from the host which are present always
   override any local values.

### Notes

All functions used to register cluster settings take an extra argument with the
class of the setting. We want to make an explicit (and reviewable) decision for
each existing cluster setting, and we want the authors of future settings to be
forced to think about the class.

When deciding which class is appropriate for a given setting, we will use the
following guidelines:

 - if the setting controls a user-visible aspect of SQL, it should be a `Tenant
   writable` setting.

 - control settings relevant to tenant-specific internal implementation (like
   tenant throttling) that we want to be able to control per-tenant should be
   `Tenant read-only`.

 - when in doubt the first choice to consider should be `System visible`. Note
   that if we need to change the class at a later point in time, it's easier to
   switch from system classes to tenant classes than vice-versa.

 - `System hidden` should be used with caution - we have to be sure that there
   is no internal code running on the tenant that needs to consult them.

We fully hide `system hidden` settings from non-system tenants. The cluster
settings subsystem will not allow accessing these values from a tenant process
(it will crash the tenant process, at least in testing builds, to find any
internal code that incorrectly relies on them). The values of these settings are
unknown to the tenant APIs for changing tenant settings (i.e. if a tenant
attempts to read or set such a setting, it will get the "unknown cluster setting"
error).


## Alternatives

There are three possibilities in terms of the system table changes:

 - a) Add a new `system.tenant_settings` table (as described above).
    - Pro: clean schema, easier to reason about.
    - Pro: no migrations (schema changes) for the existing system table.
    - Con: requires new system table ID.
    - Con: requires two range feeds.

 - b) Use the existing `system.settings` table as is. For tenant-specific
   settings and overrides, encode the tenant ID in the setting name (which is
   the table PK), for example: `tenant-10/sql.notices.enabled`.
    - Pro: no migrations (schema changes) for the existing system table.
    - Pro: no new system table ID necessary
    - Pro: requires a single range feed.
    - Con: semantics are not as "clean"; goes against the principle of taking
       advantage of SQL schema when possible

 - c) Modify the existing `system.settings` to add a `tenant_id` column, and
   change the PK to `(tenant_id, name)`.
    - Pro: no new system table ID necessary
    - Pro: cleaner schema
    - Pro: requires a single range feed.
    - Con: requires migration (schema change) for the existing system table
      (with the added concern that we have code that parses the raw KVs for this
      table directly).

A previous proposal was to store `Tenant read-only` values in each tenant's
`system.settings` table and disallowing arbitrary writes to that table. While
this would be less work in the short-term, it will give us ongoing headaches
because it breaks the tenant keyspace abstraction. For example, restoring a
backup will be problematic.

Another proposal was to store all tenant settings on the host side and allow the
tenant to update them via the tenant connector. This is problematic for a number
of reasons, including transactionality of setting changes and opportunities for
abuse.
