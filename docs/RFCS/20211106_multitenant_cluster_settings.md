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

   Example: `kv.closed_timestamp.follower_reads_enabled`.

3. Tenant read-only

   These settings are per-tenant, meaning that each tenant has its own separate
   value. However, the tenant cannot modify them; only the system tenant can,
   via a new statement:
   ```
     SET TENANT <tenant> CLUSTER SETTING <setting> = <value>
   ```
   We will also add a `SHOW [ALL] TENANT <tenant> CLUSTER SETTINGS` statement.

   Example: `tenant_cpu_usage_allowance`.

4. Tenant

   These settings are per tenant and can be modified by the tenant (as well as
   the system tenant as above).

   Example: `sql.notices.enabled`.

A note on the "threat model": the described restrictions assume that the SQL
tenant process is not compromised. There is no way to prevent a compromised
process from changing its own view of the cluster settings. However, even a
compromised process should never be able to learn the values for the
`System hidden` settings.

## Implementation notes

All functions used to register cluster settings will take an extra argument with
the class of the setting. We want to make an explicit (and reviewable) decision
for each existing cluster setting, and we want the authors of future settings to
be forced to think about the class.

Class-specific notes:

1. System hidden
   
   There is not much work to do here, other than fully hiding these settings
   from non-system tenants. The cluster settings subsystem will not allow
   accessing these values from a tenant process (it will crash the tenant
   process).

   These settings live only in the system tenant's instance of the
   `system.settings` table.

2. System visible

   This class involves a bit of new infrastructure. We modify the tenant
   KV connector APIs to allow "listening" for updates to cluster settings. We
   already have a `SystemConfigProvider` / gossip subscription API with similar
   functionality; we can extend that or use it as a precedent for a similar
   subsystem.

   Values for these settings do not exist in tenant instances of the
   `system.settings` table. They only live in the system tenant's instance, with
   tenants discovering the values through the KV connector.

3. Tenant read-only

   This class only requires adding checks to disallow changing values. We will
   also disallow writing directly to the `system.settings` table; only internal
   SQL code will be allowed to write to this table.
   
   Values for tenant settings live in each tenant's instance of the
   `system.settings` table.

   TODO: relying on effectively restricting parts of the tenant keyspace is not
   very robust. Eg what if you add the relevant KV to a tenant backup and
   restore it?

   An alternative approach previously suggested in #68406 would be to store
   these settings on the host side (for all tenants, in a new system table).
   However, this would be considerably more work.

4. Tenant

   This class is consistent with the current semantics (it's already
   implemented).

### Guidelines

When deciding which class is appropriate for a given setting, we will use the
following guidelines:

 - if the setting controls a user-visible aspect of SQL, it should be a `Tenant`
   setting.

 - control settings relevant to tenant-specific internal implementation (like
   tenant throttling) that we want to be able to control per-tenant should be
   `Tenant read-only`.

 - when in doubt the first choice to consider should be `System visible`. Note
   that if we need to change the class at a later point in time, it's easier to
   switch from system classes to tenant classes than vice-versa.

 - `System hidden` should be used with caution - it will cause crashes in tenant
   processes if the setting actually needs to be consulted.


## Rationale and Alternatives

An alternative approach to implement `System visible` is to use the same
infrastructure as for `Tenant read-only` settings. This has the advantage of not
having to implement any new infrastructure, but it would complicate things on
the host side - when changing any such setting, we would have to change the
corresponding setting in each tenant.

# Explain it to folk outside of your team

Audience: PMs, doc writers, end-users, CockroachDB team members in other areas of the project.

# Unresolved questions
