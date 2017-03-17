- Feature Name: Settings Table
- Status: in-progress
- Start Date: 2017-03-17
- Authors: David Taylor
- RFC PR: #14230
- Cockroach Issue:

# Summary

A system table of named settings with a caching accessor on each node to provide
runtime-alterable execution parameters.

# Motivation

We have a variety of knobs and flags that we currently can set at node startup,
via env vars or flag. Some of these make sense to be able to tune at runtime,
without requiring updating a startup script or service definition and subsequent
full reboot of the cluster.

Some current examples, drawn from a cursory glance at our `envutil` calls, that
might be nice to be able to alter at runtime, without rebooting:
  * `COCKROACH_SCAN_INTERVAL`
  * `COCKROACH_REBALANCE_THRESHOLD`
  * `COCKROACH_LEASE_REBALANCING_AGGRESSIVENESS`
  * `COCKROACH_CONSISTENCY_CHECK_INTERVAL`
  * `COCKROACH_MEMORY_ALLOCATION_CHUNK_SIZE`
  * `COCKROACH_NOTEWORTHY_SESSION_MEMORY_USAGE`
  * `COCKROACH_DISABLE_SQL_EVENT_LOG`
  * `COCKROACH_TRACE_SQL`

Obviously not all settings can be, or will even want to be, easily changed
at runtime, at potentially at different times on different nodes due to caching,
so this would not be a drop-in replacement for all current flags and env vars.
For example, some settings passed to RocksDB at startup or those affecting
replication and internode interactions might be less suited to this pattern.

# Detailed design

A new system.settings table, keyed by string settings names would be created.

```
CREATE TABLE system.settings (
  name STRING PRIMARY KEY,
  value STRING,
  updated TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
  valueType char NOT NULL DEFAULT 's',
)
```

The table would be created in the system config range and thus be gossiped. On
gossip update, a node would iterate over the settings table to update its
in-memory map of all current settings.

A collection of typed accessors fetch named settings from said map and marshal
their value in to a bool, string, float, etc.

Thus retrieving a setting from the cache _does not_ have any dependencies on a
`Txn`, `DB` or other any other infrastructure -- since the map is updated
asynchronously by a loop on `Server` -- making it suitable for usage at a broad
range of callsites (much like our current env vars).

While (thread-safe) map access should be relatively cheap and suitable for many
callsites, particularly performance-sensitive callsites may instead wish to use
an accessor that registers a variable to be updated atomically on cache refresh,
after which they can simply read the viable via one of the sync.atomic
functions.

Only user-set values need actually appear in the table, as all accessors provide
a default value to return if the setting is not present.

## Centralized Definition

A central list of defined settings with their type and default value provides
the ability to:
* list all known-settings
* validate access a given setting uses the appropriately typed accessor
* validate writes to a setting are of the appropriate type

The main drawback to a central registry is that adding any setting requires
altering the settings package, adding friction and likely transitively
invalidating a large portion of the package tree.

Given that Go compilation is relatively fast, it seems the validation advantages
of keeping a central registry likely outweigh the drawbacks.

## Modifying Settings

The `SET` statement will optionally take a `GLOBAL SETTING` modifier to specify
changes to a global setting, e.g.
  `SET GLOBAL SETTING 'storage.rebalance_threshold' = 0.5`

The `settings` table will not have write privileges at the SQL layer, but will
instead be read-only, and only readable by the root user, thus forcing the use
of the `SET` statement, ensuring validation and allowing for changes to be in a
`settings_history` table (e.g. with a`(name,time)` key and the old value).

# Alternatives

## Per-row TTLs

Rather than letting nodes cache the entire table, individual rows could instead
have more granular, row-specific TTLs. Accessors would attempt to fetch and
cache values not currently cached. This would potentially eliminate
false-negatives immediately after a setting is added and allow much more
granular control, but at the cost of introducing a potential KV read. The added
calling infrastructure (a client.DB or Txn, context, etc), combined with the
unpredictable performance, would make such a configuration provider suitable for
a much smaller set of callsites.

## Eagerly written defaults

If we wrote all settings at initialization along with their default values, it
would make inspecting the in-use values of all settings, default or not,
straightforward, i.e `select * from system.settings`.

Doing so however makes updating defaults much harder -- we'd need to handle the
migration process while taking care to avoid clobbering any expressed settings.

Obviously eagerly written defaults could be marked on user-changes and we could
add migrations when adding and changing them, but this adds to the engineering
overhead of adding and using these settings. Additionally, we can still get the
easy listing of in-use values, if/when we want it, by keeping a central list of
all settings and their defaults.
