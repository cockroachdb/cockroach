- Feature Name: Settings Table
- Status: draft
- Start Date: 2017-03-17
- Authors: David Taylor
- RFC PR:
- Cockroach Issue:

# Summary

A system table of named settings with a caching accessor on each node to provide
runtime-alterable execution parameters.

# Motivation

We have a variety of knobs and flags that we currently can set at node startup,
via env vars or flag. Some of these make sense to be able to tune at runtime,
without requiring updating a startup script or service definition and subsequent
full reboot of the cluster.

# Detailed design

A new system.settings table, keyed by string settings names would be created.

// TODO(dt): schema.

Nodes are allowed to cache the table in its entirety for some duration.

On startup a node would fetch the table to its cache and then periodically
refresh its cached copy.

// TODO(dt): does gossip make sense here?

A collection of typed accessors fetch named settings _from the cached copy_ of
the table and marshal their value in to bool, string, float, etc.

Retrieving a setting from the cache _does not_ have any dependencies on a `Txn`,
`DB` or other any other infrastructure -- since the per-node cache is updated
asynchronously by a loop on `Server` -- making it suitable for usage at
a broad range of callsites (much like our current env vars).

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
