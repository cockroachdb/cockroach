- Feature Name: Configs for structured data
- Status: completed
- Start Date: 2015-08-19
- RFC PR: [#2183](https://github.com/cockroachdb/cockroach/pull/2183)
- Cockroach Issue: [#2090](https://github.com/cockroachdb/cockroach/issues/2090)

# Summary

This RFC describes the application of various configurations to the structured data
representation. We address storage, distribution, and modification.

# Motivation

Structured data will soon be the only publicly accessible API. This requires a change
in the way configurations are specified and applied, moving from prefix-based logic
to databases and tables.

The following configs currently exist:
* Accounting: usage limits (bytes, counts). Not currently used.
* Permissions: user read/write permissions. Used by the KV endpoint.
* Users: password storage for the web interface. Currently settable, but not used.
* Zones: replication configuration. Used to make allocation decisions.

# Prerequisites

Complete implementation of this RFC depends on the following:
* Gossip system database: [#2179](https://github.com/cockroachdb/cockroach/issues/2179)
* Disable KV endpoint: [#2089](https://github.com/cockroachdb/cockroach/issues/2089)
* Usage accounting: [#2185](https://github.com/cockroachdb/cockroach/issues/2185)

# Detailed design

## Storage

We propose moving all configuration files to the `system` database as separate tables.

The table schema depends on the configuration. Some (eg: `users`) apply globally,
while some (eg: `accounting`, `zones`) apply to databases and tables.

For each configuration, we propose the following schemas:

#### Accounting

The accounting table applies to databases and potentially to tables as well.
Its contents have yet to be determined.

The initial use of this table will be usage limits (a.k.a quotas). Actual usage
storage is still TBD, see: [#2185](https://github.com/cockroachdb/cockroach/issues/2185).

#### Permissions

The permissions table is only used for the KV endpoint. It will not be migrated
to the structured data API and will instead be removed after
[#2089](https://github.com/cockroachdb/cockroach/issues/2089) is resolved.

#### Users

The users table is currently the only configuration applicable globally.
It contains a mapping of username to password hash and salt.

Schema:
```SQL
CREATE TABLE system.users (
  "username"  CHAR primary key,
  "hashed"    BLOB
)
```

#### Zones

The zone config applies to tables, with a database-level entry used as default.
When a database is created, it will use a global default.
When a table is created, it will inherit its database zone config.

Schema:
```SQL
CREATE TABLE system.zones (
  "target"           INT primary key,
  "attributes"       CHAR,
  "range_min_bytes"  INT,
  "range_max_bytes"  INT,
  "gc_ttl_seconds"   INT
)
```

The `target` field is the ID of the database or table the config applies to.
We will also want a global default using ID 0 (which is not a valid database or table ID).

## Privileges

The default privileges for the `system` database and all its tables is `GRANT, SELECT` for
the `root` only. `root` must always keep these exact permissions. Other users may be
granted `GRANT, SELECT`, or a subset, but never more.

Since the configurations are mutable, we also allow `INSERT, UPDATE, DELETE` to `root` by
default and allow these privileges to be granted.

The default `root` privileges and maximum grantable privileges now become:

| Table name | Maximum privileges                    |
|------------|---------------------------------------|
| namespace  | GRANT, SELECT                         |
| descriptor | GRANT, SELECT                         |
| accounting | GRANT, SELECT, INSERT, UPDATE, DELETE | 
| users      | GRANT, SELECT, INSERT, UPDATE, DELETE | 
| zones      | GRANT, SELECT, INSERT, UPDATE, DELETE | 

## Setting configuration parameters

The table structure and privilege settings allow modification of the configuration
through sql commands.

For ease of use and bulk modifications, the current command-line commands
should be modified to function with the new format.

# Implementation

Due to the current state of the system, this RFC will implement table-based configuration
in multiple steps:

| Configuration | Implementation date |
|---------------|---------------------|
| accounting    | immediately         |
| permissions   | after [#2089](https://github.com/cockroachdb/cockroach/issues/2089) |
| users         | immediately         |
| zones         | after [#2179](https://github.com/cockroachdb/cockroach/issues/2179) |

# Drawbacks

# Alternatives

Some of the configurations could be stored in the `descriptor` table.
However, we would need to implement column-based privileges to allow modification of the configuration
while preventing any changes to the descriptors.

The specific column names and types for each table are flexible. Specifically, the accounting
configuration contents are unknown.

# Unresolved questions

It may be desirable to move the privilege configuration from the database and table descriptors and
store them in their own table.

The database/table IDs in the config tables are not particularly clear. It would be nice to
dynamically show/use the database/table names, or introduce new `SHOW` statements that would perform
a join on the config tables and the descriptor table.
