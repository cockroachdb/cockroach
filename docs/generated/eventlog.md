# Documentation for notable events

Certain notable events are reported using a structured format.
Commonly, these notable events are also copied to the table
`system.eventlog`, unless the cluster setting
`server.eventlog.enabled` is unset.

Additionally, notable events are copied to specific external logging
channels, where they can be collected for further processing.

The sections below document the possible notable event types
in this version of CockroachDB. For each event type, a table
documents the possible fields. A field may be omitted from
an event if its value is empty or zero.

A field is also marked as “Sensitive” if it may contain
application-specific information or PII. In that case,
the copy of the event sent to the external logging channel
may contain redaction markers, in a way compatible
with the redaction facilities in `debug zip` or `debug merge-log`.

## Cluster-level events

Events in this category pertain to an entire cluster and are
not relative to any particular tenant.

In a multi-tenant setup, the system.eventlog table for individual
tenants cannot contain a copy of cluster-level events; conversely,
the system.eventlog table in the system tenant cannot contain the
SQL-level events for individual tenants.

Events in this category are logged to channel OPS.


### `node_decommissioned`

An event of type `node_decommissioned` is recorded when a node is marked as
decommissioned.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `RequestingNodeID` | The node ID where the event was originated. | no |
| `TargetNodeID` | The node ID affected by the operation. | no |

### `node_decommissioning`

NodeDecommissioned is recorded when a node is marked as
decommissioning.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `RequestingNodeID` | The node ID where the event was originated. | no |
| `TargetNodeID` | The node ID affected by the operation. | no |

### `node_join`

An event of type `node_join` is recorded when a node joins the cluster.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event was originated. | no |
| `StartedAt` | The time when this node was last started. | no |
| `LastUp` | The approximate last time the node was up before the last restart. | no |

### `node_recommissioned`

An event of type `node_recommissioned` is recorded when a decommissioning node is
recommissioned.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `RequestingNodeID` | The node ID where the event was originated. | no |
| `TargetNodeID` | The node ID affected by the operation. | no |

### `node_restart`

An event of type `node_restart` is recorded when an existing node rejoins the cluster
after being offline.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event was originated. | no |
| `StartedAt` | The time when this node was last started. | no |
| `LastUp` | The approximate last time the node was up before the last restart. | no |

## Miscellaneous SQL events

Events in this category report miscellaneous SQL events.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these miscellaneous events are
preserved in each tenant's own system.eventlog table.

Events in this category are logged to channel DEV.


### `set_cluster_setting`

An event of type `set_cluster_setting` is recorded when a cluster setting is changed.


| Field | Description | Sensitive |
|--|--|--|
| `SettingName` | The name of the affected cluster setting. | no |
| `Value` | The new value of the cluster setting. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

## SQL Logical Schema Changes

Events in this category pertain to DDL (Data Definition Language)
operations performed by SQL statements that modify the SQL logical
schema.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own system.eventlog table.

Events in this category are logged to channel SQL_SCHEMA.


### `alter_database_add_region`

An event of type `alter_database_add_region` is recorded when a region is added to a database.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | yes |
| `RegionName` | The region being added. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_database_primary_region`

An event of type `alter_database_primary_region` is recorded when a primary region is added/modified.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | yes |
| `PrimaryRegionName` | The new primary region. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_database_survival_goal`

An event of type `alter_database_survival_goal` is recorded when the survival goal is modified.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | yes |
| `SurvivalGoal` | The new survival goal | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_index`

An event of type `alter_index` is recorded when an index is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | yes |
| `IndexName` | The name of the affected index. | yes |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_sequence`

An event of type `alter_sequence` is recorded when a sequence is altered.


| Field | Description | Sensitive |
|--|--|--|
| `SequenceName` | The name of the affected sequence. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_table`

An event of type `alter_table` is recorded when a table is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | yes |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update, if any. | no |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_type`

EventAlterType is recorded when a user-defined type is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `comment_on_column`

An event of type `comment_on_column` is recorded when a column is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected column. | yes |
| `ColumnName` | The affected column. | yes |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `comment_on_database`

CommentOnTable is recorded when a database is commented.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the affected database. | yes |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `comment_on_index`

An event of type `comment_on_index` is recorded when an index is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | yes |
| `IndexName` | The name of the affected index. | yes |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `comment_on_table`

An event of type `comment_on_table` is recorded when a table is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | yes |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `convert_to_schema`

An event of type `convert_to_schema` is recorded when a database is converted to a schema.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database being converted to a schema. | yes |
| `NewDatabaseParent` | The name of the parent database for the new schema. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_database`

An event of type `create_database` is recorded when a database is created.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the new database. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_index`

An event of type `create_index` is recorded when an index is created.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the new index. | yes |
| `IndexName` | The name of the new index. | yes |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_schema`

An event of type `create_schema` is recorded when a schema is created.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The name of the new schema. | yes |
| `Owner` | The name of the owner for the new schema. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_sequence`

An event of type `create_sequence` is recorded when a sequence is created.


| Field | Description | Sensitive |
|--|--|--|
| `SequenceName` | The name of the new sequence. | yes |
| `Owner` | The name of the owner for the new sequence. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_statistics`

An event of type `create_statistics` is recorded when statistics are collected for a
table.

Events of this type are only collected when the cluster setting
`sql.stats.post_events.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table for which the statistics were created. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_table`

An event of type `create_table` is recorded when a table is created.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the new table. | yes |
| `Owner` | The name of the owner for the new table. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_type`

An event of type `create_type` is recorded when a user-defined type is created.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the new type. | yes |
| `Owner` | The name of the owner for the new type. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_view`

An event of type `create_view` is recorded when a view is created.


| Field | Description | Sensitive |
|--|--|--|
| `ViewName` | The name of the new view. | yes |
| `Owner` | The name of the owner of the new view. | yes |
| `ViewQuery` | The SQL selection clause used to define the view. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_database`

An event of type `drop_database` is recorded when a database is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the affected database. | yes |
| `DroppedSchemaObjects` | The names of the schemas dropped by a cascade operation. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_index`

An event of type `drop_index` is recorded when an index is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | yes |
| `IndexName` | The name of the affected index. | yes |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. | no |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_schema`

An event of type `drop_schema` is recorded when a schema is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The name of the affected schema. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_sequence`

An event of type `drop_sequence` is recorded when a sequence is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `SequenceName` | The name of the affected sequence. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_table`

An event of type `drop_table` is recorded when a table is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | yes |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_type`

An event of type `drop_type` is recorded when a user-defined type is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_view`

An event of type `drop_view` is recorded when a view is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `ViewName` | The name of the affected view. | yes |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `finish_schema_change`

An event of type `finish_schema_change` is recorded when a previously initiated schema
change has completed.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `MutationID` | The descriptor mutation that this schema change was processing. | no |

### `finish_schema_change_rollback`

An event of type `finish_schema_change_rollback` is recorded when a previously
initiated schema change rollback has completed.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `MutationID` | The descriptor mutation that this schema change was processing. | no |

### `rename_database`

An event of type `rename_database` is recorded when a database is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The old name of the affected database. | yes |
| `NewDatabaseName` | The new name of the affected database. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `rename_schema`

An event of type `rename_schema` is recorded when a schema is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The old name of the affected schema. | yes |
| `NewSchemaName` | The new name of the affected schema. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `rename_table`

An event of type `rename_table` is recorded when a table, sequence or view is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The old name of the affected table. | yes |
| `NewTableName` | The new name of the affected table. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `rename_type`

An event of type `rename_type` is recorded when a user-defined type is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The old name of the affected type. | yes |
| `NewTypeName` | The new name of the affected type. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `reverse_schema_change`

An event of type `reverse_schema_change` is recorded when an in-progress schema change
encounters a problem and is reversed.


| Field | Description | Sensitive |
|--|--|--|
| `Error` | The error encountered that caused the schema change to be reversed. The specific format of the error is variable and can change across releases without warning. | yes |
| `SQLSTATE` | The SQLSTATE code for the error. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `MutationID` | The descriptor mutation that this schema change was processing. | no |

### `truncate_table`

An event of type `truncate_table` is recorded when a table is truncated.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `unsafe_delete_descriptor`

An event of type `unsafe_delete_descriptor` is recorded when a descriptor is written
using crdb_internal.unsafe_delete_descriptor().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description | Sensitive |
|--|--|--|
| `ParentID` |  | no |
| `ParentSchemaID` |  | no |
| `Name` |  | yes |
| `Force` |  | no |
| `ForceNotice` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `unsafe_delete_namespace_entry`

An event of type `unsafe_delete_namespace_entry` is recorded when a namespace entry is
written using crdb_internal.unsafe_delete_namespace_entry().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description | Sensitive |
|--|--|--|
| `ParentID` |  | no |
| `ParentSchemaID` |  | no |
| `Name` |  | yes |
| `Force` |  | no |
| `ForceNotice` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `unsafe_upsert_descriptor`

An event of type `unsafe_upsert_descriptor` is recorded when a descriptor is written
using crdb_internal.unsafe_upsert_descriptor().


| Field | Description | Sensitive |
|--|--|--|
| `PreviousDescriptor` |  | yes |
| `NewDescriptor` |  | yes |
| `Force` |  | no |
| `ForceNotice` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `unsafe_upsert_namespace_entry`

An event of type `unsafe_upsert_namespace_entry` is recorded when a namespace entry is
written using crdb_internal.unsafe_upsert_namespace_entry().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description | Sensitive |
|--|--|--|
| `ParentID` |  | no |
| `ParentSchemaID` |  | no |
| `Name` |  | yes |
| `PreviousID` |  | no |
| `Force` |  | no |
| `FailedValidation` |  | no |
| `ValidationErrors` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

## SQL Privilege changes

Events in this category pertain to DDL (Data Definition Language)
operations performed by SQL statements that modify the privilege
grants for stored objects.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own system.eventlog table.

Events in this category are logged to channel PRIVILEGES.


### `alter_database_owner`

An event of type `alter_database_owner` is recorded when a database's owner is changed.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database being affected. | yes |
| `Owner` | The name of the new owner. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_schema_owner`

An event of type `alter_schema_owner` is recorded when a schema's owner is changed.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The name of the affected schema. | yes |
| `Owner` | The name of the new owner. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_table_owner`

An event of type `alter_table_owner` is recorded when the owner of a table, view or sequence is changed.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected object. | yes |
| `Owner` | The name of the new owner. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `alter_type_owner`

An event of type `alter_type_owner` is recorded when the owner of a user-defiend type is changed.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | yes |
| `Owner` | The name of the new owner. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `change_database_privilege`

An event of type `change_database_privilege` is recorded when privileges are
added to / removed from a user for a database object.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the affected database. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

### `change_schema_privilege`

An event of type `change_schema_privilege` is recorded when privileges are added to /
removed from a user for a schema object.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The name of the affected schema. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

### `change_table_privilege`

An event of type `change_table_privilege` is recorded when privileges are added to / removed
from a user for a table, sequence or view object.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

### `change_type_privilege`

An event of type `change_type_privilege` is recorded when privileges are added to /
removed from a user for a type object.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

## SQL Session events

Events in this category report SQL client connections
and sessions.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these miscellaneous events are
preserved in each tenant's own system.eventlog table.

Events in this category are logged to channel SESSIONS.


### `client_authentication_failed`

An event of type `client_authentication_failed` is reported when a client session
did not authenticate successfully.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_sessions.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Reason` | The reason for the authentication failure. See below for possible values for type `AuthFailReason`. | no |
| `Detail` | The detailed error for the authentication failure. | yes |
| `Method` | The authentication method used. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The username the session is for. This is the username passed by the client, after case-folding and Unicode normalization. | yes |

### `client_authentication_info`

An event of type `client_authentication_info` is reported for intermediate
steps during the authentication process.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_sessions.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Method` | The authentication method used, once known. | no |
| `Info` | The authentication progress message. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The username the session is for. This is the username passed by the client, after case-folding and Unicode normalization. | yes |

### `client_authentication_ok`

An event of type `client_authentication_ok` is reported when a client session
was authenticated successfully.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_sessions.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Method` | The authentication method used. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The username the session is for. This is the username passed by the client, after case-folding and Unicode normalization. | yes |

### `client_connection_end`

An event of type `client_connection_end` is reported when a client connection
is closed. This is reported even when authentication
fails, and even for simple cancellation messages.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_connections.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Duration` | The duration of the connection in nanoseconds. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |

### `client_connection_start`

An event of type `client_connection_start` is reported when a client connection
is established. This is reported even when authentication
fails, and even for simple cancellation messages.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_connections.enabled` is set.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |

### `client_session_end`

An event of type `client_session_end` is reported when a client session
is completed.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_sessions.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Duration` | The duration of the connection in nanoseconds. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The username the session is for. This is the username passed by the client, after case-folding and Unicode normalization. | yes |

## SQL User and Role operations

Events in this category pertain to SQL statements that modify the
properties of users and roles.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own system.eventlog table.

Events in this category are logged to channel USER_ADMIN.


### `alter_role`

An event of type `alter_role` is recorded when a role is altered.


| Field | Description | Sensitive |
|--|--|--|
| `RoleName` | The name of the affected user/role. | yes |
| `Options` | The options set on the user/role. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `create_role`

An event of type `create_role` is recorded when a role is created.


| Field | Description | Sensitive |
|--|--|--|
| `RoleName` | The name of the new user/role. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

### `drop_role`

An event of type `drop_role` is recorded when a role is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `RoleName` | The name of the affected user/role. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |

## Zone config events

Events in this category pertain to zone config changes on
the SQL schema or system ranges.

When zone configs apply to individual tables or other objects in a
SQL logical schema, they are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these zone config events are preserved
in each tenant's own system.eventlog table.

When they apply to cluster-level ranges (e.g.  the system zone config),
they are stored in the system tenant's own system.eventlog table.

Events in this category are logged to channel OPS.


### `remove_zone_config`

An event of type `remove_zone_config` is recorded when a zone config is removed.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |
| `Target` | The target object of the zone config change. | yes |
| `Config` | The applied zone config in YAML format. | yes |
| `Options` | The SQL representation of the applied zone config options. | yes |

### `set_zone_config`

An event of type `set_zone_config` is recorded when a zone config is changed.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | yes |
| `Target` | The target object of the zone config change. | yes |
| `Config` | The applied zone config in YAML format. | yes |
| `Options` | The SQL representation of the applied zone config options. | yes |




## Enumeration types

### `AuthFailReason`

AuthFailReason is the inventory of possible reasons for an
authentication failure.


| Value | Textual alias in code or documentation | Description |
|--|--|
| 0 | UNKNOWN | is reported when the reason is unknown. |
| 1 | USER_RETRIEVAL_ERROR | occurs when there was an internal error accessing the principals. |
| 2 | USER_NOT_FOUND | occurs when the principal is unknown. |
| 3 | LOGIN_DISABLED | occurs when the user does not have LOGIN privileges. |
| 4 | METHOD_NOT_FOUND | occurs when no HBA rule matches or the method does not exist. |
| 5 | PRE_HOOK_ERROR | occurs when the authentication handshake encountered a protocol error. |
| 6 | CREDENTIALS_INVALID | occurs when the client-provided credentials were invalid. |
| 7 | CREDENTIALS_EXPIRED | occur when the credentials provided by the client are expired. |



