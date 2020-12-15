# Cluster-level events

Events in this category pertain to an entire cluster and are
not relative to any particular tenant.

In a multi-tenant setup, the system.eventlog table for individual
tenants cannot contain a copy of cluster-level events; conversely,
the system.eventlog table in the system tenant cannot contain the
SQL-level events for individual tenants.

Events in this category are logged to channel DEV.


## `node_decommissioned`

An event of type `node_decommissioned` is recorded when a node is marked as
decommissioned.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `RequestingNodeID` | The node ID where the event was originated. |
| `TargetNodeID` | The node ID affected by the operation. |

## `node_decommissioning`

NodeDecommissioned is recorded when a node is marked as
decommissioning.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `RequestingNodeID` | The node ID where the event was originated. |
| `TargetNodeID` | The node ID affected by the operation. |

## `node_join`

An event of type `node_join` is recorded when a node joins the cluster.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `NodeID` | The node ID where the event was originated. |
| `StartedAt` | The time when this node was last started. |
| `LastUp` | The approximate last time the node was up before the last restart. |

## `node_recommissioned`

An event of type `node_recommissioned` is recorded when a decommissioning node is
recommissioned.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `RequestingNodeID` | The node ID where the event was originated. |
| `TargetNodeID` | The node ID affected by the operation. |

## `node_restart`

An event of type `node_restart` is recorded when an existing node rejoins the cluster
after being offline.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `NodeID` | The node ID where the event was originated. |
| `StartedAt` | The time when this node was last started. |
| `LastUp` | The approximate last time the node was up before the last restart. |

# Miscellaneous SQL events

Events in this category report miscellaneous SQL events.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these miscellaneous events are
preserved in each tenant's own system.eventlog table.

Events in this category are logged to channel DEV.


## `set_cluster_setting`

An event of type `set_cluster_setting` is recorded when a cluster setting is changed.


| Field | Description |
|--|--|
| `SettingName` | The name of the affected cluster setting. |
| `Value` | The new value of the cluster setting. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

# SQL Logical Schema Changes

Events in this category pertain to DDL (Data Definition Language)
operations performed by SQL statements that modify the SQL logical
schema.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own system.eventlog table.

Events in this category are logged to channel SQL_SCHEMA.


## `alter_index`

An event of type `alter_index` is recorded when an index is altered.


| Field | Description |
|--|--|
| `TableName` | The name of the table containing the affected index. |
| `IndexName` | The name of the affected index. |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `alter_sequence`

An event of type `alter_sequence` is recorded when a sequence is altered.


| Field | Description |
|--|--|
| `SequenceName` | The name of the affected sequence. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `alter_table`

An event of type `alter_table` is recorded when a table is altered.


| Field | Description |
|--|--|
| `TableName` | The name of the affected table. |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update, if any. |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `alter_type`

EventAlterType is recorded when a user-defined type is altered.


| Field | Description |
|--|--|
| `TypeName` | The name of the affected type. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `comment_on_column`

An event of type `comment_on_column` is recorded when a column is commented.


| Field | Description |
|--|--|
| `TableName` | The name of the table containing the affected column. |
| `ColumnName` | The affected column. |
| `Comment` | The new comment. |
| `NullComment` | Set to true if the comment was removed entirely. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `comment_on_database`

CommentOnTable is recorded when a database is commented.


| Field | Description |
|--|--|
| `DatabaseName` | The name of the affected database. |
| `Comment` | The new comment. |
| `NullComment` | Set to true if the comment was removed entirely. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `comment_on_index`

An event of type `comment_on_index` is recorded when an index is commented.


| Field | Description |
|--|--|
| `TableName` | The name of the table containing the affected index. |
| `IndexName` | The name of the affected index. |
| `Comment` | The new comment. |
| `NullComment` | Set to true if the comment was removed entirely. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `comment_on_table`

An event of type `comment_on_table` is recorded when a table is commented.


| Field | Description |
|--|--|
| `TableName` | The name of the affected table. |
| `Comment` | The new comment. |
| `NullComment` | Set to true if the comment was removed entirely. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `convert_to_schema`

An event of type `convert_to_schema` is recorded when a database is converted to a schema.


| Field | Description |
|--|--|
| `DatabaseName` | The name of the database being converted to a schema. |
| `NewDatabaseParent` | The name of the parent database for the new schema. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_database`

An event of type `create_database` is recorded when a database is created.


| Field | Description |
|--|--|
| `DatabaseName` | The name of the new database. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_index`

An event of type `create_index` is recorded when an index is created.


| Field | Description |
|--|--|
| `TableName` | The name of the table containing the new index. |
| `IndexName` | The name of the new index. |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_schema`

An event of type `create_schema` is recorded when a schema is created.


| Field | Description |
|--|--|
| `SchemaName` | The name of the new schema. |
| `Owner` | The name of the owner for the new schema. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_sequence`

An event of type `create_sequence` is recorded when a sequence is created.


| Field | Description |
|--|--|
| `SequenceName` | The name of the new sequence. |
| `Owner` | The name of the owner for the new sequence. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_statistics`

An event of type `create_statistics` is recorded when statistics are collected for a
table.

Events of this type are only collected when the cluster setting
`sql.stats.post_events.enabled` is set.


| Field | Description |
|--|--|
| `TableName` | The name of the table for which the statistics were created. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_table`

An event of type `create_table` is recorded when a table is created.


| Field | Description |
|--|--|
| `TableName` | The name of the new table. |
| `Owner` | The name of the owner for the new table. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_type`

An event of type `create_type` is recorded when a user-defined type is created.


| Field | Description |
|--|--|
| `TypeName` | The name of the new type. |
| `Owner` | The name of the owner for the new type. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_view`

An event of type `create_view` is recorded when a view is created.


| Field | Description |
|--|--|
| `ViewName` | The name of the new view. |
| `Owner` | The name of the owner of the new view. |
| `ViewQuery` | The SQL selection clause used to define the view. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_database`

An event of type `drop_database` is recorded when a database is dropped.


| Field | Description |
|--|--|
| `DatabaseName` | The name of the affected database. |
| `DroppedSchemaObjects` | The names of the schemas dropped by a cascade operation. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_index`

An event of type `drop_index` is recorded when an index is dropped.


| Field | Description |
|--|--|
| `TableName` | The name of the table containing the affected index. |
| `IndexName` | The name of the affected index. |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_schema`

An event of type `drop_schema` is recorded when a schema is dropped.


| Field | Description |
|--|--|
| `SchemaName` | The name of the affected schema. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_sequence`

An event of type `drop_sequence` is recorded when a sequence is dropped.


| Field | Description |
|--|--|
| `SequenceName` | The name of the affected sequence. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_table`

An event of type `drop_table` is recorded when a table is dropped.


| Field | Description |
|--|--|
| `TableName` | The name of the affected table. |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_type`

An event of type `drop_type` is recorded when a user-defined type is dropped.


| Field | Description |
|--|--|
| `TypeName` | The name of the affected type. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_view`

An event of type `drop_view` is recorded when a view is dropped.


| Field | Description |
|--|--|
| `ViewName` | The name of the affected view. |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `finish_schema_change`

An event of type `finish_schema_change` is recorded when a previously initiated schema
change has completed.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `MutationID` | The descriptor mutation that this schema change was processing. |

## `finish_schema_change_rollback`

An event of type `finish_schema_change_rollback` is recorded when a previously
initiated schema change rollback has completed.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `MutationID` | The descriptor mutation that this schema change was processing. |

## `remove_zone_config`

An event of type `remove_zone_config` is recorded when a zone config is removed.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `Target` | The target object of the zone config change. |
| `Config` | The applied zone config in YAML format. |
| `Options` | The SQL representation of the applied zone config options. |

## `rename_database`

An event of type `rename_database` is recorded when a database is renamed.


| Field | Description |
|--|--|
| `DatabaseName` | The old name of the affected database. |
| `NewDatabaseName` | The new name of the affected database. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `rename_schema`

An event of type `rename_schema` is recorded when a schema is renamed.


| Field | Description |
|--|--|
| `SchemaName` | The old name of the affected schema. |
| `NewSchemaName` | The new name of the affected schema. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `rename_table`

An event of type `rename_table` is recorded when a table, sequence or view is renamed.


| Field | Description |
|--|--|
| `TableName` | The old name of the affected table. |
| `NewTableName` | The new name of the affected table. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `rename_type`

An event of type `rename_type` is recorded when a user-defined type is renamed.


| Field | Description |
|--|--|
| `TypeName` | The old name of the affected type. |
| `NewTypeName` | The new name of the affected type. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `reverse_schema_change`

An event of type `reverse_schema_change` is recorded when an in-progress schema change
encounters a problem and is reversed.


| Field | Description |
|--|--|
| `Error` | The error encountered that caused the schema change to be reversed. The specific format of the error is variable and can change across releases without warning. |
| `SQLSTATE` | The SQLSTATE code for the error. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `MutationID` | The descriptor mutation that this schema change was processing. |

## `set_zone_config`

An event of type `set_zone_config` is recorded when a zone config is changed.




### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `Target` | The target object of the zone config change. |
| `Config` | The applied zone config in YAML format. |
| `Options` | The SQL representation of the applied zone config options. |

## `truncate_table`

An event of type `truncate_table` is recorded when a table is truncated.


| Field | Description |
|--|--|
| `TableName` | The name of the affected table. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `unsafe_delete_descriptor`

An event of type `unsafe_delete_descriptor` is recorded when a descriptor is written
using crdb_internal.unsafe_delete_descriptor().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description |
|--|--|
| `ParentID` |  |
| `ParentSchemaID` |  |
| `Name` |  |
| `Force` |  |
| `ForceNotice` |  |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `unsafe_delete_namespace_entry`

An event of type `unsafe_delete_namespace_entry` is recorded when a namespace entry is
written using crdb_internal.unsafe_delete_namespace_entry().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description |
|--|--|
| `ParentID` |  |
| `ParentSchemaID` |  |
| `Name` |  |
| `Force` |  |
| `ForceNotice` |  |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `unsafe_upsert_descriptor`

An event of type `unsafe_upsert_descriptor` is recorded when a descriptor is written
using crdb_internal.unsafe_upsert_descriptor().


| Field | Description |
|--|--|
| `PreviousDescriptor` |  |
| `NewDescriptor` |  |
| `Force` |  |
| `ForceNotice` |  |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `unsafe_upsert_namespace_entry`

An event of type `unsafe_upsert_namespace_entry` is recorded when a namespace entry is
written using crdb_internal.unsafe_upsert_namespace_entry().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description |
|--|--|
| `ParentID` |  |
| `ParentSchemaID` |  |
| `Name` |  |
| `PreviousID` |  |
| `Force` |  |
| `FailedValidation` |  |
| `ValidationErrors` |  |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

# SQL Privilege changes

Events in this category pertain to DDL (Data Definition Language)
operations performed by SQL statements that modify the privilege
grants for stored objects.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own system.eventlog table.

Events in this category are logged to channel PRIVILEGES.


## `alter_database_owner`

An event of type `alter_database_owner` is recorded when a database's owner is changed.


| Field | Description |
|--|--|
| `DatabaseName` | The name of the database being affected. |
| `Owner` | The name of the new owner. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `alter_schema_owner`

An event of type `alter_schema_owner` is recorded when a schema's owner is changed.


| Field | Description |
|--|--|
| `SchemaName` | The name of the affected schema. |
| `Owner` | The name of the new owner. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `alter_type_owner`

An event of type `alter_type_owner` is recorded when the owner of a user-defiend type is changed.


| Field | Description |
|--|--|
| `TypeName` | The name of the affected type. |
| `Owner` | The name of the new owner. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `change_database_privilege`

An event of type `change_database_privilege` is recorded when privileges are
added to / removed from a user for a database object.


| Field | Description |
|--|--|
| `DatabaseName` | The name of the affected database. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `Grantee` | The user/role affected by the grant or revoke operation. |
| `GrantedPrivileges` | The privileges being granted to the grantee. |
| `RevokedPrivileges` | The privileges being revoked from the grantee. |

## `change_schema_privilege`

An event of type `change_schema_privilege` is recorded when privileges are added to /
removed from a user for a schema object.


| Field | Description |
|--|--|
| `SchemaName` | The name of the affected schema. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `Grantee` | The user/role affected by the grant or revoke operation. |
| `GrantedPrivileges` | The privileges being granted to the grantee. |
| `RevokedPrivileges` | The privileges being revoked from the grantee. |

## `change_table_privilege`

An event of type `change_table_privilege` is recorded when privileges are added to / removed
from a user for a table, sequence or view object.


| Field | Description |
|--|--|
| `TableName` | The name of the affected table. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `Grantee` | The user/role affected by the grant or revoke operation. |
| `GrantedPrivileges` | The privileges being granted to the grantee. |
| `RevokedPrivileges` | The privileges being revoked from the grantee. |

## `change_type_privilege`

An event of type `change_type_privilege` is recorded when privileges are added to /
removed from a user for a type object.


| Field | Description |
|--|--|
| `TypeName` | The name of the affected type. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |
| `Grantee` | The user/role affected by the grant or revoke operation. |
| `GrantedPrivileges` | The privileges being granted to the grantee. |
| `RevokedPrivileges` | The privileges being revoked from the grantee. |

# SQL User and Role operations

Events in this category pertain to SQL statements that modify the
properties of users and roles.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own system.eventlog table.

Events in this category are logged to channel USER_ADMIN.


## `alter_role`

An event of type `alter_role` is recorded when a role is altered.


| Field | Description |
|--|--|
| `RoleName` | The name of the affected user/role. |
| `Options` | The options set on the user/role. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `create_role`

An event of type `create_role` is recorded when a role is created.


| Field | Description |
|--|--|
| `RoleName` | The name of the new user/role. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |

## `drop_role`

An event of type `drop_role` is recorded when a role is dropped.


| Field | Description |
|--|--|
| `RoleName` | The name of the affected user/role. |


### Common fields

| Field | Description |
|--|--|
| `Timestamp` | The timestamp of the event. |
| `EventType` | The type of the event. |
| `Statement` | A normalized copy of the SQL statement that triggered the event. |
| `User` | The user account that triggered the event. |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. |


