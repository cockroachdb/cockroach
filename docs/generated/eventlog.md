Certain notable events are reported using a structured format.
Commonly, these notable events are also copied to the table
`system.eventlog`, unless the cluster setting
`server.eventlog.enabled` is unset.

Additionally, notable events are copied to specific external logging
channels in log messages, where they can be collected for further processing.

The sections below document the possible notable event types
in this version of CockroachDB. For each event type, a table
documents the possible fields. A field may be omitted from
an event if its value is empty or zero.

A field is also considered "Sensitive" if it may contain
application-specific information or personally identifiable information (PII). In that case,
the copy of the event sent to the external logging channel
will contain redaction markers in a format that is compatible
with the redaction facilities in [`cockroach debug zip`](cockroach-debug-zip.html)
and [`cockroach debug merge-logs`](cockroach-debug-merge-logs.html),
provided the `redactable` functionality is enabled on the logging sink.

Events not documented on this page will have an unstructured format in log messages.

## Changefeed telemetry events

Events in this category pertain to changefeed usage and metrics.

Events in this category are logged to the `TELEMETRY` channel.


### `changefeed_canceled`

An event of type `changefeed_canceled` is an event for any changefeed cancellations.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Description` | The description of that would show up in the job's description field, redacted | yes |
| `SinkType` | The type of sink being emitted to (ex: kafka, nodelocal, webhook-https). | no |
| `NumTables` | The number of tables listed in the query that the changefeed is to run on. | no |
| `Resolved` | The behavior of emitted resolved spans (ex: yes, no, 10s) | no |
| `InitialScan` | The desired behavior of initial scans (ex: yes, no, only) | no |
| `Format` | The data format being emitted (ex: JSON, Avro). | no |
| `JobId` | The job id for enterprise changefeeds. | no |

### `changefeed_emitted_bytes`

An event of type `changefeed_emitted_bytes` is an event representing the bytes emitted by a changefeed over an interval.


| Field | Description | Sensitive |
|--|--|--|
| `EmittedBytes` | The number of bytes emitted. | no |
| `EmittedMessages` | The number of messages emitted. | no |
| `LoggingInterval` | The time period in nanoseconds between emitting telemetry events of this type (per-aggregator). | no |
| `Closing` | Flag to indicate that the changefeed is closing. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Description` | The description of that would show up in the job's description field, redacted | yes |
| `SinkType` | The type of sink being emitted to (ex: kafka, nodelocal, webhook-https). | no |
| `NumTables` | The number of tables listed in the query that the changefeed is to run on. | no |
| `Resolved` | The behavior of emitted resolved spans (ex: yes, no, 10s) | no |
| `InitialScan` | The desired behavior of initial scans (ex: yes, no, only) | no |
| `Format` | The data format being emitted (ex: JSON, Avro). | no |
| `JobId` | The job id for enterprise changefeeds. | no |

### `changefeed_failed`

An event of type `changefeed_failed` is an event for any changefeed failure since the plan hook
was triggered.


| Field | Description | Sensitive |
|--|--|--|
| `FailureType` | The reason / environment with which the changefeed failed (ex: connection_closed, changefeed_behind). | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Description` | The description of that would show up in the job's description field, redacted | yes |
| `SinkType` | The type of sink being emitted to (ex: kafka, nodelocal, webhook-https). | no |
| `NumTables` | The number of tables listed in the query that the changefeed is to run on. | no |
| `Resolved` | The behavior of emitted resolved spans (ex: yes, no, 10s) | no |
| `InitialScan` | The desired behavior of initial scans (ex: yes, no, only) | no |
| `Format` | The data format being emitted (ex: JSON, Avro). | no |
| `JobId` | The job id for enterprise changefeeds. | no |

### `create_changefeed`

An event of type `create_changefeed` is an event for any CREATE CHANGEFEED query that
successfully starts running. Failed CREATE statements will show up as
ChangefeedFailed events.


| Field | Description | Sensitive |
|--|--|--|
| `Transformation` | Flag representing whether the changefeed is using CDC queries. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Description` | The description of that would show up in the job's description field, redacted | yes |
| `SinkType` | The type of sink being emitted to (ex: kafka, nodelocal, webhook-https). | no |
| `NumTables` | The number of tables listed in the query that the changefeed is to run on. | no |
| `Resolved` | The behavior of emitted resolved spans (ex: yes, no, 10s) | no |
| `InitialScan` | The desired behavior of initial scans (ex: yes, no, only) | no |
| `Format` | The data format being emitted (ex: JSON, Avro). | no |
| `JobId` | The job id for enterprise changefeeds. | no |

## Cluster-level events

Events in this category pertain to an entire cluster and are
not relative to any particular tenant.

In a multi-tenant setup, the `system.eventlog` table for individual
tenants cannot contain a copy of cluster-level events; conversely,
the `system.eventlog` table in the system tenant cannot contain the
SQL-level events for individual tenants.

Events in this category are logged to the `OPS` channel.


### `certs_reload`

An event of type `certs_reload` is recorded when the TLS certificates are
reloaded/rotated from disk.


| Field | Description | Sensitive |
|--|--|--|
| `Success` | Whether the operation completed without errors. | no |
| `ErrorMessage` | If an error was encountered, the text of the error. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `disk_slowness_cleared`

An event of type `disk_slowness_cleared` is recorded when disk slowness in a store has cleared.


| Field | Description | Sensitive |
|--|--|--|
| `NodeID` | The node ID where the event was originated. | no |
| `StoreID` |  | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `disk_slowness_detected`

An event of type `disk_slowness_detected` is recorded when a store observes disk slowness
events.


| Field | Description | Sensitive |
|--|--|--|
| `NodeID` | The node ID where the event was originated. | no |
| `StoreID` |  | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `low_disk_space`

An event of type `low_disk_space` is emitted when a store is reaching capacity, as we reach
certain thresholds. It is emitted periodically while we are in a low disk
state.


| Field | Description | Sensitive |
|--|--|--|
| `NodeID` | The node ID where the event was originated. | no |
| `StoreID` |  | no |
| `PercentThreshold` | The free space percent threshold that we went under. | no |
| `AvailableBytes` |  | no |
| `TotalBytes` |  | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

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

An event of type `node_decommissioning` is recorded when a node is marked as
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

### `node_shutdown_connection_timeout`

An event of type `node_shutdown_connection_timeout` is recorded when SQL connections remain open
during shutdown, after waiting for the server.shutdown.connections.timeout
to transpire.


| Field | Description | Sensitive |
|--|--|--|
| `Detail` | The detailed message, meant to be a human-understandable explanation. | no |
| `ConnectionsRemaining` | The number of connections still open after waiting for the client to close them. | no |
| `TimeoutMillis` | The amount of time the server waited for the client to close the connections, defined by server.shutdown.connections.timeout. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event was originated. | no |
| `StartedAt` | The time when this node was last started. | no |
| `LastUp` | The approximate last time the node was up before the last restart. | no |

### `node_shutdown_transaction_timeout`

An event of type `node_shutdown_transaction_timeout` is recorded when SQL transactions remain open
during shutdown, after waiting for the server.shutdown.transactions.timeout
to transpire.


| Field | Description | Sensitive |
|--|--|--|
| `Detail` | The detailed message, meant to be a human-understandable explanation. | no |
| `ConnectionsRemaining` | The number of connections still running SQL transactions after waiting for the client to end them. | no |
| `TimeoutMillis` | The amount of time the server waited for the client to close the connections, defined by server.shutdown.transactions.timeout. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event was originated. | no |
| `StartedAt` | The time when this node was last started. | no |
| `LastUp` | The approximate last time the node was up before the last restart. | no |

### `tenant_shared_service_start`

An event of type `tenant_shared_service_start` is recorded when a tenant server
is started inside the same process as the KV layer.


| Field | Description | Sensitive |
|--|--|--|
| `OK` | Whether the startup was successful. | no |
| `ErrorText` | If the startup failed, the text of the error. | partially |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event was originated. | no |
| `TenantID` | The ID of the tenant owning the service. | no |
| `InstanceID` | The ID of the server instance. | no |
| `TenantName` | The name of the tenant at the time the event was emitted. | yes |

### `tenant_shared_service_stop`

An event of type `tenant_shared_service_stop` is recorded when a tenant server
is shut down inside the same process as the KV layer.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event was originated. | no |
| `TenantID` | The ID of the tenant owning the service. | no |
| `InstanceID` | The ID of the server instance. | no |
| `TenantName` | The name of the tenant at the time the event was emitted. | yes |

## Debugging events

Events in this category pertain to debugging operations performed by
operators or (more commonly) Cockroach Labs employees. These operations can
e.g. directly access and mutate internal state, breaking system invariants.

Events in this category are logged to the `OPS` channel.


### `debug_recover_replica`

An event of type `debug_recover_replica` is recorded when unsafe loss of quorum recovery is performed.


| Field | Description | Sensitive |
|--|--|--|
| `RangeID` |  | no |
| `StoreID` |  | no |
| `SurvivorReplicaID` |  | no |
| `UpdatedReplicaID` |  | no |
| `StartKey` |  | yes |
| `EndKey` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event originated. | no |
| `User` | The user which performed the operation. | yes |

### `debug_send_kv_batch`

An event of type `debug_send_kv_batch` is recorded when an arbitrary KV BatchRequest is submitted
to the cluster via the `debug send-kv-batch` CLI command.


| Field | Description | Sensitive |
|--|--|--|
| `BatchRequest` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `NodeID` | The node ID where the event originated. | no |
| `User` | The user which performed the operation. | yes |

## Health events

Events in this category pertain to the health of one or more servers.

Events in this category are logged to the `HEALTH` channel.


### `runtime_stats`

An event of type `runtime_stats` is recorded every 10 seconds as server health metrics.


| Field | Description | Sensitive |
|--|--|--|
| `MemRSSBytes` | The process resident set size. Expressed as bytes. | no |
| `GoroutineCount` | The number of goroutines. | no |
| `MemStackSysBytes` | The stack system memory used. Expressed as bytes. | no |
| `GoAllocBytes` | The memory allocated by Go. Expressed as bytes. | no |
| `GoTotalBytes` | The total memory allocated by Go but not released. Expressed as bytes. | no |
| `GoStatsStaleness` | The staleness of the Go memory statistics. Expressed in seconds. | no |
| `HeapFragmentBytes` | The amount of heap fragmentation. Expressed as bytes. | no |
| `HeapReservedBytes` | The amount of heap reserved. Expressed as bytes. | no |
| `HeapReleasedBytes` | The amount of heap released. Expressed as bytes. | no |
| `CGoAllocBytes` | The memory allocated outside of Go. Expressed as bytes. | no |
| `CGoTotalBytes` | The total memory allocated outside of Go but not released. Expressed as bytes. | no |
| `CGoCallRate` | The total number of calls outside of Go over time. Expressed as operations per second. | no |
| `CPUUserPercent` | The user CPU percentage. | no |
| `CPUSysPercent` | The system CPU percentage. | no |
| `GCPausePercent` | The GC pause percentage. | no |
| `GCRunCount` | The total number of GC runs. | no |
| `NetHostRecvBytes` | The bytes received on all network interfaces since this process started. | no |
| `NetHostSendBytes` | The bytes sent on all network interfaces since this process started. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

## Job events

Events in this category pertain to long-running jobs that are orchestrated by
a node's job registry. These system processes can create and/or modify stored
objects during the course of their execution.

A job might choose to emit multiple events during its execution when
transitioning from one "state" to another.
Egs: IMPORT/RESTORE will emit events on job creation and successful
completion. If the job fails, events will be emitted on job creation,
failure, and successful revert.

Events in this category are logged to the `OPS` channel.


### `import`

An event of type `import` is recorded when an import job is created and successful completion.
If the job fails, events will be emitted on job creation, failure, and
successful revert.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `JobID` | The ID of the job that triggered the event. | no |
| `JobType` | The type of the job that triggered the event. | no |
| `Description` | A description of the job that triggered the event. Some jobs populate the description with an approximate representation of the SQL statement run to create the job. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorIDs` | The object descriptors affected by the job. Set to zero for operations that don't affect descriptors. | yes |
| `Status` | The status of the job that triggered the event. This allows the job to indicate which phase execution it is in when the event is triggered. | no |

### `restore`

An event of type `restore` is recorded when a restore job is created and successful completion.
If the job fails, events will be emitted on job creation, failure, and
successful revert.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `JobID` | The ID of the job that triggered the event. | no |
| `JobType` | The type of the job that triggered the event. | no |
| `Description` | A description of the job that triggered the event. Some jobs populate the description with an approximate representation of the SQL statement run to create the job. | yes |
| `User` | The user account that triggered the event. | yes |
| `DescriptorIDs` | The object descriptors affected by the job. Set to zero for operations that don't affect descriptors. | yes |
| `Status` | The status of the job that triggered the event. This allows the job to indicate which phase execution it is in when the event is triggered. | no |

### `status_change`

An event of type `status_change` is recorded when a job changes statuses.


| Field | Description | Sensitive |
|--|--|--|
| `JobID` | The ID of the job that is changing statuses. | no |
| `JobType` | The type of the job that is changing statuses. | no |
| `Description` | A human parsable description of the status change | partially |
| `PreviousStatus` | The status that the job is transitioning out of | no |
| `NewStatus` | The status that the job has transitioned into | no |
| `RunNum` | The run number of the job. | no |
| `Error` | An error that may have occurred while the job was running. | yes |
| `FinalResumeErr` | An error that occurred that requires the job to be reverted. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

## Miscellaneous SQL events

Events in this category report miscellaneous SQL events.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these miscellaneous events are
preserved in each tenant's own system.eventlog table.

Events in this category are logged to the `OPS` channel.


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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `set_tenant_cluster_setting`

An event of type `set_tenant_cluster_setting` is recorded when a cluster setting override
is changed, either for another tenant or for all tenants.


| Field | Description | Sensitive |
|--|--|--|
| `SettingName` | The name of the affected cluster setting. | no |
| `Value` | The new value of the cluster setting. | yes |
| `TenantId` | The target Tenant ID. Empty if targeting all tenants. | no |
| `AllTenants` | Whether the override applies to all tenants. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

## SQL Access Audit Events

Events in this category are generated when a table has been
marked as audited via `ALTER TABLE ... EXPERIMENTAL_AUDIT SET`.

Note: These events are not written to `system.eventlog`, even
when the cluster setting `system.eventlog.enabled` is set. They
are only emitted via external logging.

Events in this category are logged to the `SENSITIVE_ACCESS` channel.


### `admin_query`

An event of type `admin_query` is recorded when a user with admin privileges (the user
is directly or indirectly a member of the admin role) executes a query.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

### `role_based_audit_event`

An event of type `role_based_audit_event` is an audit event recorded when an executed query belongs to a user whose role
membership(s) correspond to any role that is enabled to emit an audit log via the sql.log.user_audit
cluster setting.


| Field | Description | Sensitive |
|--|--|--|
| `Role` | The configured audit role that emitted this log. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

### `sensitive_table_access`

An event of type `sensitive_table_access` is recorded when an access is performed to
a table marked as audited.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table being audited. | yes |
| `AccessMode` | How the table was accessed (r=read / rw=read/write). | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

## SQL Execution Log

Events in this category report executed queries.

Note: These events are not written to `system.eventlog`, even
when the cluster setting `system.eventlog.enabled` is set. They
are only emitted via external logging.

Events in this category are logged to the `SQL_EXEC` channel.


### `query_execute`

An event of type `query_execute` is recorded when a query is executed,
and the cluster setting `sql.log.all_statements.enabled` is set.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

## SQL Logical Schema Changes

Events in this category pertain to DDL (Data Definition Language)
operations performed by SQL statements that modify the SQL logical
schema.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own `system.eventlog` table.

Events in this category are logged to the `SQL_SCHEMA` channel.


### `alter_database_add_region`

An event of type `alter_database_add_region` is recorded when a region is added to a database.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | no |
| `RegionName` | The region being added. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_database_drop_region`

AlterDatabaseAddRegion is recorded when a region is added to a database.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | no |
| `RegionName` | The region being dropped. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_database_placement`

An event of type `alter_database_placement` is recorded when the database placement is modified.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | no |
| `Placement` | The new placement policy. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_database_primary_region`

An event of type `alter_database_primary_region` is recorded when a primary region is added/modified.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | no |
| `PrimaryRegionName` | The new primary region. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_database_set_zone_config_extension`

An event of type `alter_database_set_zone_config_extension` is recorded when a zone config extension is changed.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `Target` | The target object of the zone config change. | yes |
| `Config` | The applied zone config in YAML format. | yes |
| `Options` | The SQL representation of the applied zone config options. | yes |

### `alter_database_survival_goal`

An event of type `alter_database_survival_goal` is recorded when the survival goal is modified.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the database. | no |
| `SurvivalGoal` | The new survival goal | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_function_options`

An event of type `alter_function_options` is recorded when a user-defined function's options are
altered.


| Field | Description | Sensitive |
|--|--|--|
| `FunctionName` | Name of the affected function. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_index`

An event of type `alter_index` is recorded when an index is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | no |
| `IndexName` | The name of the affected index. | no |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_index_visible`

AlterIndex is recorded when an index visibility is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | no |
| `IndexName` | The name of the affected index. | no |
| `NotVisible` | Set true if index is not visible. NOTE: THIS FIELD IS DEPRECATED in favor of invisibility. | no |
| `Invisibility` | The new invisibility of the affected index. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_sequence`

An event of type `alter_sequence` is recorded when a sequence is altered.


| Field | Description | Sensitive |
|--|--|--|
| `SequenceName` | The name of the affected sequence. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_table`

An event of type `alter_table` is recorded when a table is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | no |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update, if any. | no |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_type`

EventAlterType is recorded when a user-defined type is altered.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_column`

An event of type `comment_on_column` is recorded when a column is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected column. | no |
| `ColumnName` | The affected column. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_constraint`

An event of type `comment_on_constraint` is recorded when an constraint is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected constraint. | no |
| `ConstraintName` | The name of the affected constraint. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_database`

CommentOnTable is recorded when a database is commented.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the affected database. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_index`

An event of type `comment_on_index` is recorded when an index is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | no |
| `IndexName` | The name of the affected index. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_schema`



| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | Name of the affected schema. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_table`

An event of type `comment_on_table` is recorded when a table is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `comment_on_type`

An event of type `comment_on_type` is recorded when a type is commented.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | no |
| `Comment` | The new comment. | yes |
| `NullComment` | Set to true if the comment was removed entirely. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_database`

An event of type `create_database` is recorded when a database is created.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the new database. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_function`

An event of type `create_function` is recorded when a user-defined function is created.


| Field | Description | Sensitive |
|--|--|--|
| `FunctionName` | Name of the created function. | no |
| `IsReplace` | If the new function is a replace of an existing function. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_index`

An event of type `create_index` is recorded when an index is created.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the new index. | no |
| `IndexName` | The name of the new index. | no |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_policy`

An event of type `create_policy` is recorded when a policy is created.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | Name of the policy's table. | no |
| `PolicyName` | Name of the created policy. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_schema`

An event of type `create_schema` is recorded when a schema is created.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The name of the new schema. | no |
| `Owner` | The name of the owner for the new schema. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_sequence`

An event of type `create_sequence` is recorded when a sequence is created.


| Field | Description | Sensitive |
|--|--|--|
| `SequenceName` | The name of the new sequence. | no |
| `Owner` | The name of the owner for the new sequence. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_statistics`

An event of type `create_statistics` is recorded when statistics are collected for a
table.

Events of this type are only collected when the cluster setting
`sql.stats.post_events.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table for which the statistics were created. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_table`

An event of type `create_table` is recorded when a table is created.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the new table. | no |
| `Owner` | The name of the owner for the new table. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_trigger`

An event of type `create_trigger` is recorded when a trigger is created.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | Name of the trigger's table. | no |
| `TriggerName` | Name of the created trigger. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_type`

An event of type `create_type` is recorded when a user-defined type is created.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the new type. | no |
| `Owner` | The name of the owner for the new type. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `create_view`

An event of type `create_view` is recorded when a view is created.


| Field | Description | Sensitive |
|--|--|--|
| `ViewName` | The name of the new view. | no |
| `Owner` | The name of the owner of the new view. | no |
| `ViewQuery` | The SQL selection clause used to define the view. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_database`

An event of type `drop_database` is recorded when a database is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the affected database. | no |
| `DroppedSchemaObjects` | The names of the schemas dropped by a cascade operation. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_function`

An event of type `drop_function` is recorded when a user-defined function is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `FunctionName` | Name of the dropped function. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_index`

An event of type `drop_index` is recorded when an index is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the table containing the affected index. | no |
| `IndexName` | The name of the affected index. | no |
| `MutationID` | The mutation ID for the asynchronous job that is processing the index update. | no |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_policy`

An event of type `drop_policy` is recorded when a policy is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | Name of the policy's table. | no |
| `PolicyName` | Name of the dropped policy. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_schema`

An event of type `drop_schema` is recorded when a schema is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The name of the affected schema. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_sequence`

An event of type `drop_sequence` is recorded when a sequence is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `SequenceName` | The name of the affected sequence. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_table`

An event of type `drop_table` is recorded when a table is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | no |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_trigger`

An event of type `drop_trigger` is recorded when a trigger is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | Name of the trigger's table. | no |
| `TriggerName` | Name of the dropped trigger. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_type`

An event of type `drop_type` is recorded when a user-defined type is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The name of the affected type. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `drop_view`

An event of type `drop_view` is recorded when a view is dropped.


| Field | Description | Sensitive |
|--|--|--|
| `ViewName` | The name of the affected view. | no |
| `CascadeDroppedViews` | The names of the views dropped as a result of a cascade operation. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `finish_schema_change`

An event of type `finish_schema_change` is recorded when a previously initiated schema
change has completed.


| Field | Description | Sensitive |
|--|--|--|
| `LatencyNanos` | The amount of time the schema change job took to complete. | no |


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


| Field | Description | Sensitive |
|--|--|--|
| `LatencyNanos` | The amount of time the schema change job took to rollback. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `MutationID` | The descriptor mutation that this schema change was processing. | no |

### `force_delete_table_data_entry`



| Field | Description | Sensitive |
|--|--|--|
| `DescriptorID` |  | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `rename_database`

An event of type `rename_database` is recorded when a database is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The old name of the affected database. | no |
| `NewDatabaseName` | The new name of the affected database. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `rename_function`

An event of type `rename_function` is recorded when a user-defined function is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `FunctionName` | The old name of the affected function. | no |
| `NewFunctionName` | The new name of the affected function. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `rename_schema`

An event of type `rename_schema` is recorded when a schema is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `SchemaName` | The old name of the affected schema. | no |
| `NewSchemaName` | The new name of the affected schema. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `rename_table`

An event of type `rename_table` is recorded when a table, sequence or view is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The old name of the affected table. | no |
| `NewTableName` | The new name of the affected table. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `rename_type`

An event of type `rename_type` is recorded when a user-defined type is renamed.


| Field | Description | Sensitive |
|--|--|--|
| `TypeName` | The old name of the affected type. | no |
| `NewTypeName` | The new name of the affected type. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `reverse_schema_change`

An event of type `reverse_schema_change` is recorded when an in-progress schema change
encounters a problem and is reversed.


| Field | Description | Sensitive |
|--|--|--|
| `Error` | The error encountered that caused the schema change to be reversed. The specific format of the error is variable and can change across releases without warning. | partially |
| `SQLSTATE` | The SQLSTATE code for the error. | no |
| `LatencyNanos` | The amount of time the schema change job took before being reverted. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `MutationID` | The descriptor mutation that this schema change was processing. | no |

### `set_schema`

An event of type `set_schema` is recorded when a table, view, sequence or type's schema is changed.


| Field | Description | Sensitive |
|--|--|--|
| `DescriptorName` | The old name of the affected descriptor. | no |
| `NewDescriptorName` | The new name of the affected descriptor. | no |
| `DescriptorType` | The descriptor type being changed (table, view, sequence, type). | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `truncate_table`

An event of type `truncate_table` is recorded when a table is truncated.


| Field | Description | Sensitive |
|--|--|--|
| `TableName` | The name of the affected table. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `unsafe_delete_descriptor`

An event of type `unsafe_delete_descriptor` is recorded when a descriptor is written
using crdb_internal.unsafe_delete_descriptor().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description | Sensitive |
|--|--|--|
| `ParentID` |  | no |
| `ParentSchemaID` |  | no |
| `Name` |  | no |
| `Force` |  | no |
| `ForceNotice` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `unsafe_delete_namespace_entry`

An event of type `unsafe_delete_namespace_entry` is recorded when a namespace entry is
written using crdb_internal.unsafe_delete_namespace_entry().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description | Sensitive |
|--|--|--|
| `ParentID` |  | no |
| `ParentSchemaID` |  | no |
| `Name` |  | no |
| `Force` |  | no |
| `ForceNotice` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `unsafe_upsert_namespace_entry`

An event of type `unsafe_upsert_namespace_entry` is recorded when a namespace entry is
written using crdb_internal.unsafe_upsert_namespace_entry().

The fields of this event type are reserved and can change across
patch releases without advance notice.


| Field | Description | Sensitive |
|--|--|--|
| `ParentID` |  | no |
| `ParentSchemaID` |  | no |
| `Name` |  | no |
| `PreviousID` |  | no |
| `Force` |  | no |
| `FailedValidation` |  | no |
| `ValidationErrors` |  | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

## SQL Privilege changes

Events in this category pertain to DDL (Data Definition Language)
operations performed by SQL statements that modify the privilege
grants for stored objects.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own `system.eventlog` table.

Events in this category are logged to the `PRIVILEGES` channel.


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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `alter_default_privileges`

An event of type `alter_default_privileges` is recorded when default privileges are changed.


| Field | Description | Sensitive |
|--|--|--|
| `DatabaseName` | The name of the affected database. | yes |
| `RoleName` | Either role_name should be populated or for_all_roles should be true. The role having its default privileges altered. | yes |
| `ForAllRoles` | Identifies if FOR ALL ROLES is used. | no |
| `SchemaName` | The name of the affected schema. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

### `alter_function_owner`

AlterTableOwner is recorded when the owner of a user-defined function is changed.


| Field | Description | Sensitive |
|--|--|--|
| `FunctionName` | The name of the affected user-defined function. | yes |
| `Owner` | The name of the new owner. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

### `change_function_privilege`



| Field | Description | Sensitive |
|--|--|--|
| `FuncName` | The name of the affected function. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `Grantee` | The user/role affected by the grant or revoke operation. | yes |
| `GrantedPrivileges` | The privileges being granted to the grantee. | no |
| `RevokedPrivileges` | The privileges being revoked from the grantee. | no |

## SQL Session events

Events in this category report SQL client connections
and sessions.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these miscellaneous events are
preserved in each tenant's own `system.eventlog` table.

Events in this category are logged to the `SESSIONS` channel.


### `client_authentication_failed`

An event of type `client_authentication_failed` is reported when a client session
did not authenticate successfully.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_sessions.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Reason` | The reason for the authentication failure. See below for possible values for type `AuthFailReason`. | no |
| `Detail` | The detailed error for the authentication failure. | partially |
| `Method` | The authentication method used. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |
| `SessionID` | The connection's hex encoded session id. | no |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The database username the session is for. This username will have undergone case-folding and Unicode normalization. | yes |
| `SystemIdentity` | The original system identity provided by the client, if an identity mapping was used per Host-Based Authentication rules. This may be a GSSAPI or X.509 principal or any other external value, so no specific assumptions should be made about the contents of this field. | yes |

### `client_authentication_info`

An event of type `client_authentication_info` is reported for intermediate
steps during the authentication process.

Events of this type are only emitted when the cluster setting
`server.auth_log.sql_sessions.enabled` is set.


| Field | Description | Sensitive |
|--|--|--|
| `Method` | The authentication method used, once known. | no |
| `Info` | The authentication progress message. | partially |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `InstanceID` | The instance ID (not tenant ID) of the SQL server where the event was originated. | no |
| `Network` | The network protocol for this connection: tcp4, tcp6, unix, etc. | no |
| `RemoteAddress` | The remote address of the SQL client. Note that when using a proxy or other intermediate server, this field will contain the address of the intermediate server. | yes |
| `SessionID` | The connection's hex encoded session id. | no |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The database username the session is for. This username will have undergone case-folding and Unicode normalization. | yes |
| `SystemIdentity` | The original system identity provided by the client, if an identity mapping was used per Host-Based Authentication rules. This may be a GSSAPI or X.509 principal or any other external value, so no specific assumptions should be made about the contents of this field. | yes |

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
| `SessionID` | The connection's hex encoded session id. | no |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The database username the session is for. This username will have undergone case-folding and Unicode normalization. | yes |
| `SystemIdentity` | The original system identity provided by the client, if an identity mapping was used per Host-Based Authentication rules. This may be a GSSAPI or X.509 principal or any other external value, so no specific assumptions should be made about the contents of this field. | yes |

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
| `SessionID` | The connection's hex encoded session id. | no |

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
| `SessionID` | The connection's hex encoded session id. | no |

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
| `SessionID` | The connection's hex encoded session id. | no |
| `Transport` | The connection type after transport negotiation. | no |
| `User` | The database username the session is for. This username will have undergone case-folding and Unicode normalization. | yes |
| `SystemIdentity` | The original system identity provided by the client, if an identity mapping was used per Host-Based Authentication rules. This may be a GSSAPI or X.509 principal or any other external value, so no specific assumptions should be made about the contents of this field. | yes |

## SQL Slow Query Log

Events in this category report slow query execution.

Note: these events are not written to `system.eventlog`, even
when the cluster setting `system.eventlog.enabled` is set. They
are only emitted via external logging.

Events in this category are logged to the `SQL_PERF` channel.


### `large_row`

An event of type `large_row` is recorded when a statement tries to write a row larger than
cluster setting `sql.guardrails.max_row_size_log` to the database. Multiple
LargeRow events will be recorded for statements writing multiple large rows.
LargeRow events are recorded before the transaction commits, so in the case
of transaction abort there will not be a corresponding row in the database.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `RowSize` |  | no |
| `TableID` |  | no |
| `FamilyID` |  | no |
| `PrimaryKey` |  | yes |

### `slow_query`

An event of type `slow_query` is recorded when a query triggers the "slow query" condition.

As of this writing, the condition requires:
- the cluster setting `sql.log.slow_query.latency_threshold`
set to a non-zero value, AND
- EITHER of the following conditions:
- the actual age of the query exceeds the configured threshold; AND/OR
- the query performs a full table/index scan AND the cluster setting
`sql.log.slow_query.experimental_full_table_scans.enabled` is set.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

### `txn_rows_read_limit`

An event of type `txn_rows_read_limit` is recorded when a transaction tries to read more rows than
cluster setting `sql.defaults.transaction_rows_read_log`. There will only be
a single record for a single transaction (unless it is retried) even if there
are more statement within the transaction that haven't been executed yet.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `TxnID` | TxnID is the ID of the transaction that hit the row count limit. | no |
| `SessionID` | SessionID is the ID of the session that initiated the transaction. | no |
| `NumRows` | NumRows is the number of rows written/read (depending on the event type) by the transaction that reached the corresponding guardrail. | no |

### `txn_rows_written_limit`

An event of type `txn_rows_written_limit` is recorded when a transaction tries to write more rows
than cluster setting `sql.defaults.transaction_rows_written_log`. There will
only be a single record for a single transaction (unless it is retried) even
if there are more mutation statements within the transaction that haven't
been executed yet.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `TxnID` | TxnID is the ID of the transaction that hit the row count limit. | no |
| `SessionID` | SessionID is the ID of the session that initiated the transaction. | no |
| `NumRows` | NumRows is the number of rows written/read (depending on the event type) by the transaction that reached the corresponding guardrail. | no |

## SQL Slow Query Log (Internal)

Events in this category report slow query execution by
internal executors, i.e., when CockroachDB internally issues
SQL statements.

Note: these events are not written to `system.eventlog`, even
when the cluster setting `system.eventlog.enabled` is set. They
are only emitted via external logging.

Events in this category are logged to the `SQL_INTERNAL_PERF` channel.


### `large_row_internal`

An event of type `large_row_internal` is recorded when an internal query tries to write a row
larger than cluster settings `sql.guardrails.max_row_size_log` or
`sql.guardrails.max_row_size_err` to the database.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `RowSize` |  | no |
| `TableID` |  | no |
| `FamilyID` |  | no |
| `PrimaryKey` |  | yes |

### `slow_query_internal`

An event of type `slow_query_internal` is recorded when a query triggers the "slow query" condition,
and the cluster setting `sql.log.slow_query.internal_queries.enabled` is
set.
See the documentation for the event type `slow_query` for details about
the "slow query" condition.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

### `txn_rows_read_limit_internal`

An event of type `txn_rows_read_limit_internal` is recorded when an internal transaction tries to
read more rows than cluster setting `sql.defaults.transaction_rows_read_log`
or `sql.defaults.transaction_rows_read_err`. There will only be a single
record for a single transaction (unless it is retried) even if there are more
mutation statements within the transaction that haven't been executed yet.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `TxnID` | TxnID is the ID of the transaction that hit the row count limit. | no |
| `SessionID` | SessionID is the ID of the session that initiated the transaction. | no |
| `NumRows` | NumRows is the number of rows written/read (depending on the event type) by the transaction that reached the corresponding guardrail. | no |

### `txn_rows_written_limit_internal`

An event of type `txn_rows_written_limit_internal` is recorded when an internal transaction tries to
write more rows than cluster setting
`sql.defaults.transaction_rows_written_log` or
`sql.defaults.transaction_rows_written_err`. There will only be a single
record for a single transaction (unless it is retried) even if there are more
mutation statements within the transaction that haven't been executed yet.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `TxnID` | TxnID is the ID of the transaction that hit the row count limit. | no |
| `SessionID` | SessionID is the ID of the session that initiated the transaction. | no |
| `NumRows` | NumRows is the number of rows written/read (depending on the event type) by the transaction that reached the corresponding guardrail. | no |

## SQL User and Role operations

Events in this category pertain to SQL statements that modify the
properties of users and roles.

They are relative to a particular SQL tenant.
In a multi-tenant setup, copies of DDL-related events are preserved
in each tenant's own `system.eventlog` table.

Events in this category are logged to the `USER_ADMIN` channel.


### `alter_role`

An event of type `alter_role` is recorded when a role is altered.


| Field | Description | Sensitive |
|--|--|--|
| `RoleName` | The name of the affected user/role. | yes |
| `Options` | The options set on the user/role. | no |
| `SetInfo` | Information corresponding to an ALTER ROLE SET statement. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

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
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `grant_role`

An event of type `grant_role` is recorded when a role is granted.


| Field | Description | Sensitive |
|--|--|--|
| `GranteeRoles` | The roles being granted to. | yes |
| `Members` | The roles being granted. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |

### `password_hash_converted`

An event of type `password_hash_converted` is recorded when the password credentials
are automatically converted server-side.


| Field | Description | Sensitive |
|--|--|--|
| `RoleName` | The name of the user/role whose credentials have been converted. | yes |
| `OldMethod` | The previous hash method. | no |
| `NewMethod` | The new hash method. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

## Storage telemetry events



Events in this category are logged to the `TELEMETRY` channel.


### `level_stats`

An event of type `level_stats` contains per-level statistics for an LSM.


| Field | Description | Sensitive |
|--|--|--|
| `Level` | level is the level ID in a LSM (e.g. level(L0) == 0, etc.) | no |
| `NumFiles` | num_files is the number of files in the level (gauge). | no |
| `SizeBytes` | size_bytes is the size of the level, in bytes (gauge). | no |
| `Score` | score is the compaction score of the level (gauge). | no |
| `BytesIn` | bytes_in is the number of bytes written to this level (counter). | no |
| `BytesIngested` | bytes_ingested is the number of bytes ingested into this level (counter). | no |
| `BytesMoved` | bytes_moved is the number of bytes moved into this level via a move-compaction (counter). | no |
| `BytesRead` | bytes_read is the number of bytes read from this level, during compactions (counter). | no |
| `BytesCompacted` | bytes_compacted is the number of bytes written to this level during compactions (counter). | no |
| `BytesFlushed` | bytes flushed is the number of bytes flushed to this level. This value is always zero for levels other than L0 (counter). | no |
| `TablesCompacted` | tables_compacted is the count of tables compacted into this level (counter). | no |
| `TablesFlushed` | tables_flushed is the count of tables flushed into this level (counter). | no |
| `TablesIngested` | tables_ingested is the count of tables ingested into this level (counter). | no |
| `TablesMoved` | tables_moved is the count of tables moved into this level via move-compactions (counter). | no |
| `NumSublevels` | num_sublevel is the count of sublevels for the level. This value is always zero for levels other than L0 (gauge). | no |



### `store_stats`

An event of type `store_stats` contains per store stats.

Note that because stats are scoped to the lifetime of the process, counters
(and certain gauges) will be reset across node restarts.


| Field | Description | Sensitive |
|--|--|--|
| `NodeId` | node_id is the ID of the node. | no |
| `StoreId` | store_id is the ID of the store. | no |
| `Levels` | levels is a nested message containing per-level statistics. | yes |
| `CacheSize` | cache_size is the size of the cache for the store, in bytes (gauge). | no |
| `CacheCount` | cache_count is the number of items in the cache (gauge). | no |
| `CacheHits` | cache_hits is the number of cache hits (counter). | no |
| `CacheMisses` | cache_misses is the number of cache misses (counter). | no |
| `CompactionCountDefault` | compaction_count_default is the count of default compactions (counter). | no |
| `CompactionCountDeleteOnly` | compaction_count_delete_only is the count of delete-only compactions (counter). | no |
| `CompactionCountElisionOnly` | compaction_count_elision_only is the count of elision-only compactions (counter). | no |
| `CompactionCountMove` | compaction_count_move is the count of move-compactions (counter). | no |
| `CompactionCountRead` | compaction_count_read is the count of read-compactions (counter). | no |
| `CompactionCountRewrite` | compaction_count_rewrite is the count of rewrite-compactions (counter). | no |
| `CompactionNumInProgress` | compactions_num_in_progress is the number of compactions in progress (gauge). | no |
| `CompactionMarkedFiles` | compaction_marked_files is the count of files marked for compaction (gauge). | no |
| `FlushCount` | flush_count is the number of flushes (counter). | no |
| `FlushIngestCount` |  | no |
| `FlushIngestTableCount` |  | no |
| `FlushIngestTableBytes` |  | no |
| `IngestCount` | ingest_count is the number of successful ingest operations (counter). | no |
| `MemtableSize` | memtable_size is the total size allocated to all memtables and (large) batches, in bytes (gauge). | no |
| `MemtableCount` | memtable_count is the count of memtables (gauge). | no |
| `MemtableZombieCount` | memtable_zombie_count is the count of memtables no longer referenced by the current DB state, but still in use by an iterator (gauge). | no |
| `MemtableZombieSize` | memtable_zombie_size is the size, in bytes, of all zombie memtables (gauge). | no |
| `WalLiveCount` | wal_live_count is the count of live WAL files (gauge). | no |
| `WalLiveSize` | wal_live_size is the size, in bytes, of live data in WAL files. With WAL recycling, this value is less than the actual on-disk size of the WAL files (gauge). | no |
| `WalObsoleteCount` | wal_obsolete_count is the count of obsolete WAL files (gauge). | no |
| `WalObsoleteSize` | wal_obsolete_size is the size of obsolete WAL files, in bytes (gauge). | no |
| `WalPhysicalSize` | wal_physical_size is the size, in bytes, of the WAL files on disk (gauge). | no |
| `WalBytesIn` | wal_bytes_in is the number of logical bytes written to the WAL (counter). | no |
| `WalBytesWritten` | wal_bytes_written is the number of bytes written to the WAL (counter). | no |
| `TableObsoleteCount` | table_obsolete_count is the number of tables which are no longer referenced by the current DB state or any open iterators (gauge). | no |
| `TableObsoleteSize` | table_obsolete_size is the size, in bytes, of obsolete tables (gauge). | no |
| `TableZombieCount` | table_zombie_count is the number of tables no longer referenced by the current DB state, but are still in use by an open iterator (gauge). | no |
| `TableZombieSize` | table_zombie_size is the size, in bytes, of zombie tables (gauge). | no |
| `RangeKeySetsCount` | range_key_sets_count is the approximate count of internal range key sets in the store. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

## Telemetry events



Events in this category are logged to the `TELEMETRY` channel.


### `captured_index_usage_stats`

An event of type `captured_index_usage_stats`


| Field | Description | Sensitive |
|--|--|--|
| `TotalReadCount` | TotalReadCount is the number of times the index has been read. | no |
| `LastRead` | LastRead is the timestamp at which the index was last read. | no |
| `TableID` | TableID is the ID of the table on which the index was created. This is same as descpb.TableID and is unique within the cluster. | no |
| `IndexID` | IndexID is the ID of the index within the scope of the given table. | no |
| `DatabaseName` | DatabaseName is the name of the database in which the index was created. | no |
| `TableName` | TableName is the name of the table on which the index was created. | no |
| `IndexName` | IndexName is the name of the index within the scope of the given table. | no |
| `IndexType` | IndexType is the type of the index. Index types include "primary" and "secondary". | no |
| `IsUnique` | IsUnique indicates if the index has a UNIQUE constraint. | no |
| `IsInverted` | IsInverted indicates if the index is an inverted index. | no |
| `CreatedAt` | CreatedAt is the timestamp at which the index was created. | no |
| `SchemaName` | SchemaName is the name of the schema in which the index was created. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `hot_ranges_stats`

An event of type `hot_ranges_stats`


| Field | Description | Sensitive |
|--|--|--|
| `RangeID` |  | no |
| `Qps` |  | no |
| `SchemaName` | SchemaName is the name of the schema in which the index was created. | yes |
| `LeaseholderNodeID` | LeaseholderNodeID indicates the Node ID that is the current leaseholder for the given range. | no |
| `WritesPerSecond` | Writes per second is the recent number of keys written per second on this range. | no |
| `ReadsPerSecond` | Reads per second is the recent number of keys read per second on this range. | no |
| `WriteBytesPerSecond` | Write bytes per second is the recent number of bytes written per second on this range. | no |
| `ReadBytesPerSecond` | Read bytes per second is the recent number of bytes read per second on this range. | no |
| `CPUTimePerSecond` | CPU time per second is the recent cpu usage in nanoseconds of this range. | no |
| `Databases` | Databases for the range. | yes |
| `Tables` | Tables for the range | yes |
| `Indexes` | Indexes for the range | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `m_v_c_c_iterator_stats`

Internal storage iteration statistics for a single execution.


| Field | Description | Sensitive |
|--|--|--|
| `StepCount` | StepCount collects the number of times the iterator moved forward or backward over the DB's underlying storage keyspace. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `StepCountInternal` | StepCountInternal collects the number of times the iterator moved forward or backward over LSM internal keys. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `SeekCount` | SeekCount collects the number of times the iterator moved to a specific key/value pair in the DB's underlying storage keyspace. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `SeekCountInternal` | SeekCountInternal collects the number of times the iterator moved to a specific LSM internal key. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `BlockBytes` | BlockBytes collects the bytes in the loaded SSTable data blocks. For details, see pebble.InternalIteratorStats. | no |
| `BlockBytesInCache` | BlockBytesInCache collects the subset of BlockBytes in the block cache. For details, see pebble.InternalIteratorStats. | no |
| `KeyBytes` | KeyBytes collects the bytes in keys that were iterated over. For details, see pebble.InternalIteratorStats. | no |
| `ValueBytes` | ValueBytes collects the bytes in values that were iterated over. For details, see pebble.InternalIteratorStats. | no |
| `PointCount` | PointCount collects the count of point keys iterated over. For details, see pebble.InternalIteratorStats. | no |
| `PointsCoveredByRangeTombstones` | PointsCoveredByRangeTombstones collects the count of point keys that were iterated over that were covered by range tombstones. For details, see pebble.InternalIteratorStats and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `RangeKeyCount` | RangeKeyCount collects the count of range keys encountered during iteration. For details, see pebble.RangeKeyIteratorStats and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `RangeKeyContainedPoints` | RangeKeyContainedPoints collects the count of point keys encountered within the bounds of a range key. For details, see pebble.RangeKeyIteratorStats and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `RangeKeySkippedPoints` | RangeKeySkippedPoints collects the count of the subset of ContainedPoints point keys that were skipped during iteration due to range-key masking. For details, see pkg/storage/engine.go, pebble.RangeKeyIteratorStats, and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |



### `recovery_event`

An event of type `recovery_event` is an event that is logged on every invocation of BACKUP,
RESTORE, and on every BACKUP schedule creation, with the appropriate subset
of fields populated depending on the type of event. This event is is also
logged whenever a BACKUP and RESTORE job completes or fails.


| Field | Description | Sensitive |
|--|--|--|
| `RecoveryType` | RecoveryType is the type of recovery described by this event, which is one of - backup - scheduled_backup - create_schedule - restore<br><br>It can also be a job event corresponding to the recovery, which is one of - backup_job - scheduled_backup_job - restore_job | no |
| `TargetScope` | TargetScope is the largest scope of the targets that the user is backing up or restoring based on the following order: table < schema < database < full cluster. | no |
| `IsMultiregionTarget` | IsMultiregionTarget is true if any of the targets contain objects with multi-region primitives. | no |
| `TargetCount` | TargetCount is the number of targets the in the BACKUP/RESTORE. | no |
| `DestinationSubdirType` | DestinationSubdirType is - latest: if using the latest subdir - standard: if using a date-based subdir - custom: if using a custom subdir that's not date-based | no |
| `DestinationStorageTypes` | DestinationStorageTypes are the types of storage that the user is backing up to or restoring from. | no |
| `DestinationAuthTypes` | DestinationAuthTypes are the types of authentication methods that the user is using to access the destination storage. | no |
| `IsLocalityAware` | IsLocalityAware indicates if the BACKUP or RESTORE is locality aware. | no |
| `AsOfInterval` | AsOfInterval is the time interval in nanoseconds between the statement timestamp and the timestamp resolved by the AS OF SYSTEM TIME expression. The interval is expressed in nanoseconds. | no |
| `WithRevisionHistory` | WithRevisionHistory is true if the BACKUP includes revision history. | no |
| `HasEncryptionPassphrase` | HasEncryptionPassphrase is true if the user provided an encryption passphrase to encrypt/decrypt their backup. | no |
| `KMSType` | KMSType is the type of KMS the user is using to encrypt/decrypt their backup. | no |
| `KMSCount` | KMSCount is the number of KMS the user is using. | no |
| `Options` | Options contain all the names of the options specified by the user in the BACKUP or RESTORE statement. For options that are accompanied by a value, only those with non-empty values will be present.<br><br>It's important to note that there are no option values anywhere in the event payload. Future changes to telemetry should refrain from adding values to the payload unless they are properly redacted. | no |
| `DebugPauseOn` | DebugPauseOn is the type of event that the restore should pause on for debugging purposes. Currently only "error" is supported. | no |
| `JobID` | JobID is the ID of the BACKUP/RESTORE job. | no |
| `ResultStatus` | ResultStatus indicates whether the job succeeded or failed. | no |
| `ErrorText` | ErrorText is the text of the error that caused the job to fail. | partially |
| `RecurringCron` | RecurringCron is the crontab for the incremental backup. | no |
| `FullBackupCron` | FullBackupCron is the crontab for the full backup. | no |
| `CustomFirstRunTime` | CustomFirstRunTime is the timestamp for the user configured first run time. Expressed as nanoseconds since the Unix epoch. | no |
| `OnExecutionFailure` | OnExecutionFailure describes the desired behavior if the schedule fails to execute. | no |
| `OnPreviousRunning` | OnPreviousRunning describes the desired behavior if the previously scheduled BACKUP is still running. | no |
| `IgnoreExistingBackup` | IgnoreExistingBackup is true iff the BACKUP schedule should still be created even if a backup is already present in its destination. | no |
| `ApplicationName` | The application name for the session where recovery event was created. | no |
| `NumRows` | NumRows is the number of rows successfully imported, backed up or restored. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `sampled_exec_stats`

An event of type `sampled_exec_stats` contains execution statistics that apply to both statements
and transactions. These stats as a whole are collected using a sampling approach.
These exec stats are meant to contain the same fields as ExecStats in
apps_stats.proto but are for a single execution rather than aggregated executions.
Fields in this struct should be updated in sync with apps_stats.proto.


| Field | Description | Sensitive |
|--|--|--|
| `NetworkBytes` | NetworkBytes collects the number of bytes sent over the network. | no |
| `MaxMemUsage` | MaxMemUsage collects the maximum memory usage that occurred on a node. | no |
| `ContentionTime` | ContentionTime collects the time in seconds statements in the transaction spent contending. | no |
| `NetworkMessages` | NetworkMessages collects the number of messages that were sent over the network. | no |
| `MaxDiskUsage` | MaxDiskUsage collects the maximum temporary disk usage that occurred. This is set in cases where a query had to spill to disk, e.g. when performing a large sort where not all of the tuples fit in memory. | no |
| `CPUSQLNanos` | CPUSQLNanos collects the CPU time spent executing SQL operations in nanoseconds. Currently, it is only collected for statements without mutations that have a vectorized plan. | no |
| `MVCCIteratorStats` | Internal storage iteration statistics. | yes |



### `sampled_query`

An event of type `sampled_query` is the SQL query event logged to the telemetry channel. It
contains common SQL event/execution details.


| Field | Description | Sensitive |
|--|--|--|
| `SkippedQueries` | skipped_queries indicate how many SQL statements were not considered for sampling prior to this one. If the field is omitted, or its value is zero, this indicates that no statement was omitted since the last event. | no |
| `CostEstimate` | Cost of the query as estimated by the optimizer. | no |
| `Distribution` | The distribution of the DistSQL query plan (local, full, or partial). | no |
| `PlanGist` | The query's plan gist bytes as a base64 encoded string. | no |
| `SessionID` | SessionID is the ID of the session that initiated the query. | no |
| `Database` | Name of the database that initiated the query. | no |
| `StatementID` | Statement ID of the query. | no |
| `TransactionID` | Transaction ID of the query. | no |
| `MaxFullScanRowsEstimate` | Maximum number of rows scanned by a full scan, as estimated by the optimizer. | no |
| `TotalScanRowsEstimate` | Total number of rows read by all scans in the query, as estimated by the optimizer. | no |
| `OutputRowsEstimate` | The number of rows output by the query, as estimated by the optimizer. | no |
| `StatsAvailable` | Whether table statistics were available to the optimizer when planning the query. | no |
| `NanosSinceStatsCollected` | The maximum number of nanoseconds that have passed since stats were collected on any table scanned by this query. | no |
| `BytesRead` | The number of bytes read from disk. | no |
| `RowsRead` | The number of rows read from disk. | no |
| `RowsWritten` | The number of rows written. | no |
| `InnerJoinCount` | The number of inner joins in the query plan. | no |
| `LeftOuterJoinCount` | The number of left (or right) outer joins in the query plan. | no |
| `FullOuterJoinCount` | The number of full outer joins in the query plan. | no |
| `SemiJoinCount` | The number of semi joins in the query plan. | no |
| `AntiJoinCount` | The number of anti joins in the query plan. | no |
| `IntersectAllJoinCount` | The number of intersect all joins in the query plan. | no |
| `ExceptAllJoinCount` | The number of except all joins in the query plan. | no |
| `HashJoinCount` | The number of hash joins in the query plan. | no |
| `CrossJoinCount` | The number of cross joins in the query plan. | no |
| `IndexJoinCount` | The number of index joins in the query plan. | no |
| `LookupJoinCount` | The number of lookup joins in the query plan. | no |
| `MergeJoinCount` | The number of merge joins in the query plan. | no |
| `InvertedJoinCount` | The number of inverted joins in the query plan. | no |
| `ApplyJoinCount` | The number of apply joins in the query plan. | no |
| `ZigZagJoinCount` | The number of zig zag joins in the query plan. | no |
| `ContentionNanos` | The duration of time in nanoseconds that the query experienced contention. | no |
| `Regions` | The regions of the nodes where SQL processors ran. | no |
| `NetworkBytesSent` | The number of network bytes sent by nodes for this query. | no |
| `MaxMemUsage` | The maximum amount of memory usage by nodes for this query. | no |
| `MaxDiskUsage` | The maximum amount of disk usage by nodes for this query. | no |
| `KVBytesRead` | The number of bytes read at the KV layer for this query. | no |
| `KVPairsRead` | The number of key-value pairs read at the KV layer for this query. | no |
| `KVRowsRead` | The number of rows read at the KV layer for this query. | no |
| `NetworkMessages` | The number of network messages sent by nodes for this query. | no |
| `IndexRecommendations` | Generated index recommendations for this query. | no |
| `ScanCount` | The number of scans in the query plan. | no |
| `ScanWithStatsCount` | The number of scans using statistics (including forecasted statistics) in the query plan. | no |
| `ScanWithStatsForecastCount` | The number of scans using forecasted statistics in the query plan. | no |
| `TotalScanRowsWithoutForecastsEstimate` | Total number of rows read by all scans in the query, as estimated by the optimizer without using forecasts. | no |
| `NanosSinceStatsForecasted` | The greatest quantity of nanoseconds that have passed since the forecast time (or until the forecast time, if it is in the future, in which case it will be negative) for any table with forecasted stats scanned by this query. | no |
| `Indexes` | The list of indexes used by this query. | no |
| `CpuTimeNanos` | Collects the cumulative CPU time spent executing SQL operations in nanoseconds. Currently, it is only collected for statements without mutations that have a vectorized plan. | no |
| `KvGrpcCalls` | The number of grpc calls done to get data form KV nodes | no |
| `KvTimeNanos` | Cumulated time spent waiting for a KV request. This includes disk IO time and potentially network time (if any of the keys are not local). | no |
| `ServiceLatencyNanos` | The time to service the query, from start of parse to end of execute. | no |
| `OverheadLatencyNanos` | The difference between service latency and the sum of parse latency + plan latency + run latency . | no |
| `RunLatencyNanos` | The time to run the query and fetch or compute the result rows. | no |
| `PlanLatencyNanos` | The time to transform the AST into a logical query plan. | no |
| `IdleLatencyNanos` | The time between statement executions in a transaction | no |
| `ParseLatencyNanos` | The time to transform the SQL string into an abstract syntax tree (AST). | no |
| `MvccStepCount` | StepCount collects the number of times the iterator moved forward or backward over the DB's underlying storage keyspace. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `MvccStepCountInternal` | StepCountInternal collects the number of times the iterator moved forward or backward over LSM internal keys. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `MvccSeekCount` | SeekCount collects the number of times the iterator moved to a specific key/value pair in the DB's underlying storage keyspace. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `MvccSeekCountInternal` | SeekCountInternal collects the number of times the iterator moved to a specific LSM internal key. For details, see pkg/storage/engine.go and pkg/sql/opt/exec/factory.go. | no |
| `MvccBlockBytes` | BlockBytes collects the bytes in the loaded SSTable data blocks. For details, see pebble.InternalIteratorStats. | no |
| `MvccBlockBytesInCache` | BlockBytesInCache collects the subset of BlockBytes in the block cache. For details, see pebble.InternalIteratorStats. | no |
| `MvccKeyBytes` | KeyBytes collects the bytes in keys that were iterated over. For details, see pebble.InternalIteratorStats. | no |
| `MvccValueBytes` | ValueBytes collects the bytes in values that were iterated over. For details, see pebble.InternalIteratorStats. | no |
| `MvccPointCount` | PointCount collects the count of point keys iterated over. For details, see pebble.InternalIteratorStats. | no |
| `MvccPointsCoveredByRangeTombstones` | PointsCoveredByRangeTombstones collects the count of point keys that were iterated over that were covered by range tombstones. For details, see pebble.InternalIteratorStats and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `MvccRangeKeyCount` | RangeKeyCount collects the count of range keys encountered during iteration. For details, see pebble.RangeKeyIteratorStats and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `MvccRangeKeyContainedPoints` | RangeKeyContainedPoints collects the count of point keys encountered within the bounds of a range key. For details, see pebble.RangeKeyIteratorStats and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `MvccRangeKeySkippedPoints` | RangeKeySkippedPoints collects the count of the subset of ContainedPoints point keys that were skipped during iteration due to range-key masking. For details, see pkg/storage/engine.go, pebble.RangeKeyIteratorStats, and https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md. | no |
| `SchemaChangerMode` | SchemaChangerMode is the mode that was used to execute the schema change, if any. | no |
| `SQLInstanceIDs` | SQLInstanceIDs is a list of all the SQL instances used in this statement's execution. | no |
| `KVNodeIDs` | KVNodeIDs is a list of all the KV nodes used in this statement's execution. | no |
| `StatementFingerprintID` | Statement fingerprint ID of the query. | no |
| `UsedFollowerRead` | UsedFollowerRead indicates whether at least some reads were served by the follower replicas. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `ExecMode` | How the statement was being executed (exec/prepare, etc.) | no |
| `NumRows` | Number of rows returned. For mutation statements (INSERT, etc) that do not produce result rows, this field reports the number of rows affected. | no |
| `SQLSTATE` | The SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | The text of the error if any. | partially |
| `Age` | Age of the query in milliseconds. | no |
| `NumRetries` | Number of retries, when the txn was reretried automatically by the server. | no |
| `FullTableScan` | Whether the query contains a full table scan. | no |
| `FullIndexScan` | Whether the query contains a full secondary index scan of a non-partial index. | no |
| `TxnCounter` | The sequence number of the SQL transaction inside its session. | no |
| `BulkJobId` | The job id for bulk job (IMPORT/BACKUP/RESTORE). | no |
| `StmtPosInTxn` | The statement's index in the transaction, starting at 1. | no |

### `sampled_transaction`

An event of type `sampled_transaction` is the event logged to telemetry at the end of transaction execution.


| Field | Description | Sensitive |
|--|--|--|
| `User` | User is the user account that triggered the transaction. The special usernames `root` and `node` are not considered sensitive. | depends |
| `ApplicationName` | ApplicationName is the application name for the session where the transaction was executed. This is included in the event to ease filtering of logging output by application. | no |
| `TxnCounter` | TxnCounter is the sequence number of the SQL transaction inside its session. | no |
| `SessionID` | SessionID is the ID of the session that initiated the transaction. | no |
| `TransactionID` | TransactionID is the id of the transaction. | no |
| `Committed` | Committed indicates if the transaction committed successfully. We want to include this value even if it is false. | no |
| `ImplicitTxn` | ImplicitTxn indicates if the transaction was an implicit one. We want to include this value even if it is false. | no |
| `StartTimeUnixNanos` | StartTimeUnixNanos is the time the transaction was started. Expressed as unix time in nanoseconds. | no |
| `EndTimeUnixNanos` | EndTimeUnixNanos the time the transaction finished (either committed or aborted). Expressed as unix time in nanoseconds. | no |
| `ServiceLatNanos` | ServiceLatNanos is the time to service the whole transaction, from start to end of execution. | no |
| `SQLSTATE` | SQLSTATE is the SQLSTATE code for the error, if an error was encountered. Empty/omitted if no error. | no |
| `ErrorText` | ErrorText is the text of the error if any. | partially |
| `NumRetries` | NumRetries is the number of time when the txn was retried automatically by the server. | no |
| `LastAutoRetryReason` | LastAutoRetryReason is a string containing the reason for the last automatic retry. | partially |
| `NumRows` | NumRows is the total number of rows returned across all statements. | no |
| `RetryLatNanos` | RetryLatNanos is the amount of time spent retrying the transaction. | no |
| `CommitLatNanos` | CommitLatNanos is the amount of time spent committing the transaction after all statement operations. | no |
| `IdleLatNanos` | IdleLatNanos is the amount of time spent waiting for the client to send statements while the transaction is open. | no |
| `BytesRead` | BytesRead is the number of bytes read from disk. | no |
| `RowsRead` | RowsRead is the number of rows read from disk. | no |
| `RowsWritten` | RowsWritten is the number of rows written to disk. | no |
| `SampledExecStats` | SampledExecStats is a nested field containing execution statistics. This field will be omitted if the stats were not sampled. | yes |
| `SkippedTransactions` | SkippedTransactions is the number of transactions that were skipped as part of sampling prior to this one. We only count skipped transactions when telemetry logging is enabled and the sampling mode is set to "transaction". | no |
| `TransactionFingerprintID` | TransactionFingerprintID is the fingerprint ID of the transaction. This can be used to find the transaction in the console. | no |
| `StatementFingerprintIDs` | StatementFingerprintIDs is an array of statement fingerprint IDs belonging to this transaction. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `schema_descriptor`

An event of type `schema_descriptor` is an event for schema telemetry, whose purpose is
to take periodic snapshots of the cluster's SQL schema and publish them in
the telemetry log channel. For all intents and purposes, the data in such a
snapshot can be thought of the outer join of certain system tables:
namespace, descriptor, and at some point perhaps zones, etc.

Snapshots are too large to conveniently be published as a single log event,
so instead they're broken down into SchemaDescriptor events which
contain the data in one record of this outer join projection. These events
are prefixed by a header (a SchemaSnapshotMetadata event).


| Field | Description | Sensitive |
|--|--|--|
| `SnapshotID` | SnapshotID is the unique identifier of the snapshot that this event is part of. | no |
| `ParentDatabaseID` | ParentDatabaseID matches the same key column in system.namespace. | no |
| `ParentSchemaID` | ParentSchemaID matches the same key column in system.namespace. | no |
| `Name` | Name matches the same key column in system.namespace. | no |
| `DescID` | DescID matches the 'id' column in system.namespace and system.descriptor. | no |
| `Desc` | Desc matches the 'descriptor' column in system.descriptor. Some contents of the descriptor may be redacted to prevent leaking PII. | no |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

### `schema_snapshot_metadata`

An event of type `schema_snapshot_metadata` is an event describing a schema snapshot, which
is a set of SchemaDescriptor messages sharing the same SnapshotID.


| Field | Description | Sensitive |
|--|--|--|
| `SnapshotID` | SnapshotID is the unique identifier of this snapshot. | no |
| `NumRecords` | NumRecords is how many SchemaDescriptor events are in the snapshot. | no |
| `AsOfTimestamp` | AsOfTimestamp is when the snapshot was taken. This is equivalent to the timestamp given in the AS OF SYSTEM TIME clause when querying the namespace and descriptor tables in the system database. Expressed as nanoseconds since the Unix epoch. | no |
| `Errors` | Errors records any errors encountered when post-processing this snapshot, which includes the redaction of any potential PII. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |

## Zone config events

Events in this category pertain to zone configuration changes on
the SQL schema or system ranges.

When zone configs apply to individual tables or other objects in a
SQL logical schema, they are relative to a particular SQL tenant.
In a multi-tenant setup, copies of these zone config events are preserved
in each tenant's own `system.eventlog` table.

When they apply to cluster-level ranges (e.g., the system zone config),
they are stored in the system tenant's own `system.eventlog` table.

Events in this category are logged to the `OPS` channel.


### `remove_zone_config`

An event of type `remove_zone_config` is recorded when a zone config is removed.




#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `Target` | The target object of the zone config change. | yes |
| `Config` | The applied zone config in YAML format. | yes |
| `Options` | The SQL representation of the applied zone config options. | yes |

### `set_zone_config`

An event of type `set_zone_config` is recorded when a zone config is changed.


| Field | Description | Sensitive |
|--|--|--|
| `ResolvedOldConfig` | The string representation of the resolved old zone config. This is not necessarily the same as the zone config that was previously set -- as it includes the resolved values of the zone config options. In other words, a zone config that hasn't been properly "set" yet (and inherits from its parent) will have a resolved_old_config that has details of the values it inherits from its parent. This is particularly useful to get a proper diff between the old and new zone config. | yes |


#### Common fields

| Field | Description | Sensitive |
|--|--|--|
| `Timestamp` | The timestamp of the event. Expressed as nanoseconds since the Unix epoch. | no |
| `EventType` | The type of the event. | no |
| `Statement` | A normalized copy of the SQL statement that triggered the event. The statement string contains a mix of sensitive and non-sensitive details (it is redactable). | partially |
| `Tag` | The statement tag. This is separate from the statement string, since the statement string can contain sensitive information. The tag is guaranteed not to. | no |
| `User` | The user account that triggered the event. The special usernames `root` and `node` are not considered sensitive. | depends |
| `DescriptorID` | The primary object descriptor affected by the operation. Set to zero for operations that don't affect descriptors. | no |
| `ApplicationName` | The application name for the session where the event was emitted. This is included in the event to ease filtering of logging output by application. | no |
| `PlaceholderValues` | The mapping of SQL placeholders to their values, for prepared statements. | yes |
| `Target` | The target object of the zone config change. | yes |
| `Config` | The applied zone config in YAML format. | yes |
| `Options` | The SQL representation of the applied zone config options. | yes |




## Enumeration types

### `AuthFailReason`

AuthFailReason is the inventory of possible reasons for an
authentication failure.


| Value | Textual alias in code or documentation | Description |
|--|--|--|
| 0 | UNKNOWN | is reported when the reason is unknown. |
| 1 | USER_RETRIEVAL_ERROR | occurs when there was an internal error accessing the principals. |
| 2 | USER_NOT_FOUND | occurs when the principal is unknown. |
| 3 | LOGIN_DISABLED | occurs when the user does not have LOGIN privileges. |
| 4 | METHOD_NOT_FOUND | occurs when no HBA rule matches or the method does not exist. |
| 5 | PRE_HOOK_ERROR | occurs when the authentication handshake encountered a protocol error. |
| 6 | CREDENTIALS_INVALID | occurs when the client-provided credentials were invalid. |
| 7 | CREDENTIALS_EXPIRED | occur when the credentials provided by the client are expired. |
| 8 | NO_REPLICATION_ROLEOPTION | occurs when the connection requires a replication role option, but the user does not have it. |
| 9 | AUTHORIZATION_ERROR | is used for errors during the authorization phase. For example, this would include issues with mapping LDAP groups to SQL roles and granting those roles to the user. |



