// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vtable

// IndexUsageStatistics describes the schema of the internal index_usage_statistics table.
const CRDBIndexUsageStatistics = `
CREATE TABLE information_schema.crdb_index_usage_statistics (
  table_id        INT NOT NULL,
  index_id        INT NOT NULL,
  total_reads     INT NOT NULL,
  last_read       TIMESTAMPTZ
)`

// CRDBNodeActiveSessionHistory describes the schema of the
// information_schema view that surfaces crdb_internal.node_active_session_history.
const CRDBNodeActiveSessionHistory = `
CREATE VIEW information_schema.crdb_node_active_session_history AS
    SELECT sample_time,
           node_id,
           tenant_id,
           workload_id,
           app_name,
           work_event_type,
           work_event,
           goroutine_id
    FROM crdb_internal.node_active_session_history`

// CRDBClusterActiveSessionHistory describes the schema of the
// information_schema view that surfaces crdb_internal.cluster_active_session_history.
const CRDBClusterActiveSessionHistory = `
CREATE VIEW information_schema.crdb_cluster_active_session_history AS
    SELECT sample_time,
           node_id,
           tenant_id,
           workload_id,
           app_name,
           work_event_type,
           work_event,
           goroutine_id
    FROM crdb_internal.cluster_active_session_history`
