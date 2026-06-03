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
           workload_type,
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
           workload_type,
           app_name,
           work_event_type,
           work_event,
           goroutine_id
    FROM crdb_internal.cluster_active_session_history`

// CRDBStatementStatistics describes the schema of the information_schema view
// that surfaces persisted statement statistics. It reads directly from
// system.statement_statistics joined to system.statements, filtered to exclude
// internal-app traffic. The query/query_summary/database COALESCEs fall back to
// the metadata JSONB when the system.statements row isn't present yet.
const CRDBStatementStatistics = `
CREATE VIEW information_schema.crdb_statement_statistics AS
    SELECT
        ss.aggregated_ts,
        ss.fingerprint_id,
        ss.transaction_fingerprint_id,
        ss.plan_hash,
        ss.app_name,
        ss.node_id,
        ss.agg_interval,
        ss.execution_count,
        ss.service_latency,
        ss.cpu_sql_nanos,
        ss.contention_time,
        ss.total_estimated_execution_time,
        ss.p99_latency,
        ss.exec_sample_count,
        ss.svc_lat_sum,
        ss.cpu_sql_nanos_sum,
        ss.contention_time_sum,
        ss.svc_lat_sum_sq,
        ss.cpu_sql_nanos_sum_sq,
        ss.contention_time_sum_sq,
        ss.kv_cpu_time_nanos_sum,
        ss.kv_cpu_time_nanos_sum_sq,
        ss.admission_wait_time_sum,
        ss.admission_wait_time_sum_sq,
        ss.rows_read_sum,
        ss.rows_written_sum,
        ss.bytes_read_sum,
        ss.bytes_read_sum_sq,
        ss.max_retries,
        COALESCE(s.fingerprint, ss.metadata->>'query', '')        AS query,
        COALESCE(s.summary,     ss.metadata->>'querySummary', '') AS query_summary,
        COALESCE(s.db,          ss.metadata->>'db', '')           AS database
    FROM
        system.statement_statistics AS ss
    LEFT JOIN
        system.statements AS s ON ss.fingerprint_id = s.fingerprint_id
    WHERE
        ss.app_name NOT LIKE '$ internal%'`

// CRDBTransactionStatistics describes the schema of the information_schema view
// that surfaces persisted transaction statistics. It reads directly from
// system.transaction_statistics, filtered to exclude internal-app traffic.
// stmt_fingerprint_ids is projected to BYTES[] (decoded from the hex-encoded
// JSONB array stored in metadata->'stmtFingerprintIDs'); elements are
// byte-identical to crdb_statement_statistics.fingerprint_id, so joining the
// two views requires unnest(stmt_fingerprint_ids) first.
const CRDBTransactionStatistics = `
CREATE VIEW information_schema.crdb_transaction_statistics AS
    SELECT
        aggregated_ts,
        fingerprint_id,
        app_name,
        node_id,
        agg_interval,
        execution_count,
        service_latency,
        cpu_sql_nanos,
        contention_time,
        total_estimated_execution_time,
        p99_latency,
        exec_sample_count,
        svc_lat_sum,
        cpu_sql_nanos_sum,
        contention_time_sum,
        svc_lat_sum_sq,
        cpu_sql_nanos_sum_sq,
        contention_time_sum_sq,
        kv_cpu_time_nanos_sum,
        kv_cpu_time_nanos_sum_sq,
        admission_wait_time_sum,
        admission_wait_time_sum_sq,
        rows_read_sum,
        rows_written_sum,
        bytes_read_sum,
        bytes_read_sum_sq,
        max_retries,
        commit_lat_sum,
        commit_lat_sum_sq,
        ARRAY(
            SELECT decode(elem, 'hex')
            FROM jsonb_array_elements_text(metadata->'stmtFingerprintIDs') AS elem
        ) AS stmt_fingerprint_ids
    FROM
        system.transaction_statistics
    WHERE
        app_name NOT LIKE '$ internal%'`

// CRDBJobs describes the schema of the information_schema view that exposes
// per-job metadata from system.jobs without the truncation or column omission
// that SHOW JOBS applies for human readability. The column set is intentionally
// scoped to what is useful to customers programmatically: internal execution
// detail (claim session/instance), retry bookkeeping (num_runs, last_run), and
// creator coupling (created_by_*) are deliberately excluded so the contract
// does not depend on those implementation choices.
const CRDBJobs = `
CREATE VIEW information_schema.crdb_jobs AS
    SELECT id                  AS job_id,
           job_type,
           owner,
           description,
           created::TIMESTAMPTZ AS created,
           finished,
           status               AS state,
           error_msg            AS error
    FROM system.public.jobs
    WHERE crdb_internal.can_view_job(owner)`

// CRDBJobsWithProgress describes the schema of the information_schema view
// that extends crdb_jobs with the current progress reading and the current
// user-visible status message. The `resolved` column is exposed as the raw
// HLC decimal it is stored as, matching the underlying system.job_progress
// column. Callers wanting a wall-clock timestamp can apply hlc_to_timestamp
// at the call site; exposing the raw HLC avoids baking a lossy conversion
// (loss of the HLC logical component) into the stable contract.
// `last_updated` collapses the job_progress.written and job_status.written
// timestamps into one column so the surface does not commit to the internal
// split between the two tables.
const CRDBJobsWithProgress = `
CREATE VIEW information_schema.crdb_jobs_with_progress AS
    SELECT j.id                              AS job_id,
           j.job_type,
           j.owner,
           j.description,
           j.created::TIMESTAMPTZ            AS created,
           j.finished,
           j.status                          AS state,
           j.error_msg                       AS error,
           p.fraction                        AS progress_fraction,
           p.resolved                        AS resolved,
           s.status                          AS status_message,
           greatest(p.written, s.written)    AS last_updated
    FROM system.public.jobs AS j
    LEFT OUTER JOIN system.public.job_progress AS p ON j.id = p.job_id
    LEFT OUTER JOIN system.public.job_status   AS s ON j.id = s.job_id
    WHERE crdb_internal.can_view_job(j.owner)`
