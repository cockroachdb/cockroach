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
