# Debugging Roachtest Job Failures

Roachtests that run specific jobs verify both successful completion and correct behavior, including
performance characteristics. This guide provides a systematic approach to investigating job-related
test failures.

## Find the Job ID

Identifying the specific job ID is critical for investigation. If not found in test.log, search 
`debug/system.jobs.txt` for the relevant job type:
- `c2c/*` (cluster-to-cluster) → `STREAM INGESTION` (in `dest_debug`).
- `ldr/*` → `LOGICAL REPLICATION`.
- `cdc/*`, `changefeed/*` → `CHANGEFEED`.
- `backup/*` → `BACKUP`, `restore/*` → `RESTORE`.
- `import/*` → `IMPORT`.
- `schemachange/*` → `SCHEMA CHANGE`.

Note for c2c tests: The cluster is split into source and destination clusters, with separate debug 
folders `source_debug` and `dest_debug/`.

Verify the job is correctly identified before proceeding:
- If multiple jobs are found: Stop and ask which job ID is relevant for this test failure.
- If no jobs are found: Stop and ask if this test involves a different job type not listed above.
- Record the identified job ID for reference throughout the investigation.

## General Investigation Approach

Document all observations with citations and references to their locations in the test output. 
Create an investigation.md file to track findings systematically.

### Collect job-related logs

Create a focused, combined log containing all entries from all nodes that mention the job ID:

```bash
grep -h "JOB_ID" PATH/TO/FETCHED/artifacts/logs/*.unredacted/cockroach.log | cut -c2- | sort > job_JOB_ID.log
```
NB: Use the unredacted logs, not the redacted ones in artifacts/logs/*.cockroach.log or you may be 
missing details.

Review these logs for unusual patterns, error messages, retries, notable events and their 
timestamps.

### Review system tables

Examine these system tables for job-specific information:

- `debug/crdb_internal.jobs.txt`: job status and errors.
  - Error messages are almost always significant.
  - Start and end times help build a timeline.
- `debug/system.job_message.txt`: status messages recorded by the job.
  - Include relevant messages in your investigation timeline.
- `debug/system.job_progress_history.txt`: historical progress values sorted by time.
  - Some jobs track "fraction_completed" progress; others use "resolved" timestamp progress.
  - Jobs may switch between progress types during execution.
  - Analyze progress patterns: is it linear? does it stall? are there inflection points?
  - For jobs that timeout or get stuck, use `./scripts/job-progress-plot <job-id> <path>` to 
    generate an ASCII plot and insert it in your investigation document below the timeline.
  - Identify periods with different progress rates and document them in your timeline:
    - Sudden changes: X%/min immediately shifts to Y%/min at time T
    - Gradual changes: progress slows from X%/min at time T1 to Y%/min by time T2 over N minutes
  - When rate changes occur (sudden or gradual), investigate what else happened around those 
    time periods and document correlations in the timeline.
  - Compare before/after periods: examine node distribution, retry patterns, flow control 
    blocking, and infrastructure events to identify what differs between fast and slow periods.
- `debug/jobs/<jobID>/*` sometimes contains zipped up job trace data.

Note: C2C tests have separate system tables for source and destination clusters. The destination
cluster typically contains the most relevant events.

### Investigation strategies

- Never assume job success means correct behavior; tests exist because jobs can succeed while
  behaving incorrectly.
- If a job succeeds but the test fails, investigate both the job behavior and test logic.
- Retries and replannings:
  - Jobs with retry loops may "get stuck" if an underlying issue causes it to retry without making
    progress.
  - A series of retries without making progress is an indicator something is happening that might be
    wrong _even though the job might not fail outright_.
- Investigate progress stalls: When a job stops making progress, examine events around that time:
  - Increased retry attempts.
  - Logged errors.
  - Node failures or restarts.
  - KV range splits or other cluster events?
- If you find yourself digging into a slow job, look for a job trace zip.
  - Combined trace has count of and cumulative time spent per operation.
  - Note: many jobs run many processors, each with many workers that perform operations
    concurrently; cumulative time often exceeds wall time.
  - For distributed jobs: coordination nodes aggregate cumulative times from all worker nodes - a
    single node showing 100+ hours may actually represent work distributed across many nodes.
- Many jobs utilize a "frontier" to track progress happening across many tasks, spans, ranges, etc.
  - The frontier tracks and reports the _furthest behind_ task or span, so if the "frontier is
    lagging" it means a span is behind, not that there is a problem with the frontier itself.

Timestamp handling: Timestamps appear in different formats:
- `<seconds>.<fractional-seconds>,<logical>`.
- `<nanoseconds>.<logical>`.

When searching logs, use only the higher (seconds) digits to match both formats. Maintain 
consistency in your notes by using one format throughout.

### Processor Start Messages

Most distributed jobs utilize "processors" that run on multiple nodes to execute work in parallel.

Analyzing processor distribution and behavior can be key to spotting issues in work distribution,
and can be a signal of replannings or retry loops if those aren't explicitly logged already.

Processor log messages are already in the job-focused log since they are tagged with the job ID.

Processor start messages indicate a job was planned or replanned: if this wasn't observed in the
logs, this is often worth noting in the timeline (along with if the start message is seen on all 
nodes or just some).

Sometimes these messages include a number of spans or work units assigned; if this is not relatively
balanced across nodes, or some nodes do not report starting processors when others do, this is 
notable.

### Data Writing Job Behavior

- Jobs which write data such as restore, import and schema changes are expected to perform many 
  splits, particularly when they begin.
- Heavy splitting during initial phases is normal and expected behavior.
- `SSTable cannot be added spanning range bounds` errors are automatically handled by retrying 
  each half of the SST.
- These errors are unlikely to be the cause of a failure and are usually benign.
- However, if a very high proportion of all SSTable requests hit spanning errors, it suggests the 
  SSTable adding process is hitting splits it did not expect, and can slow down the process of 
  adding SSTables.

## Job-Specific Investigation Approaches

This section is continuously evolving. If your job type isn't covered here:
- Ask for specific guidance on what to investigate.
- Document useful patterns you discover for future additions to this guide.

### Backup and Restore Jobs (backup/* and restore/* tests)

For BACKUP and RESTORE jobs, always record these key infrastructure details in your investigation:

- **Cloud provider**: (GCE, AWS, Azure, etc.).
- **Cluster region(s)/zone(s)**: Where the CockroachDB nodes are located.
- **Bucket location**: The cloud storage bucket region/location.
- **Cross-region setup**: Note if cluster and bucket are in different regions.

### Physical Replication Stream Ingestion Jobs (c2c/* tests)

Job phases:
1. Initial scan: copies all data as of the initial scan timestamp.
2. Replication: applies streamed changes, updating replicated time.
3. Cutover: rewinds applied changes to the chosen cutover timestamp (≤ replicated time).

Key terminology:
- Replicated time ("resolved timestamp" internally): highest time to which cutover is allowed.
- "Lag"/"latency": how far replicated time trails behind current time.
- Retained time: protected timestamp trailing resolved time; earliest cutover time allowed.

Expected behaviors:
- Initial scan phase: replicated time remains zero; lag shows as "thousands of years" (normal).
- Replication phase:
  - Replicated/resolved time should steadily increase, or "advance", when running normally.
  - If resolved time is increasing, lag remains small and constant (usually ~30 seconds).
    - Minutes or more of lag suggests replication is behind and trying to catch up.
    - High *and steadily increasing* lag suggests replication has stalled and may be stuck.
    - NB: constant lag == increasing resolved time; constant resolved time == increasing lag.
- Cutover phase: progress shows as fraction of rewind completed.
- Job completion: when cutover to the target timestamp finishes.

Replication components:
- Source side: stream producer (event_stream and rangefeeds).
- Destination side: stream ingestion job (stream ingestion processor, AddSSTable).

C2C-specific checklists:
1. Augment the investigation timeline to document:
   - Initial scan start/completion and the value of the initial scan timestamp.
   - Replication phase.
   - Cutover start/completion, and cutover timestamp.
   - Key replicated/resolved timestamp values around any notable events.
  Be sure to note these phases and what phase the job was in when the test failed.

2. Resolved progress and lag analysis:
  Look at the progress history, and logged resolved timestamps and logged lag observations:
  
  During replication phase:
    - Are there periods where lag increases?
    - Is resolved time advancing consistently or does it stall at a constant value for extended 
      periods?
    - Do stalls correlate with other cluster events or errors?
  
  Update the timeline with any observations, particularly periods of constant vs increasing lag, or 
  sudden changes in resolved time.

3. Component investigation: if replication stalls, examine both source and destination components for 
   issues.

4. Understanding frontier stalls: If resolved time stops advancing during replication:
   - The frontier algorithm takes the minimum timestamp across all spans.
   - A stalled frontier indicates one or more spans are stuck, not a coordination failure.
   - Look for patterns in which nodes are "lagging behind the frontier" - this indicates which spans 
     are stuck.
   - Check for replanning events: frequent replanning (search "replanning") suggests persistent stuck 
     spans.
   - Examine AdminSplit operations and SSTable retries on specific tables that correlate with stuck 
     spans.

5. Data validation: tests typically compare fingerprints between clusters.
   - Fingerprint mismatches indicate data corruption or replication errors.
   - "Start timestamp greater than end timestamp" errors mean cutover time was below retained time.
    - Start time, i.e. retained time, usually should be minutes, or hours, before end time.
    - Usually caused by extreme replication lag; re-examine the replication phase more closely.
