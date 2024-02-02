# Index recommendations on SQL Statistics
Last Update: February 2024
Original author: maryliag

This document provides an overview of how Index Recommendations are 
generated and stored on statement statistics.

Table of contents:

- [Overview](#overview)
- [Cache](#cache)
- [Cluster Settings](#cluster-settings)
- [Console](#console)

## Overview
When a statement is executed, we want to calculate index recommendations for it, in
case there are better indexes that can improve its execution. Generating an index
recommendation is a costly operation, so we don't want to generate it on each execution. 
At the same time, generating frequent index recommendations for the same plan would
likely generate the same results (unless there is a big increase/decrease in rows affected
by the query).
With this in mind, we use a cache to store the latest index recommendation for the most
recent statement fingerprints, avoiding a new generation on each execution.

The value of the recommendation can be found on the column `index_recommendation`
as a `STRING[] NOT NULL` on:
- `system.statement_statistics`
- `crdb_internal.statement_statistics`
- `crdb_internal_statement_statistics_persisted`

When recording a statement, it calls the function `ShouldGenerateIndexRecommendation`.
This function returns true if there was no index recommendation generated for the
statement fingerprint in the past hour and there was at least 5 executions 
(`minExecCount`) of it.
We don't generate index recommendations for statement fingerprints with 5 or fewer 
executions because we don't want to perform a heavy operation for a statement that
is barely executed.

Index recommendations are only generated for DML statements that are not internal.

It then calls `UpdateIndexRecommendations`. This function can make two types of updates:
1. A new index recommendation was generated: it updates the value on the cache and 
reset the last update time and the execution count.
2. No index recommendation was generated: increase the counter of execution count. The
counter is only increased if less than 5, since that is the count we care about. If the 
value is already greater than 5, no need to keep updating.

If a recommendation is generated for a new fingerprint, and we reached the limit
on the cache of how much we can store, it will try to remove any entries that have
the last update value older than 24hrs (`timeThresholdForDeletion`). It also needs 
to be at least 5 minutes (`timeBetweenCleanups`) between cleanups (to avoid cases where a lot of new fingerprints are created and could
cause contention on the cache)

## Cache
The Index Recommendation cache is a mutex map with corresponding info:
- Key (indexRecKey): stmtNoConstants, database, planHash
- Value (indexRecInfo): lastGeneratedTs, recommendations, executionCount

## Cluster Settings
There are 2 cluster settings controlling this system:
- `sql.metrics.statement_details.index_recommendation_collection.enabled`:
enable/disable if the system will generate index recommendations. This is a safety
measure in case there is a performance degradation on this feature, and it can be
disabled. Default value is `true`.
- `sql.metrics.statement_details.max_mem_reported_idx_recommendations`: 
defines the maximum number of reported index recommendations info we store in the
cache defined previously. Default value is `5000`.

## Console
This information can be seen in a few different places:
- Statement Details page (Explain Plan tab): on the table on the page, there is a 
column for `Insights`. If a row shows that there are insights for that particular
plan, when clicking on it, it will display the recommendation.
- Insights (Workload): Any Insights with type `Suboptimal Plan` can be selected and
the index recommendation will be displayed on its details page.
- Insights (Schema): It will list the latest index recommendation per fingerprint.

All options above will display a button to create/alter/drop the index directly 
from the Console UI.
