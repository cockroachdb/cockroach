# Session Variables

This page documents the session variables available in CockroachDB.

Session variables control the behavior of the current session and can be set using the `SET` statement. For example:

```sql
SET application_name = 'myapp';
```

To view the current value of a session variable, use `SHOW`:

```sql
SHOW application_name;
```

<table>
<thead><tr>
<th>Variable Name</th>
<th>Description</th>
<th>Default Value</th>
<th>Read-only</th>
</tr></thead>
<tbody>
<tr><td><code>allow_ordinal_column_references</code></td><td>Controls whether the deprecated ordinal column reference syntax (e.g., SELECT @1 FROM t) is allowed.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>allow_role_memberships_to_change_during_transaction</code></td><td>Controls whether operations consulting role membership cache retain their lease throughout the transaction.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>alter_primary_region_super_region_override</code></td><td>Controls whether the user can modify a primary region that is part of a super region.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>always_distribute_full_scans</code></td><td>Controls whether full table scans always force the plan to be distributed, regardless of the estimated row count.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>application_name</code></td><td>Sets the name of the application running the current session. Used for logging and per-application statistics.</td><td><code>-</code></td><td>No</td></tr>
<tr><td><code>authentication_method</code></td><td>The authentication method used for the session.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>autocommit_before_ddl</code></td><td>Causes any DDL statement received during a multi-statement transaction to auto-commit before executing.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>avoid_buffering</code></td><td>Indicates that the returned data should not be buffered by conn executor. This is currently used by replication primitives to ensure the data is flushed to the consumer immediately.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>avoid_full_table_scans_in_mutations</code></td><td>Controls whether mutation queries that plan full table scans should be avoided.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>backslash_quote</code></td><td>Controls whether backslash can be used as a quote escape character (compatibility setting).</td><td><code>safe_encoding</code></td><td>No</td></tr>
<tr><td><code>bypass_pcr_reader_catalog_aost</code></td><td>Disables the AOST used by all user queries on the PCR reader catalog.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>bytea_output</code></td><td>Controls how to encode byte arrays when converting to string.</td><td><code>hex</code></td><td>No</td></tr>
<tr><td><code>check_function_bodies</code></td><td>Controls whether functions are validated during function creation.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>client_encoding</code></td><td>Controls the client-side character encoding. Only UTF8 is supported.</td><td><code>UTF8</code></td><td>No</td></tr>
<tr><td><code>client_min_messages</code></td><td>Controls which message levels are sent to the client.</td><td><code>notice</code></td><td>No</td></tr>
<tr><td><code>close_cursors_at_commit</code></td><td>Determines whether cursors remain open after their parent transaction closes.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>copy_fast_path_enabled</code></td><td>Controls whether the optimized copy mode is enabled.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>copy_from_atomic_enabled</code></td><td>Controls whether implicit transaction COPY FROM operations are atomic or segmented.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>copy_from_retries_enabled</code></td><td>Controls whether retries should be internally attempted for retriable errors in COPY FROM operations.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>copy_num_retries_per_batch</code></td><td>Determines the number of times a single batch of rows can be retried for non-atomic COPY operations.</td><td><code>5</code></td><td>No</td></tr>
<tr><td><code>copy_transaction_quality_of_service</code></td><td>Sets the QoSLevel/WorkPriority of the transactions used to evaluate COPY commands.</td><td><code>background</code></td><td>No</td></tr>
<tr><td><code>copy_write_pipelining_enabled</code></td><td>Controls whether write pipelining is enabled for implicit transactions used by COPY.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>cost_scans_with_default_col_size</code></td><td>Controls whether the optimizer should cost scans and joins using a default number of bytes per column instead of column sizes from statistics.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>crdb_version</code></td><td>The version of CockroachDB.</td><td><code>CockroachDB OSS v25.2.0-alpha.00000000-dev (darwin arm64, built , go1.22.8 X:nocoverageredesign)</code></td><td>Yes</td></tr>
<tr><td><code>database</code></td><td>Sets the current database for resolving names in queries.</td><td><code>-</code></td><td>No</td></tr>
<tr><td><code>datestyle</code></td><td>Controls the display format for date and time values as well as the rules for interpreting ambiguous date inputs.</td><td><code>ISO, MDY</code></td><td>No</td></tr>
<tr><td><code>deadlock_timeout</code></td><td>Sets the amount of time to wait on a lock before checking for deadlock. If set to 0, there is no timeout.</td><td><code>0s</code></td><td>No</td></tr>
<tr><td><code>declare_cursor_statement_timeout_enabled</code></td><td>Controls whether statement timeouts apply during DECLARE CURSOR operations.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>default_int_size</code></td><td>Specifies the size in bits or bytes (preferred) of how the INT type should be parsed.</td><td><code>8</code></td><td>No</td></tr>
<tr><td><code>default_table_access_method</code></td><td>Sets the default table access method (compatibility setting).</td><td><code>heap</code></td><td>No</td></tr>
<tr><td><code>default_tablespace</code></td><td>Supported only for pg compatibility - CockroachDB has no notion of tablespaces.</td><td><code>-</code></td><td>No</td></tr>
<tr><td><code>default_text_search_config</code></td><td>Sets the default text search configuration used for builtins like to_tsvector and to_tsquery.</td><td><code>pg_catalog.english</code></td><td>No</td></tr>
<tr><td><code>default_transaction_isolation</code></td><td>Sets the transaction isolation level of new transactions.</td><td><code>serializable</code></td><td>No</td></tr>
<tr><td><code>default_transaction_priority</code></td><td>Sets the default priority of newly created transactions.</td><td><code>normal</code></td><td>No</td></tr>
<tr><td><code>default_transaction_quality_of_service</code></td><td>Sets the default QoSLevel/WorkPriority of newly created transactions.</td><td><code>regular</code></td><td>No</td></tr>
<tr><td><code>default_transaction_read_only</code></td><td>Sets whether new transactions are read-only by default.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>default_transaction_use_follower_reads</code></td><td>Sets whether new transactions use follower reads by default.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>default_with_oids</code></td><td>Controls whether new tables are created with OIDs by default (compatibility setting).</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>descriptor_validation</code></td><td>Controls whether descriptors are validated at read and write time, read time only, or never.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>direct_columnar_scans_enabled</code></td><td>Controls whether the COL_BATCH_RESPONSE scan format should be used for ScanRequests and ReverseScanRequests whenever possible.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>disable_changefeed_replication</code></td><td>Disables changefeed events from being emitted for data changes made in a session, applying to new transactions only.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>disable_hoist_projection_in_join_limitation</code></td><td>Disables the restrictions placed on projection hoisting during query planning in the optimizer.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>disable_partially_distributed_plans</code></td><td>Controls whether partially distributed plans are disabled.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>disable_plan_gists</code></td><td>Controls whether plan gists are disabled.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>disable_vec_union_eager_cancellation</code></td><td>Disables the eager cancellation that is performed by the vectorized engine when transitioning into the draining state in some cases.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>disallow_full_table_scans</code></td><td>Controls whether queries that plan full table scans should be rejected.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>distribute_group_by_row_count_threshold</code></td><td>Sets the minimum number of rows estimated to be processed by the GroupBy operator to distribute the plan.</td><td><code>1000</code></td><td>No</td></tr>
<tr><td><code>distribute_join_row_count_threshold</code></td><td>Sets the minimum number of rows estimated to be processed from both inputs by the hash or merge join to distribute the plan.</td><td><code>1000</code></td><td>No</td></tr>
<tr><td><code>distribute_scan_row_count_threshold</code></td><td>Sets the minimum number of rows estimated to be read by the Scan operator to distribute the plan.</td><td><code>10000</code></td><td>No</td></tr>
<tr><td><code>distribute_sort_row_count_threshold</code></td><td>Sets the minimum number of rows estimated to be processed by the Sort operator to distribute the plan.</td><td><code>1000</code></td><td>No</td></tr>
<tr><td><code>distsql</code></td><td>Controls whether distributed SQL execution is enabled.</td><td><code>auto</code></td><td>No</td></tr>
<tr><td><code>distsql_plan_gateway_bias</code></td><td>Controls how many more partition spans the gateway node can be assigned compared to other nodes when distributing SQL execution.</td><td><code>2</code></td><td>No</td></tr>
<tr><td><code>distsql_workmem</code></td><td>Determines how much RAM (in bytes) a single operation of a single query can use before it has to spill to disk.</td><td><code>64 MiB</code></td><td>No</td></tr>
<tr><td><code>enable_auto_rehoming</code></td><td>Controls whether auto-rehoming is enabled for REGIONAL BY ROW tables.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_create_stats_using_extremes</code></td><td>Controls whether CREATE STATISTICS using the EXTREMES method is enabled.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>enable_create_stats_using_extremes_bool_enum</code></td><td>Controls whether CREATE STATISTICS using the EXTREMES method is enabled for boolean and enum types.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_durable_locking_for_serializable</code></td><td>Controls whether durable locking is used for FOR UPDATE/FOR SHARE statements and constraint checks under serializable isolation.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_experimental_alter_column_type_general</code></td><td>Controls whether ALTER TABLE ... ALTER COLUMN ... TYPE can be used for general conversions requiring online schema changes.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_implicit_fk_locking_for_serializable</code></td><td>Controls whether FOR SHARE locking is used when checking referenced tables during foreign key operations under serializable isolation.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_implicit_select_for_update</code></td><td>Controls whether FOR UPDATE locking may be used during the row-fetch phase of mutation statements.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>enable_implicit_transaction_for_batch_statements</code></td><td>Controls whether a batch of statements sent in one query is executed as an implicit transaction.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>enable_insert_fast_path</code></td><td>Controls whether the fast path for INSERT operations with VALUES input may be used.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>enable_multiple_modifications_of_table</code></td><td>Allows statements with multiple modification subqueries for the same table, risking data corruption if rows are modified multiple times.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_multiregion_placement_policy</code></td><td>Controls whether placement can be used in multi-region contexts.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_seqscan</code></td><td>enable_seqscan is included for compatibility with Postgres; changing it has no effect.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>enable_shared_locking_for_serializable</code></td><td>Controls whether SELECT FOR SHARE statements acquire shared locks under serializable isolation.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_super_regions</code></td><td>Controls whether super region functionality is enabled.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enable_zigzag_join</code></td><td>Controls whether the optimizer should try to plan a zigzag join.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enforce_home_region</code></td><td>Controls whether to error on queries scanning rows from multiple regions or from a different home region than the gateway.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>enforce_home_region_follower_reads_enabled</code></td><td>Allows using follower reads to dynamically detect and report a query's home region when enforce_home_region is enabled.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>escape_string_warning</code></td><td>Controls whether warnings are issued for escape string syntax.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>expect_and_ignore_not_visible_columns_in_copy</code></td><td>Changes behavior for COPY FROM to expect and ignore not visible column fields.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>experimental_enable_implicit_column_partitioning</code></td><td>Controls whether implicit column partitioning can be created.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>experimental_enable_temp_tables</code></td><td>Controls whether temporary tables can be created.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>experimental_enable_unique_without_index_constraints</code></td><td>Controls whether creating unique constraints without an index is allowed.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>experimental_hash_group_join_enabled</code></td><td>Controls whether the physical planner will convert a hash join followed by hash aggregator into a single hash group-join.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>extra_float_digits</code></td><td>Sets the number of digits beyond the standard number to use for float conversions.</td><td><code>1</code></td><td>No</td></tr>
<tr><td><code>force_savepoint_restart</code></td><td>Overrides the default SAVEPOINT behavior for compatibility with certain ORMs.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>foreign_key_cascades_limit</code></td><td>Sets the maximum number of cascading operations for foreign key actions.</td><td><code>10000</code></td><td>No</td></tr>
<tr><td><code>idle_in_transaction_session_timeout</code></td><td>Sets the maximum allowed duration for an idle transaction session. The session is terminated if it exceeds this limit.</td><td><code>0s</code></td><td>No</td></tr>
<tr><td><code>idle_session_timeout</code></td><td></td><td><code>0s</code></td><td>No</td></tr>
<tr><td><code>index_join_streamer_batch_size</code></td><td>Sets the size limit on input rows to the ColIndexJoin operator when using the Streamer API for a single lookup KV batch.</td><td><code>8.0 MiB</code></td><td>No</td></tr>
<tr><td><code>index_recommendations_enabled</code></td><td>Controls whether index recommendations are enabled.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>inject_retry_errors_enabled</code></td><td>Causes statements inside explicit transactions to return a transaction retry error for testing application retry logic.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>inject_retry_errors_on_commit_enabled</code></td><td>Causes statements inside explicit transactions to return a retry error just before transaction commit for testing retry logic.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>integer_datetimes</code></td><td>Reports whether integer datetime representation is used (always on).</td><td><code>on</code></td><td>Yes</td></tr>
<tr><td><code>intervalstyle</code></td><td>Controls the display format for interval values.</td><td><code>postgres</code></td><td>No</td></tr>
<tr><td><code>is_superuser</code></td><td>Indicates whether the current user has superuser privileges.</td><td><code>off</code></td><td>Yes</td></tr>
<tr><td><code>join_reader_index_join_strategy_batch_size</code></td><td>Sets the size limit on input rows to the joinReader processor when performing index joins for a single lookup KV batch.</td><td><code>4.0 MiB</code></td><td>No</td></tr>
<tr><td><code>join_reader_no_ordering_strategy_batch_size</code></td><td>Sets the size limit on input rows to the joinReader processor (when ordering is not maintained) for a single lookup KV batch.</td><td><code>2.0 MiB</code></td><td>No</td></tr>
<tr><td><code>join_reader_ordering_strategy_batch_size</code></td><td>Sets the size limit on input rows to the joinReader processor (when ordering must be maintained) for a single lookup KV batch.</td><td><code>100 KiB</code></td><td>No</td></tr>
<tr><td><code>kv_transaction_buffered_writes_enabled</code></td><td>Controls whether the buffered writes KV transaction protocol is used for user queries on the current session.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>large_full_scan_rows</code></td><td>Sets the estimated row count at which a full scan is considered large and worthy of logging or disabling.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>lc_collate</code></td><td>Reports the database collation locale.</td><td><code>C.UTF-8</code></td><td>Yes</td></tr>
<tr><td><code>lc_ctype</code></td><td>Reports the database character classification locale.</td><td><code>C.UTF-8</code></td><td>Yes</td></tr>
<tr><td><code>lc_messages</code></td><td>Sets the language in which messages are displayed (compatibility setting).</td><td><code>C.UTF-8</code></td><td>No</td></tr>
<tr><td><code>lc_monetary</code></td><td>Sets the locale for formatting monetary amounts (compatibility setting).</td><td><code>C.UTF-8</code></td><td>No</td></tr>
<tr><td><code>lc_numeric</code></td><td>Sets the locale for formatting numbers (compatibility setting).</td><td><code>C.UTF-8</code></td><td>No</td></tr>
<tr><td><code>lc_time</code></td><td>Sets the locale for formatting dates and times (compatibility setting).</td><td><code>C.UTF-8</code></td><td>No</td></tr>
<tr><td><code>legacy_varchar_typing</code></td><td>Controls the legacy behavior of allowing some invalid mix-typed comparisons with VARCHAR types.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>locality</code></td><td>The locality of the current node.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>locality_optimized_partitioned_index_scan</code></td><td>Controls whether locality-optimized partitioned index scans are enabled.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>lock_timeout</code></td><td>Sets the maximum amount of time that a query will wait while attempting to acquire a lock or while blocking on an existing lock.</td><td><code>0s</code></td><td>No</td></tr>
<tr><td><code>log_timezone</code></td><td>The timezone used for logging (always UTC).</td><td><code>UTC</code></td><td>No</td></tr>
<tr><td><code>max_connections</code></td><td>Reports the maximum number of concurrent connections.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>max_identifier_length</code></td><td>The maximum length allowed for identifiers.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>max_index_keys</code></td><td>Reports the maximum number of index keys (always 32).</td><td><code>32</code></td><td>Yes</td></tr>
<tr><td><code>max_prepared_transactions</code></td><td>Reports the maximum number of prepared transactions (always maximum int32).</td><td><code>2147483647</code></td><td>Yes</td></tr>
<tr><td><code>max_retries_for_read_committed</code></td><td>Sets the maximum number of automatic retries for statements in explicit READ COMMITTED transactions that encounter retry errors.</td><td><code>10</code></td><td>No</td></tr>
<tr><td><code>multiple_active_portals_enabled</code></td><td>Determines if pgwire portal execution for certain queries can be paused, allowing interleaved execution with local plans.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>node_id</code></td><td>The ID of the current node.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>null_ordered_last</code></td><td>Controls whether NULL values are ordered last. When true, NULL values appear after non-NULL values in ordered results.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>on_update_rehome_row_enabled</code></td><td>Controls whether the ON UPDATE rehome_row() will actually trigger on row updates.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>opt_split_scan_limit</code></td><td>Sets the maximum number of UNION ALL statements a Scan may be split into during query optimization to avoid a sort.</td><td><code>2048</code></td><td>No</td></tr>
<tr><td><code>optimizer</code></td><td>Controls whether the cost-based optimizer is enabled.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_always_use_histograms</code></td><td>Ensures that the optimizer always uses histograms to calculate statistics if available.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_check_input_min_row_count</code></td><td>Sets a lower bound on row count estimates for the buffer scan of foreign key and uniqueness checks.</td><td><code>1</code></td><td>No</td></tr>
<tr><td><code>optimizer_hoist_uncorrelated_equality_subqueries</code></td><td>Controls whether the optimizer hoists uncorrelated subqueries in equality expressions with columns, potentially producing more efficient plans.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_merge_joins_enabled</code></td><td>Controls whether the optimizer should explore query plans with merge joins.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_min_row_count</code></td><td>Sets a lower bound on row count estimates during query planning, except for expressions with zero cardinality.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>optimizer_prefer_bounded_cardinality</code></td><td>Instructs the optimizer to prefer query plans where every expression has a bounded cardinality over plans with unbounded cardinality expressions.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>optimizer_prove_implication_with_virtual_computed_columns</code></td><td>Controls whether the optimizer should use virtual computed columns to prove partial index implication.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_push_limit_into_project_filtered_scan</code></td><td>Controls whether the optimizer should push limit expressions into projects of filtered scans.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_push_offset_into_index_join</code></td><td>Controls whether the optimizer should push offset expressions into index joins.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_conditional_hoist_fix</code></td><td>Prevents the optimizer from hoisting volatile expressions that are conditionally executed by CASE, COALESCE, or IFERR expressions.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_forecasts</code></td><td>Controls whether the optimizer should use statistics forecasts for cardinality estimation.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_histograms</code></td><td>Controls whether the optimizer should use histogram statistics for cardinality estimation.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_computed_column_filters_derivation</code></td><td>Enables the optimizer to derive filters on computed columns in more cases beyond simple single-column equations.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_disjunction_stats</code></td><td>Controls whether the optimizer should use improved statistics calculations for disjunctive filters.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_distinct_on_limit_hint_costing</code></td><td>Controls whether the optimizer should use an improved costing estimate for DistinctOn operators with limit hints.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_join_elimination</code></td><td>Allows the optimizer to eliminate joins in more cases by remapping columns from eliminated joins to equivalent columns.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_multi_column_selectivity_estimate</code></td><td>Controls whether the optimizer should use an improved selectivity estimate for multi-column predicates.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_split_disjunction_for_joins</code></td><td>Enables the optimizer to split more disjunctions (OR expressions) in join conditions by building a UNION of join expressions.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_trigram_similarity_selectivity</code></td><td>Controls whether the optimizer should use an improved selectivity estimate for trigram similarity filters.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_improved_zigzag_join_costing</code></td><td>Controls whether the optimizer should use improved logic in the cost model for zigzag joins.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_limit_ordering_for_streaming_group_by</code></td><td>Enables optimization for 'SELECT ... GROUP BY ... ORDER BY ... LIMIT n' queries by using the limit ordering to inform group-by requirements.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_lock_op_for_serializable</code></td><td>Controls whether the optimizer implements SELECT FOR UPDATE and FOR SHARE using the Lock operator under serializable isolation.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_merged_partial_statistics</code></td><td>Controls whether the optimizer should use statistics merged from partial and full statistics for cardinality estimation.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_multicol_stats</code></td><td>Controls whether the optimizer should use multi-column statistics for cardinality estimation.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_not_visible_indexes</code></td><td>Controls whether the optimizer can still choose to use not visible indexes for query plans.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_polymorphic_parameter_fix</code></td><td>Controls whether the optimizer validates routine polymorphic parameters during overload resolution and type-checking.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_provided_ordering_fix</code></td><td>Controls whether the optimizer reconciles provided orderings with required ordering choices to prevent internal errors.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_trigram_similarity_optimization</code></td><td>Controls whether the optimizer should generate improved plans for queries with trigram similarity filters.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>optimizer_use_virtual_computed_column_stats</code></td><td>Controls whether the optimizer should use statistics on virtual computed columns for cardinality estimation.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>override_multi_region_zone_config</code></td><td>Controls whether zone configurations can be modified for multi-region databases and their objects.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>parallelize_multi_key_lookup_joins_enabled</code></td><td>Controls whether the join reader should parallelize lookup batches. When enabled, this increases the speed of lookup joins with multiple looked up rows at the cost of increased memory usage.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>password_encryption</code></td><td>The encryption method used for passwords.</td><td><code>scram-sha-256</code></td><td>No</td></tr>
<tr><td><code>pg_trgm.similarity_threshold</code></td><td>Sets the value used to compare trigram similarities for the string % string overload.</td><td><code>0.3</code></td><td>No</td></tr>
<tr><td><code>plan_cache_mode</code></td><td>Controls the method that the optimizer should use to choose between a custom and generic query plan.</td><td><code>auto</code></td><td>No</td></tr>
<tr><td><code>plpgsql_use_strict_into</code></td><td>Causes PL/pgSQL "SELECT ... INTO" and "RETURNING INTO" syntax to always behave as if specified with STRICT option.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>prefer_lookup_joins_for_fks</code></td><td>Causes foreign key operations to prefer lookup joins.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>prepared_statements_cache_size</code></td><td>Causes the LRU prepared statements in a session to be automatically deallocated when total prepared statement memory usage exceeds this value.</td><td><code>0 B</code></td><td>No</td></tr>
<tr><td><code>propagate_input_ordering</code></td><td>Controls whether to propagate inner ordering to the outer scope when planning subqueries or CTEs if the outer scope is unordered.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>recursion_depth_limit</code></td><td>Sets the maximum depth that nested trigger-function calls can reach.</td><td><code>1000</code></td><td>No</td></tr>
<tr><td><code>reorder_joins_limit</code></td><td>Sets the number of joins at which the optimizer should stop attempting to reorder.</td><td><code>8</code></td><td>No</td></tr>
<tr><td><code>require_explicit_primary_keys</code></td><td>Controls whether CREATE TABLE statements should error out if no primary key is provided.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>results_buffer_size</code></td><td>Specifies the size at which the pgwire results buffer will self-flush.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>role</code></td><td>The current role for the session.</td><td><code>none</code></td><td>No</td></tr>
<tr><td><code>row_security</code></td><td>Controls whether row level security is enabled.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>search_path</code></td><td>Sets the list of namespaces to search when resolving unqualified names.</td><td><code>"$user", public</code></td><td>No</td></tr>
<tr><td><code>serial_normalization</code></td><td>Controls how SERIAL columns are normalized.</td><td><code>rowid</code></td><td>No</td></tr>
<tr><td><code>server_encoding</code></td><td>Reports the database encoding (always UTF8). This cannot be changed.</td><td><code>UTF8</code></td><td>Yes</td></tr>
<tr><td><code>server_version</code></td><td>Reports the server version string.</td><td><code>13.0.0</code></td><td>Yes</td></tr>
<tr><td><code>server_version_num</code></td><td>Reports the server version as an integer.</td><td><code>130000</code></td><td>Yes</td></tr>
<tr><td><code>session_id</code></td><td>The unique identifier for the current session.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>show_primary_key_constraint_on_not_visible_columns</code></td><td>Controls whether SHOW CONSTRAINTS and pg_catalog.pg_constraint include primary key constraints with only hidden columns.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>sql_safe_updates</code></td><td>When enabled, causes errors when the client sends syntax that may have unwanted side effects, like DELETE without a WHERE clause.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>ssl</code></td><td>Controls whether SSL is enabled.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>standard_conforming_strings</code></td><td>Controls whether strings conform to SQL standard.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>statement_timeout</code></td><td>Sets the maximum allowed duration for a statement. The query is cancelled if it exceeds this limit.</td><td><code>0s</code></td><td>No</td></tr>
<tr><td><code>streamer_always_maintain_ordering</code></td><td>Controls whether the SQL users of the Streamer should always maintain ordering, even when not strictly necessary.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>streamer_enabled</code></td><td>Controls whether the Streamer API can be used.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>streamer_head_of_line_only_fraction</code></td><td>Controls the fraction of the streamer's memory budget used for the head-of-the-line request when the eager memory usage limit has been exceeded.</td><td><code>0.8</code></td><td>No</td></tr>
<tr><td><code>streamer_in_order_eager_memory_usage_fraction</code></td><td>Controls the fraction of the streamer's memory budget that might be used for issuing requests eagerly in the InOrder mode.</td><td><code>0.5</code></td><td>No</td></tr>
<tr><td><code>streamer_out_of_order_eager_memory_usage_fraction</code></td><td>Controls the fraction of the streamer's memory budget that might be used for issuing requests eagerly in the OutOfOrder mode.</td><td><code>0.8</code></td><td>No</td></tr>
<tr><td><code>strict_ddl_atomicity</code></td><td>When enabled, causes errors when DDL operations inside an explicit transaction cannot be guaranteed to be performed atomically, preventing partial transaction commits.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>stub_catalog_tables</code></td><td>Controls whether catalog tables are stubbed out.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>synchronize_seqscans</code></td><td>Controls whether synchronized sequential scans are enabled (compatibility setting).</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>synchronous_commit</code></td><td>synchronous_commit is included for compatibility with Postgres; changing it has no effect.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>system_identity</code></td><td>Indicates the original name of the client presented to pgwire before it was mapped to a SQL identifier.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>testing_optimizer_cost_perturbation</code></td><td>Controls the random cost perturbation factor for producing non-optimal query plans during testing.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>testing_optimizer_disable_rule_probability</code></td><td>Sets the probability of randomly disabling non-essential optimizer transformation rules for testing.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>testing_optimizer_inject_panics</code></td><td>Controls whether random panics are injected during optimization to test error-propagation.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>testing_optimizer_random_seed</code></td><td>Sets a random seed for the optimizer for testing by initializing an RNG with the given integer.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>testing_vectorize_inject_panics</code></td><td>Controls whether random panics are injected into vectorized execution to test error propagation.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>timezone</code></td><td>Sets the timezone used for parsing timestamps.</td><td><code>UTC</code></td><td>No</td></tr>
<tr><td><code>tracing</code></td><td>Controls whether tracing is enabled for the session.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>transaction_isolation</code></td><td>The isolation level of the current transaction. Also allows the isolation level to change as long as queries have not been executed yet.</td><td><code>serializable</code></td><td>No</td></tr>
<tr><td><code>transaction_priority</code></td><td>The priority level of the current transaction.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>transaction_read_only</code></td><td>Controls whether the current transaction is read-only.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>transaction_rows_read_err</code></td><td>Sets the limit for the number of rows read by a SQL transaction which - once exceeded - will fail the transaction (0 means disabled).</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>transaction_rows_read_log</code></td><td>Sets the threshold for the number of rows read by a SQL transaction which - once exceeded - will trigger a logging event.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>transaction_rows_written_err</code></td><td>The limit for the number of rows written by a SQL transaction which - once exceeded - will fail the transaction (0 means disabled).</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>transaction_rows_written_log</code></td><td>Sets the threshold for the number of rows written by a SQL transaction which - once exceeded - will trigger a logging event.</td><td><code>0</code></td><td>No</td></tr>
<tr><td><code>transaction_status</code></td><td>Reports the current transaction status.</td><td><code>-</code></td><td>Yes</td></tr>
<tr><td><code>transaction_timeout</code></td><td>Sets the maximum duration a transaction is permitted to run before cancellation.</td><td><code>0s</code></td><td>No</td></tr>
<tr><td><code>troubleshooting_mode</code></td><td>Controls whether to refuse collecting and emitting telemetry data for queries.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>unbounded_parallel_scans</code></td><td>Controls whether the TableReader DistSQL processors should parallelize scans across ranges.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>unconstrained_non_covering_index_scan_enabled</code></td><td>Controls whether unconstrained non-covering index scan access paths are explored by the optimizer.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>unsafe_allow_triggers_modifying_cascades</code></td><td>When enabled, allows row-level BEFORE triggers to modify or filter rows that are being updated or deleted as part of a cascading foreign key action. This is unsafe because it can lead to constraint violations.</td><td><code>off</code></td><td>No</td></tr>
<tr><td><code>use_declarative_schema_changer</code></td><td>Controls whether the declarative schema changer is used.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>variable_inequality_lookup_join_enabled</code></td><td>Controls whether the optimizer should consider lookup joins with inequality conditions.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>vectorize</code></td><td>Controls if and when the Executor executes queries using the columnar execution engine. Can be 'off', 'on', or 'experimental_always'.</td><td><code>on</code></td><td>No</td></tr>
<tr><td><code>xmloption</code></td><td>Sets how XML data is to be implicitly parsed (compatibility setting).</td><td><code>content</code></td><td>No</td></tr>
</tbody></table>
