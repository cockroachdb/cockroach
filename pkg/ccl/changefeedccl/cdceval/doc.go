// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

/***

cdceval package is a library for evaluating various expressions in CDC.

The expression evaluation and execution is integrated with planner/distSQL.

First, when starting changefeed, CDC expression is planned and normalized
(NormalizeAndPlan).  The normalization step involves various sanity checks
(ensuring for example that expression does not target multiple column familes).
The expression is planned as well because this step, by leveraging optimizer,
may simplify expressions, and, more importantly, can detect if the expression
will not match any rows (this results in an error being returned).

The normalized expression is persisted into job record.
When changefeed job starts running, it once again plans expression execution.
Part of the planning stage figures out which spans need to be scanned.  If the
predicate restricted primary index span, then we will scan only portion
of the table.

Then, once aggregators start up, they will once again plan the expression
(sql.PlanCDCExpression), but this time each incoming KV event will be evaluated
by DistSQL to produce final result (projection).
PlanCDCExpression fully integrates with optimizer, and produces a plan
that can then be used to execute the "flow" via normal
distSQL mechanism (PlanAndRun).

What makes CDC expression execution different is that CDC is responsible for
pushing the data into the execution pipeline.  This is accomplished via
execinfra.RowReceiver which is returned as part of the plan.
CDC will receive rows (encoded datums) from rangefeed, and then "pushes" those
rows into execution pipeline.

CDC then reads the resulting projection via distSQL result writer.

Evaluator is the gateway into the evaluation logic;  it takes care of running
execution flow, caching (to reuse the same plan as long as the descriptor version
does not change), etc.

Expressions can contain functions.  We restrict the set of functions that can be used by CDC.
Volatile functions, window functions, aggregate functions are disallowed (see function
section below for more info).
Certain stable functions (s.a. now(), current_timestamp(), etc) are allowed -- they will always
return the MVCC timestamp of the event.

Access to the previous state of the row is accomplished via (typed) cdc_prev tuple.
This tuple can be used to build complex expressions around the previous state of the row:
   SELECT * FROM foo WHERE status='active' AND cdc_prev.status='inactive'

During normalization stage, we determine if the expression has cdc_prev access.
If so, the expression is rewritten as:
  SELECT ... FROM tbl, (SELECT ((crdb_internal.cdc_prev_row()).*)) AS cdc_prev
The crdb_internal.cdc_prev_row function is created to return a tuple based on
the previous table descriptor.  Access to this function is arranged via custom
function resolver.

In addition, prior to evaluating CDC expression, the WHERE clause is rewritten as:
  SELECT where, ... FROM tbl, ...
That is, WHERE clause is turned into a boolean datum.  When projection results are
consumed, we can determine if the row ought to be filtered.  This step is done to
ensure that we correctly release resources for each event -- even the ones that
are filtered out.

Virtual computed columns can be easily supported but currently are not.
To support virtual computed columns we must ensure that the expression in that
column references only the target changefeed column family.

*** Functions

As mentioned above, some functions, notably volatile, aggregate, and windowing
ones are disallowed. Immutable and leakproof functions, on the other hand
are definitely safe.  The question is: what should be done about stable functions?
Since changefeed serializes session data in the job record, all stable
functions, at least in theory, can be allowed.  However, that's not the
approach currently taken.  Instead, stable functions are allowed on a white
listed basis (though, of course, this can be flipped to be a blacklist basis
instead)..

The following is a litmus test that was applied to determine if a
stable function should be allowed.
  * Is the stable function actually volatile in CDC context?
    Example functions include crdb_internal.node_id, current_setting, gateway_region, etc.
    These functions are stable, but if the changefeed restarts, all of these
    can produce different values.
  * Functions, such as format_type, that access external resources (i.e. fire off additional queries,
    hydrate descriptors, etc) are disallowed.
  * Is the stable function useful in CDC context?
    Some functions with questionable usefulness include enum_first, enum_last, enum_range.
    Note, it's absolutely fine to emit enum values as part of an event, but
    emitting enum_first (on some external to the table enum) does not seem to be particularly
    useful.
  * Can the function be considered a liability in CDC context? Is it useful?
    Example functions include hax_XXX_provilege family of functions.
    One could imagine "exfiltrating" security related settings/configurations through
    changefeed via has_XXX_privelege.  Then, again, are these functions useful in CDC context.
  * Is the function, or functions, confusing in CDC context.  This bit is specifically
    about time related functions -- now(), localtime(), etc.  It is understood that
    now() refers to the timestamp of the transaction -- but that time is roughly around the
    time transaction executed; That's just not the case with CDC which emits events in the past.
    Now, we've chosen to allow transaction_timestamp() override, because that one explicitly
    talks about transaction (that committed the event).  But other time related functions
    currently are blocked.

The following query can be used to retrieve the list of stable (non window/non aggregate)
functions along with the number of arguments and the return type of the overload:
   SELECT p.proname, p.pronargs, t.typname
   FROM pg_proc p, pg_type t
   WHERE p.provolatile='s' and p.prorettype = t.oid and
         p.prokind='f' and p.proretset=false
   ORDER BY proname

                            proname                            | pronargs |   typname
---------------------------------------------------------------+----------+--------------
  age                                                          |        1 | interval
  array_to_json                                                |        1 | jsonb
  array_to_json                                                |        2 | jsonb
  array_to_string                                              |        2 | text
  array_to_string                                              |        3 | text
  col_description                                              |        2 | text
  crdb_internal.assignment_cast                                |        2 | anyelement
  crdb_internal.cluster_id                                     |        0 | uuid
  crdb_internal.encode_key                                     |        3 | bytea
  crdb_internal.filter_multiregion_fields_from_zone_config_sql |        1 | text
  crdb_internal.get_database_id                                |        1 | int8
  crdb_internal.get_namespace_id                               |        2 | int8
  crdb_internal.get_namespace_id                               |        3 | int8
  crdb_internal.get_zone_config                                |        1 | bytea
  crdb_internal.locality_value                                 |        1 | text
  crdb_internal.node_id                                        |        0 | int8
  crdb_internal.num_geo_inverted_index_entries                 |        3 | int8
  crdb_internal.num_geo_inverted_index_entries                 |        3 | int8
  crdb_internal.num_inverted_index_entries                     |        1 | int8
  crdb_internal.num_inverted_index_entries                     |        1 | int8
  crdb_internal.num_inverted_index_entries                     |        2 | int8
  crdb_internal.num_inverted_index_entries                     |        2 | int8
  crdb_internal.num_inverted_index_entries                     |        2 | int8
  crdb_internal.round_decimal_values                           |        2 | _numeric
  current_database                                             |        0 | text
  current_date                                                 |        0 | date
  current_schema                                               |        0 | text
  current_schemas                                              |        1 | _text
  current_setting                                              |        1 | text
  current_setting                                              |        2 | text
  current_time                                                 |        0 | timetz
  current_time                                                 |        0 | time
  current_time                                                 |        1 | timetz
  current_time                                                 |        1 | time
  current_timestamp                                            |        1 | timestamptz
  current_timestamp                                            |        1 | timestamp
  current_timestamp                                            |        1 | date
  current_timestamp                                            |        0 | timestamptz
  current_timestamp                                            |        0 | timestamp
  current_timestamp                                            |        0 | date
  current_user                                                 |        0 | text
  date_part                                                    |        2 | float8
  date_trunc                                                   |        2 | timestamptz
  date_trunc                                                   |        2 | timestamptz
  date_trunc                                                   |        2 | interval
  default_to_database_primary_region                           |        1 | text
  enum_first                                                   |        1 | anyelement
  enum_last                                                    |        1 | anyelement
  enum_range                                                   |        1 | anyelement
  enum_range                                                   |        2 | anyelement
  extract                                                      |        2 | float8
  format                                                       |        2 | text
  format_type                                                  |        2 | text
  gateway_region                                               |        0 | text
  getdatabaseencoding                                          |        0 | text
  has_any_column_privilege                                     |        2 | bool
  has_any_column_privilege                                     |        2 | bool
  has_any_column_privilege                                     |        3 | bool
  has_any_column_privilege                                     |        3 | bool
  has_any_column_privilege                                     |        3 | bool
  has_any_column_privilege                                     |        3 | bool
  has_column_privilege                                         |        3 | bool
  has_column_privilege                                         |        3 | bool
  has_column_privilege                                         |        3 | bool
  has_column_privilege                                         |        3 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_column_privilege                                         |        4 | bool
  has_database_privilege                                       |        2 | bool
  has_database_privilege                                       |        2 | bool
  has_database_privilege                                       |        3 | bool
  has_database_privilege                                       |        3 | bool
  has_database_privilege                                       |        3 | bool
  has_database_privilege                                       |        3 | bool
  has_foreign_data_wrapper_privilege                           |        2 | bool
  has_foreign_data_wrapper_privilege                           |        2 | bool
  has_foreign_data_wrapper_privilege                           |        3 | bool
  has_foreign_data_wrapper_privilege                           |        3 | bool
  has_foreign_data_wrapper_privilege                           |        3 | bool
  has_foreign_data_wrapper_privilege                           |        3 | bool
  has_function_privilege                                       |        2 | bool
  has_function_privilege                                       |        2 | bool
  has_function_privilege                                       |        3 | bool
  has_function_privilege                                       |        3 | bool
  has_function_privilege                                       |        3 | bool
  has_function_privilege                                       |        3 | bool
  has_language_privilege                                       |        2 | bool
  has_language_privilege                                       |        2 | bool
  has_language_privilege                                       |        3 | bool
  has_language_privilege                                       |        3 | bool
  has_language_privilege                                       |        3 | bool
  has_language_privilege                                       |        3 | bool
  has_schema_privilege                                         |        2 | bool
  has_schema_privilege                                         |        2 | bool
  has_schema_privilege                                         |        3 | bool
  has_schema_privilege                                         |        3 | bool
  has_schema_privilege                                         |        3 | bool
  has_schema_privilege                                         |        3 | bool
  has_sequence_privilege                                       |        2 | bool
  has_sequence_privilege                                       |        2 | bool
  has_sequence_privilege                                       |        3 | bool
  has_sequence_privilege                                       |        3 | bool
  has_sequence_privilege                                       |        3 | bool
  has_sequence_privilege                                       |        3 | bool
  has_server_privilege                                         |        2 | bool
  has_server_privilege                                         |        2 | bool
  has_server_privilege                                         |        3 | bool
  has_server_privilege                                         |        3 | bool
  has_server_privilege                                         |        3 | bool
  has_server_privilege                                         |        3 | bool
  has_table_privilege                                          |        2 | bool
  has_table_privilege                                          |        2 | bool
  has_table_privilege                                          |        3 | bool
  has_table_privilege                                          |        3 | bool
  has_table_privilege                                          |        3 | bool
  has_table_privilege                                          |        3 | bool
  has_tablespace_privilege                                     |        2 | bool
  has_tablespace_privilege                                     |        2 | bool
  has_tablespace_privilege                                     |        3 | bool
  has_tablespace_privilege                                     |        3 | bool
  has_tablespace_privilege                                     |        3 | bool
  has_tablespace_privilege                                     |        3 | bool
  has_type_privilege                                           |        2 | bool
  has_type_privilege                                           |        2 | bool
  has_type_privilege                                           |        3 | bool
  has_type_privilege                                           |        3 | bool
  has_type_privilege                                           |        3 | bool
  has_type_privilege                                           |        3 | bool
  inet_client_addr                                             |        0 | inet
  inet_client_port                                             |        0 | int8
  inet_server_addr                                             |        0 | inet
  inet_server_port                                             |        0 | int8
  information_schema._pg_index_position                        |        2 | int8
  json_build_array                                             |        1 | jsonb
  json_build_object                                            |        1 | jsonb
  json_populate_record                                         |        2 | anyelement
  json_populate_recordset                                      |        2 | anyelement
  jsonb_build_array                                            |        1 | jsonb
  jsonb_build_object                                           |        1 | jsonb
  jsonb_populate_record                                        |        2 | anyelement
  jsonb_populate_recordset                                     |        2 | anyelement
  localtime                                                    |        0 | timetz
  localtime                                                    |        0 | time
  localtime                                                    |        1 | timetz
  localtime                                                    |        1 | time
  localtimestamp                                               |        1 | timestamptz
  localtimestamp                                               |        1 | timestamp
  localtimestamp                                               |        1 | date
  localtimestamp                                               |        0 | timestamptz
  localtimestamp                                               |        0 | timestamp
  localtimestamp                                               |        0 | date
  now                                                          |        0 | timestamptz
  now                                                          |        0 | timestamp
  now                                                          |        0 | date
  obj_description                                              |        1 | text
  obj_description                                              |        2 | text
  overlaps                                                     |        4 | bool
  pg_backend_pid                                               |        0 | int8
  pg_blocking_pids                                             |        0 | _int8
  pg_client_encoding                                           |        0 | text
  pg_collation_for                                             |        1 | text
  pg_column_is_updatable                                       |        3 | bool
  pg_encoding_to_char                                          |        1 | text
  pg_function_is_visible                                       |        1 | bool
  pg_get_constraintdef                                         |        2 | text
  pg_get_constraintdef                                         |        1 | text
  pg_get_expr                                                  |        2 | text
  pg_get_expr                                                  |        3 | text
  pg_get_function_arguments                                    |        1 | text
  pg_get_function_identity_arguments                           |        1 | text
  pg_get_function_result                                       |        1 | text
  pg_get_functiondef                                           |        1 | text
  pg_get_indexdef                                              |        1 | text
  pg_get_indexdef                                              |        3 | text
  pg_get_partkeydef                                            |        1 | text
  pg_get_serial_sequence                                       |        2 | text
  pg_get_userbyid                                              |        1 | text
  pg_get_viewdef                                               |        1 | text
  pg_get_viewdef                                               |        2 | text
  pg_has_role                                                  |        2 | bool
  pg_has_role                                                  |        2 | bool
  pg_has_role                                                  |        3 | bool
  pg_has_role                                                  |        3 | bool
  pg_has_role                                                  |        3 | bool
  pg_has_role                                                  |        3 | bool
  pg_is_other_temp_schema                                      |        1 | bool
  pg_my_temp_schema                                            |        0 | oid
  pg_relation_is_updatable                                     |        2 | int4
  pg_sequence_parameters                                       |        1 | text
  pg_table_is_visible                                          |        1 | bool
  pg_type_is_visible                                           |        1 | bool
  pg_typeof                                                    |        1 | text
  quote_literal                                                |        1 | text
  quote_nullable                                               |        1 | text
  rehome_row                                                   |        0 | text
  row_to_json                                                  |        1 | jsonb
  session_user                                                 |        0 | text
  shobj_description                                            |        2 | text
  st_asgeojson                                                 |        2 | text
  st_asgeojson                                                 |        3 | text
  st_asgeojson                                                 |        4 | text
  st_estimatedextent                                           |        4 | box2d
  st_estimatedextent                                           |        3 | box2d
  st_estimatedextent                                           |        2 | box2d
  statement_timestamp                                          |        0 | timestamptz
  statement_timestamp                                          |        0 | timestamp
  timeofday                                                    |        0 | text
  timezone                                                     |        2 | timestamp
  timezone                                                     |        2 | timetz
  timezone                                                     |        2 | timetz
  to_char                                                      |        2 | text
  to_char                                                      |        2 | text
  to_char                                                      |        2 | text
  to_json                                                      |        1 | jsonb
  to_jsonb                                                     |        1 | jsonb
  to_regclass                                                  |        1 | regtype
  to_regnamespace                                              |        1 | regtype
  to_regproc                                                   |        1 | regtype
  to_regprocedure                                              |        1 | regtype
  to_regrole                                                   |        1 | regtype
  to_regtype                                                   |        1 | regtype
  transaction_timestamp                                        |        0 | timestamptz
  transaction_timestamp                                        |        0 | timestamp
  transaction_timestamp                                        |        0 | date

***/
