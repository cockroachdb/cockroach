- Feature Name: Redact Statement Diagnostics Bundles
- Status: draft
- Start Date: 2022-10-17
- Authors: Michael Erickson
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes options to redact PII from statement diagnostics bundles in
order to comply with PCI DSS and other security audits, without unduly hampering
customer support investigations.

The options are divided into three sets:
1. Options to _redact_ statement diagnostics bundles.
2. Options to _unredact_ statement diagnostics bundles.
3. Options to _obfuscate_ statement diagnostics bundles.

The first set of options are currently being prototyped, and must be finished by
the end of the year for an upcoming audit. The second and third sets of options
are speculative, and are included here to solicit comments.

# Motivation



# Technical design



## Background

Statement diagnostics bundles are zip archives containing information about the
execution of individual SQL statements. They are used to troubleshoot SQL
execution during customer support investigations. The primary users of statement
diagnostics bundles are Cockroach Labs TSEs, SREs, SEs, and Engineering.

### Collection

Collection of statement diagnostics bundles can be triggered via several
methods:
1. By executing the SQL statement prepended with `EXPLAIN ANALYZE (DEBUG)`.
2. By calling the built-in SQL function `crdb_internal.request_statement_bundle`
   with the fingerprint of the SQL statement.
3. By POSTing a `CreateStatementDiagnosticsReportRequest` message to the
   `/_status/stmtdiagreports` gRPC endpoint with the fingerprint of the SQL
   statement.
4. By clicking "Activate statement diagnostics" on the Diagnostics tab of the
   Statement Fingerprint page for the statement in the DB Console.

Method 1 immediately executes the statement with verbose tracing, creates a
bundle after execution finishes, inserts the bundle and bundle metadata into
`system.statement_bundle_chunks` and `system.statement_diagnostics`, and then
either updates or inserts a completed row into
`system.statement_diagnostics_requests`. After this, instead of normal query
results a message with bundle download instructions is returned to the client.

Method 2 inserts a row into `system.statement_diagnostics_requests` and also
adds the request to the local statement diagnostics registry. Other nodes poll
`system.statement_diagnostics_requests` and add any new requests to their local
registries. When executing a statement, each node checks with its local registry
whether the statement matches a diagnostics request fingerprint, and if so
proceeds with method 1 (except returning normal query results instead of
download instructions).

Method 3 uses a gRPC message to trigger the insert into
`system.statement_diagnostics_requests` instead of a SQL function, but otherwise
follows the same steps as method 2.

Method 4 POSTs a SQL statement inserting into
`system.statement_diagnostics_requests` to `/api/v2/sql/` instead of using a
gRPC message or a SQL function, but otherwise follows the same steps as method
2.

All four methods require either the `ADMIN` role option, or the `VIEWACTIVITY`
role option and absence of the `VIEWACTIVITYREDACTED` role option.

### Contents

The contents of statement diagnostics bundles are various files recording
information about the execution of the statement. These files are currently
mostly unredacted*, and may include potential PII.

(*Redaction of the KV portion of traces was added for multi-tenant clusters in
PR #70562.)

| File(s) | Description | Customer Information | Sensitive Customer Information |
|-|-|-|-|
| `distsql.html` | DistSQL plan diagram (output of `EXPLAIN (DISTSQL)`) | (see statement.txt) | (see statement.txt) |
| `env.sql` | cluster and session settings | some cluster settings (e.g. `cluster.organization`) | values of all string-typed settings |
| `opt*.txt` | optimizer plan (output of variants of `EXPLAIN (OPT)`) | (see statement.txt and stats*.sql) | (see statement.txt and stats*.sql) |
| `plan.txt` | plan annotated with execution stats (output of `EXPLAIN ANALYZE`) | (see statement.txt) | (see statement.txt) |
| `schema.sql` | `CREATE` statements for all referenced schemas, tables, views, and sequences | table and column names, view definitions, default values, constraints, partition keys, table comments | constants |
| `statement.txt` | the statement, formatted | table and column names, constants, placeholder values | constants, placeholder values |
| `stats*.sql` | stats for all referenced tables (output of `SHOW STATISTICS USING JSON`) | table and column names, constants in histograms | constants |
| `trace*` | execution trace of the statement in various formats | range keys, key spans, row data, table and column names, user name, constants | constants and other row data |
| `vec*.txt` | vectorized execution plan (output of variants of `EXPLAIN (VEC)`) | none | none |

## Redaction

Initially we will add two options controlling redaction of statement diagnostics bundles:
1. `REDACT` will completely remove all sensitive customer information, replacing
   it with `‹×›` (analogous to the `redact` logging configuration).
2. `REDACTABLE` will surround sensitive customer information with redaction
   markers `‹` and `›`, but will not remove it (analogous to the `redactable`
   logging configuration).

If both options are used, `REDACT` will take precedence (as it does currently in
logging configurations).

These options will be available for all methods used to trigger collection of
bundles:
1. As additional options in `EXPLAIN`, e.g. `EXPLAIN ANALYZE (DEBUG, REDACT)`.
2. As a string of comma-separated options in another overload of built-in SQL
   function `crdb_internal.request_statement_bundle`, e.g.
   ```sql
   SELECT crdb_internal.request_statement_bundle('SELECT _', 1.0, '1 ms', '1 day', 'REDACT');
   ```
3.
4.

Furthermore, collecting statement diagnostics bundles with the
`VIEWACTIVITYREDACTED` role option will now be allowed, in which case the
`REDACT` option will automatically be applied.

## Unredaction

During an investigation it can be surprisingly difficult to collect a "smoking
gun" statement diagnostics bundle actually showing the phenomenon of
interest. Sometimes it is not clear which specific statement should be
investigated, and so bundles are collected for multiple statements. Even when
the specific statement is known, different executions of the same statement may
use different query plans, visit different nodes, or read different data. Often
multiple bundles must be collected to find one that shows the problem.

Furthermore, reproduction steps must often be taken before collecting the
bundle, such as changing cluster settings, adding or removing indexes, turning
application load on or off, etc.

Redaction of statement diagnostics bundles will make this process more
onerous. Suppose that after a long series of steps to reproduce a problem, a few
bundles are collected, and one of these seems to show the problem. If the bundle
is redacted, after a few rounds of emails, engineering may ask for an unredacted
bundle to investigate further. Now the customer must go through the same long
series of steps (perhaps days or weeks later) to try and obtain another bundle
showing the problem again, this time unredacted.

The `REDACTABLE` option could be used to make this less painful. The customer
would collect a redactable bundle, use a tool to create a redacted copy, and
would initially share this redacted copy with Support. If the unredacted bundle
were needed the customer could provide the original. But this would require the
customer to do additional work, and to hold on to the original bundle until the
investigation was over.

To make this process easier for the customer, we propose a new option:
- `UNREDACTABLE` will include an encrypted copy of the original unredacted
  bundle in the redacted bundle. Instead of keeping the original bundle, the
  customer only has to keep the key that will decrypt the unredacted bundle, and
  can easily provide this if an unredacted bundle is needed.

## Obfuscation
