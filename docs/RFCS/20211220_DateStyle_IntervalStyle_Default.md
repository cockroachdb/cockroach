- Feature Name: DateStyle/IntervalStyle Enabled by Default
- Status: in-progress
- Start Date: 2021-11-12
- Authors: Ebony Brown
- RFC PR: [#75084](https://github.com/cockroachdb/cockroach/pull/75084)
- Cockroach Issue: [#69352](https://github.com/cockroachdb/cockroach/issues/69352)

# Summary

This document describes the process of enabling the DateStyle/IntervalStyle
options by default. In its current state, date to string and interval to string
casts aren't restricted when the DateStyle/IntervalStyle session variables are
false. This allows for inconsistencies in formatting when the session variables
are true.

This is an example of how having IntervalStyle set can lead to a corrupt
computed index.
```sql
CREATE TABLE t (
    it interval,
    computed string AS ((it + interval '2 minutes')::string) STORED
);

INSERT INTO t VALUES ('12:34:56.123456');
SELECT  * FROM t;
        it          |       computed
--------------------+---------------------
PT12H34M56.123456S  | PT12H36M56.123456S

SET intervalstyle_enabled = true;
SET intervalstyle = 'sql_standard';

INSERT INTO t VALUES ('12:34:56.1234');
SELECT  * FROM t;

it               |       computed
-----------------+---------------------
12:34:56.123456  | PT12H36M56.123456S
12:34:56.1234    | 12:36:56.1234



```

This will be corrected by removing the session variables as well as rewriting
all instances of the violating cast in computed columns, index expressions and
partial indexes.

# Background



In v21.2 an [experimental feature](https://github.com/cockroachdb/cockroach/pull/67000)
was added to CockroachDB which allowed DateStyle and IntervalStyle to take on multiple
values. The change was experimental due to it affecting the volatility of cast
to and from the Date, Time, and Interval types. The casts were changed from
Immutable to Stable, meaning their results can change across SQL statements and
should not be supported in computed columns.

Currently, users can cast date and interval types to strings within a computed
column, enable the DateStyle feature and change formats. This leads to
formatting inconsistencies since there is no way to reformat these strings.

# Design

## Migration

In v22.1,we will start a long-running migration that queries for all
TableDescriptors on the cluster. We will iterate through the descriptors looking
at each column. For every computed column found, we will check the computed
expression volatility. If we find an expression that isn't immutable, we can
assume it's the violating cast and rewrite the expression. We will walk through
the expression, and using a visitor type assert the cast expressions into
function expressions. We found that the formatting issue could also affect
indexes with expressions and partial indexes. Indexes with expressions would be
accounted when iterating through computed however partial indexes would not. To
account for partial indexes, all partial indexes will be iterated over after the
computed columns. The rewriting will use the builtin `parse_interval` and
`to_char_with_style`, created during the original Date/IntervalStyle
implementation. Other date to string cast seem to be blocked, so we can focus on
the few instances that are not. After rewriting the casts they are batched
together and saved.


# Alternative Considered

We also considered a simpler approach which involved keeping the session
variables for one more release. If v21.1 is active, DateStyle/IntervalStyle
enabled variables will be ignored since they will be enabled by default. If not
we’d add a migration to the registry which checks the virtual table and if it is
populated blocks the upgrade. We’d issue an error that specifies that the
violating cast are no longer supported. The customers could use the virtual
table to determine what needs to be changed in order for the migration to be
finalized.

For CC, the SRE team would have to be notified as soon as possible about this
change. They’d then have to facilitate reaching out to the customers about
altering their data if they want to finalize the upgrade. We can then allow
customers to keep their date-string cast, if they enable DateStyle/IntervalStyle
we can warn them about the cast issue and leave the decision to them.

We also considered using a virtual table with a precondition when rewriting the
violating cast. The virtual table would contain all computed columns where there
are instances of date/interval type to string type casts. It would be populated
by iterating through the table descriptors of all public columns and creating a
row for every computed column containing a cast with stable volatility. This
would require a full KV scan which would be expensive. We found this step
wouldn't be necessary since we could iterate through the descriptors during the
long-running migration.


