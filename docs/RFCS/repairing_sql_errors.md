- Feature Name: Repairing SQL Errors
- Status: draft
- Start Date: 2017-11-20
- Authors: Joey Pereira
- RFC PR: [#20293](https://github.com/cockroachdb/cockroach/pull/20293)
- Cockroach Issue: [Scrub SQL Consistency Check RFC][scrub command rfc]


# Summary

This RFC proposes methods to repair SQL data errors that have been
detected by the [SCRUB command][scrub command rfc]. The primary goal of
the repair process is to bring the database into a usable state, which
may not have been possible in some situations prior to this RFC.

The command, the errors being repaired, and the strategy for repairing
are discussed in this RFC. The strategy for repair suggested which
matches the motivations consists of directly deleting bad data from the
database.

[scrub command rfc]: 20171025_scrub_sql_consistency_check_command.md


# Motivation

Several data errors can happen during the lifetime of the database and
while rare, are bound to happen. Many of these errors are mentioned in
the [scrub command RFC motivation section][scrub_command_rfc#motivation].

Not only do we need to find errors, but we need to resolve them. This is
crucial for disaster recovery where errors may cause data may become
inaccessible. Correctness issues may also surface as a result of errors.

For example, if a non-nullable column has a NULL value the database will
abort the scanning process immediately upon encountering it. This makes
it difficult to do any database-wide operation that scans that row. Also
without knowing the exact key or row to avoid, it becomes manual work to
search for the bad row. Even after finding it, many commands do not
provide a way to avoid certain keys, such as backup.

This leads to the core motivations for this RFC which is to repair
errors that would make the database unusable. Whether the usability
problems are because of correctness problems or being unable to access
data, if it was caused by a data error then scrub is responsible for
detecting it and repairing.

The first strategy for repairing data is where any bad data is fixed, if
possible, otherwise, it is deleted. This strategy is aimed towards a
simpler, first iteration of a repair process. This is to help any
immediate problems that would outright prevent data access. This method
is not meant to provide any capabilities to the database operator to
recover data, but it should be noted that any data removed will be
logged so an operator can manually recover data.

To facilitate recovery of data, another strategy is mentioned in the
alternatives section. This strategy is where bad data is first
quarantined, bringing the database into a usable state. The operator
then can go through an interactive process to repair or recover the
data. It is meant to be more of a tool for database operators, aimed at
fixing data issues while also providing an easy way to recover any of
the bad data.

[scrub_command_rfc#motivation]: 20171025_scrub_sql_consistency_check_command.md#motivation


# Guide-level explanation


## Running a repair

Repairing is an extension of the already existing `SCRUB` command. The
syntax of repairing with `SCRUB` statements is the following, where
`<error_type>` are the errors that will be repaired:

```sql
EXPERIMENTAL SCRUB TABLE <table> [WITH OPTIONS ...] WITH REPAIR [(<error_type>...)]
```

When no error types are provided, the default behavior is to repair all
errors encountered. This includes constraint violations.

For a list of all the possible values for `<error_type>`, refer to the
[scrub command RFC error types section][scrub_command_rfc#error-types].

[scrub_command_rfc#error-types]: 20171025_scrub_sql_consistency_check_command.md#error-types

### Examples

To repair all errors encountered, `WITH REPAIR` can just be provided. In
this example, the statement will repair all errors found while just
checking a table for constraint violations:

```sql
SCRUB TABLE mytable WITH OPTIONS CONSTRAINT ALL WITH REPAIR
```

Instead, a list of `<error_type>` can be specified to only repair
certain types of failures. In this example, we search for constraint
violations and physical data errors but only attempt to repair encoding
errors:

```sql
SCRUB TABLE mytable WITH OPTIONS CONSTRAINT ALL, PHYSICAL WITH REPAIR (encoding)
```

If any `<error_type>` specified does not result from any of the types of
checks being run, then the command will fail. In this example, we
specify to only check physical data errors but we also specify to fix
constraint errors.

```sql
SCRUB TABLE mytable WITH OPTIONS PHYSICAL WITH REPAIR (constraint)
error: "attempting to repair error type that is not being checked: constraint"
```


## Data loss during repairs

While many cases of repairing the database will not cause data loss,
there is no guarantee. Because of this, there are two measures in place
to assist an operator in recovering data that has been removed during
repair.

1) All errors found, whether or not they were repaired, will be returned
   by the `SCRUB` statement. Each error in the results will have all the
   data that was retrievable from the data. These results will also be
   inserted into the system table `system.scrub_errors`, for historical
   reference.

2) Any action taken by repair will be verbosely logged. Log messages
   will contain similar information as the returned values in order to
   provide all information that was contained in CockroachDB.


# Reference-level explanation


## Modifications to scrub execution

The repair procedure can piggyback the results already produced while
running scrub checks.

The following changes are proposed for adding interfaces for the repair
process on top of the existing scrub interfaces. The rationale for
defining a separate interface is to separate the concerns of repair from
the rest of the scrub command:

- Add `checkResult` interface that each check operation will define a
  structure for. The structures will hold any intermediate information
  needed to:

  - Produce a row to return to the user, describing the error. This will
    be done through a `checkResult.ProduceResult() -> tree.Datums`
    interface.

  - Repair the error. Done with `checkOperation.Repair(checkResult)`,
    which will pull out the specific struct type.

- Modify `checkOperation.Next() (tree.Datums, error)` interface to
  instead be `checkOperation.Next() (checkResult, error)`.

- Add `checkOperation.Repair(checkResult)` which is called on
  checkResults if `checkResult.Type()` is an error type that is being
  repaired. SQL or KV operations may result from this, in order to
  repair the problem.


## Repairing data

As mentioned, the repair strategy proposed by this RFC consists of
trying to fix errors and deleting the errors if it cannot. This achieves
the goal of bringing the database to a usable state in the simplest
manner. An alternative,
[[1] Quarantining bad data](#1-quarantining-bad-data), is also mentioned
as a method to provide a way to assist operators in manually fixing
errors.

The below sections outline how each type of error will be thought of and
repaired. It mentions the best action to take given this strategy and
the possible causes for the error which motivate the action.


### Missing secondary indexes (`missing_secondary_index`)

When we detect any missing secondary index entries, we will always
create the the secondary index entry.

Two cases could have happened:
1) We failed to create the index entry when inserting or updating a row.
2) We failed to remove the primary key during deletion.

When the error happens because of the first case, the correct action
would be to create the index entry.

Unfortunately, we cannot discern the difference between the case 1 and
case 2. We will still create the secondary index for case 2, but any of
these errors detected will be logged in the scrub output. It is up to
the operator to thoroughly review the errors and repairs to discern
these two cases.


### Dangling index reference (`dangling_secondary_index`)

If a dangling index reference is detected, we will always delete the
index entry. Because of this, the repair may result in data loss.

When this error happens, it could have been caused by:
1) The secondary index entry was not deleted when the row was deleted.
2) The row was updated and the secondary index was not updated.
3) The row was updated and the secondary index was updated but the
   primary index was not.
4) The primary data may have been erroneously deleted while the
   secondary index entry remained.

For the cases 1 and 2 the primary index data will be correct, so it is
safe to delete the secondary index entry. It should be noted that the
index may still not correct after deleting the entry, as a new entry
needs to be made -- this will be automatically caught and repaired as
the `missing_secondary_index` error.

Cases 3 and 4 are more complicated. If multiple secondary indexes all
point to a missing primary index entry then what may have happened was
that the primary index entry was erroneously modified while the
secondary index entries remain correct. Because of this, deleting the
entries will result in data loss or correctness issues. It is up to the
operator to discern these cases. In case 3, the row in CockroachDB will
need to be deleted then re-added with the correct data, while case 4 it
will just need to be added.


### SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)

As these constraints correspond to user-level data, there is no best
action for repair. When any errors corresponding to SQL constraint
violations are set to be repaired, the rows with violations will be
removed. All the row data will be logged, for the operator to manually
fix them.

Alternatively, the operator can run scrub without repairing these
violations and then correct them manually.


### Encoding or other KV problems

There are lots of errors that can occur at the encoding level. They all
pertain to problems with how CockroachDB has stored or retrieved the SQL
data from the underlying KV store. The exact errors are not yet final,
and will be influenced by an in-progress PR for checking physical data
errors, [#19961].

Unfortunately, in these circumstances, we may not be able to retrieve
much data. Moreover, the data may not have corresponded to anything
meaningful to the user and could have been entirely internal to
CockroachDB.

If an encoding error is found on a secondary index, a simple repair
action will be to regenerate the secondary index entry.

If the error is instead on the primary index, we will delete the key
with the error. If the key-value pair is able to decode at all into SQL
values, they will be logged.

[#19961]: https://github.com/cockroachdb/cockroach/pull/19961


## Rationale and Drawbacks

The strategy presented is more aggressive in deleting data. It was
chosen for dealing with errors as it solves the key motivation, bringing
the database into a correct and usable state, even if it results in data
loss.

All of the repair operations are presented here with a big red flag. In
many of the error situations above, multiple things that could have gone
wrong and require a human to investigate them.

While data loss may occur, this repair process does ensure all data that
is removed will be appropriately logged and reported so that an operator
manually address the problems.

Without taking this first (rather minimal) step in repairing, there are
situations where issues may surface where a database may be inaccessible
and there is no simple or quick way to gain access again.


## Alternatives and Future Work

As most of the repair process is internal it is easy to consider these
as both alternatives and future work. They serve as ideas that were not
completely explored or not considered for the initial repair feature as
their scope extends outside of just the motivations mentioned.


### [1] Quarantining bad data

In all of the error cases, there is a risk of deleting good data or
introducing bad data. As an alternative strategy, we can instead
quarantine any k/v that we would normally repair, regardless of whether
we were going to delete or update it.

The benefit of this strategy is that it gives the operator more tools to
repair data rather than letting the entire process be manual. All
quarantined errors could then be repaired through an interactive repair
process that the operator is guided through. This means that we could
provide multiple choices of repair to take on quarantined data that the
operator decides what to do.

This process can guide the operator through whether it is okay to delete
the data with no data loss, or how to reconstruct the data.

As for implementing this alternative, it would be possible with a system
table, `quarantine`, and a reserved key space, `/Qaurantine/` for
quarantined keys.

In order to move a key into the reserved key space, we could prepend the
following to the existing key: `/Quarantine/<scrub_job_uuid>`. The
reason we want to incorporate the `scrub_job_uuid` is if an error with
the same key happens in the future and would collide.

The system table entries would store metadata pointing to the
quarantined key and information about the error such as the descriptors
at the time of the error.


### [2] Repairing previously found errors

A possible idea may be to provide a way to repair errors that were
previously found. The following is an example of what a command may look
like:

```sql
EXPERIMENTAL REPAIR SCRUB ERROR <scrub_error_id>
```

There are several potential problems when attempting to repair an error
previously found, which make this feature difficult or undesirable. In
particular, if the error was already fixed. Attempting to repair a fixed
error could impact existing user data.


# Unresolved questions

- Can we provide a way to repair data for errors detected for a past
  scrub run? This might prove difficult when the same error was detected
  multiple times and has already been repaired, or if the data has since
  changed.


- Logging or returning information from the removed data while repairing
  may be flawed. We are string serializing all of the information, which
  will be lossy. Is there a better medium for presenting recovery data?
  Since we can export all data types as CSV, this might not actually be
  lossy.


- What do we do when we are unable to decode or process a k/v to get SQL
  data? Do we surface the raw bytes? Do we need any additional context
  in order to decode it, e.g. table, column, or index descriptors?


### Can manual repair happen in the same transaction as `SCRUB`?

Given that data will be outright removed and an operator may want to
repair and re-insert data, it may be important to provide the ability to
do all of that within the same transaction as scrub repairs.

Is this something that seems possible, if it is not already?

e.g. if do the following

```sql
BEGIN;
EXPERIMENTAL SCRUB TABLE <table> WITH REPAIR
# ... get results and investigate them
# insert a row that was removed by repair
INSERT INTO <table> VALUES (...)
COMMIT;
```

Will this happen all in one transaction? Does this always abort if there
were any reads or writes to the row after it was removed in the scrub
command? If it does, does this also end up retrying `SCRUB`, depending
on the abort?


### Can a repair fail?

This seems to be a question related to the type of repairing we aim for.
In the case of "delete anything", there is no reason repairing should
fail if the database is operating correctly.

If a repair is correctional, there is a chance the correction fails.
Because there a correctional strategy is not suggested here, this does
not seem too applicable.


### Should there be an abstraction/grouping of the error types being repaired?

For example, when running secondary index checks we need to repair both
`dangling_secondary_index` and `missing_secondary_index`, otherwise,
there will still be correctness issues. These could instead just be
grouped into something like `WITH REPAIR (index)`.

The same could apply to physical data errors, as they are all the same
to the user -- errors that may break the correctness when encountering
that data, or prevent access to data.

Constraint violations are the only errors where grouping is undesirable,
where the error types may be `nonnullable_violation`,
`check_constraint_violation`, `foreign_key_violation`.


### Repairing historical values

Discussing repairing historical values can get fairly tricky.

Consider the following situation:

A new index is created on an existing table, and the index is backfilled
at the current timestamp. If a query is made against the index at a
timestamp prior to when index values were backfilled, any index entries
expected will be missing.

While this (hopefully) is not allowed in CockroachDB currently, a
similar situation can happen to other processes that add new
expectations or constraints that historical queries would violate.

In this case, historical queries will be incorrect due to data errors.

What can we do, if we can do anything about it?
