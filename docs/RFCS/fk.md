- Feature Name: Foreign Keys
- Status: completed
- Start Date: 2016-04-20
- Authors: David Taylor
- RFC PR: [#6309](https://github.com/cockroachdb/cockroach/pull/6309)
- Cockroach Issue: [#2968](https://github.com/cockroachdb/cockroach/issues/2968)

# Summary
Support REFERENCES / FOREIGN KEY constraints to enforce referential integrity
between tables.

Specifically, if a column (the "referencing" or "child" column) declares that it
`REFERENCES` a another column, often in another table, (the "referenced" or
"parent" column), then:
  * a value `X` may only be inserted into the child column if `X` also exists in
  the parent column.
  * a value `X` in a child column may only be updated to a value `Y` if `Y`
  exists in the parent column.
  * a value `X` in the parent column may only be changed or deleted if `X` does
  not exist in the child column.

In the default (`RESTRICT`) case, operations that would violate one of the above
rules fail. Alternative behaviors can be specified though e.g. `CASCADE`.

Note: For simplicity, this specification primarily discusses references as being
between a referencing and referenced _column_, but it can also be between
matching _lists of columns_, in which case the values mentioned above are tuples
of values, but the semantics are the same.

# Motivation
One of the major advantages of a transactional database is the ability to
maintain consistency between separate but related collections.
Server-enforced referential integrity takes this even further, by preventing
even misbehaving applications from creating inconsistency between collections.

# Detailed design

## Indexes
Enforcement of foreign key relationships primarily consists of looking for the
presence (or absence) of specific values in specific columns, making indexes on
those columns highly desirable. While some databases encourage but do not
require indexes on both referenced and referencing columns, making them
mandatory makes the implementation easier to reason about.

Values in a referenced column must be unique, otherwise it would be ambiguous
which  instance of a duplicated value was the one referenced and thus subject to
the foreign key restrictions. In practice, uniqueness enforcement is implemented
via an index, so the requirement of an index on the referenced column was already
implied by the uniqueness requirement.

Allowing the referencing column to be unindexed has obvious performance
drawbacks (operations on the referenced table would need to perform table-scans).
Implicitly adding an index however can also easily lead to unpleasant surprises for
operators if it changed performance characteristics without notice, and it is
unclear what would be expected if they constraint were dropped -- leave an
orphaned index no one explicitly created, or implicitly drop it, possibly
causing serious  changes in query execution?

Requiring indexes exist ensures that lookups are performant and makes the
implementation simpler, since it can rely on their presence, while requiring they
be explicitly created by operators, avoids any surprising or unexpected changes.

## Tracking FK Relationships
When a column references another column, the indexes on both that column and on
referenced column are annotated to reflect that relationship:

```proto
message ForeignKeyRef {
  optional uint32 table = 1
  optional uint32 index = 2
  optional bool unvalidated = 3
  optional string constraint_name = 4
}

message IndexDescriptor {
...
  optional TableAndIndexID foreign_key = 8;
  repeated TableAndIndexID referenced_by = 9;
```

When and how these index descriptors are modified is discussed below.

On insert or update, if the descriptor for the index on the written column
indicates that it references another table, the presence of a matching value in
that table is checked.

On delete or update, for each index mentioned in the `referenced_by` in the
descriptors of the indexes on the written table, the presence of a referencing
value is checked.

Batched inserts or modifications of many rows could potentially be validated via
a single range lookup or even a JOIN with the referenced table as a performance
optimization.

## Creating and Modifying FK constraints
There are three rough classes of modification to FK constraints:
* Creating a new table containing a column with a constraint.
* Adding a new column with a constraint to an existing table.
* Adding a new constraint to an existing column.

In all three cases, the operation succeeds iff there are no existing
violations and no write can later succeed that would create a violation.

In the cases that add a new element (a new table or new column in an existing
table) that references an existing column, the new element must only become
public (i.e. accept reads/writes) once referential integrity is ensured, meaning
all nodes are aware of it and have started applying integrity checks to
operations on the referenced column. Specifically, the steps to add a new element
are:
1. Add the new element and its index in a non-public state.
  *  If adding a column to an existing table, wait for backfill of default values, then scan for violations.
1. Wait for all nodes to be aware of the new (hidden) element.
1. Update the referenced parent table, noting the reference from the (hidden) element.
1. Wait for all nodes to be aware of the new version of the parent table.
1. Update the new element to be public.

When adding a new constraint between *existing* columns, the process changes
slightly, in that the constraint itself is the one with an extra, intermediate
state:
1. Update the child table descriptor to include the outbound reference,
 but mark the reference indicating it is not validated.
  * Whether or not unvalidated constraints should be considered public is an open question.
1. Update the referenced parent table, noting the reference.
1. Wait for all nodes to be aware of the new versions of the both tables.
1. Scan to find existing violations.
1. Update the child table to mark the constraint as validated.

### Concerns Handling Related Edits to Multiple Tables
Unlike the process described in the F1 paper, Cockroach leases individual table
descriptors instead of whole schemas. For isolated changes on individual tables
this difference is minor, but it complicates FK changes, which make related
alterations to multiple tables at once: the system ensures that at-most-two
versions of a particular table are active at the same time in the cluster, but
does not, directly, offer guarantees about separate tables -- thus the process
for making related changes to multiple tables must at times wait until all nodes
are using a particular version of one descriptor before initiating a change to
another.

Transactional modification of multiple table descriptors makes modifying FK
relationships much more difficult, since it needs to be denormalized to both
of the referencing and referenced table. Managing updates to a single record
per-database, e.g. a list of all the FK relationships or even just a single
record with all the table descriptors embedded in it, might be easier to reason
about, but would require substantial refactoring for DB-level leasing.
See discussion on #7508.

# Drawbacks
The addition of the checks before scanning for existing violations means those
checks may reject some writes but later be reverted when the scan finds a
violation, and the rejected writes would mention a constraint that was never
(successfully) created. (more discussion in Alternatives).

# Future Work
## When to Evaluate References
In the simplest case, which will be implemented first, as each row is written,
any columns it references or is referenced by are checked for violations. This
however means that other writes in the same statement may or may not be visible
to those checks.

The `DEFERRED` keyword allows delaying checks until the transaction is committed,
thus including the effects of any other statements in the transaction (and is
thus required for handling circularly dependent values).

Even without `DEFERRED`, Postgres allows referencing a value inserted in the
same statement, even if it appears later in the statement, meaning it must stage
all the writes for a statement before attempting validation. Implementation of
this behavior would incur additional runtime overhead though, as checks and/or
writes would need to be buffered.

Feedback from early users of foreign-key constraints, using the simpler row-by-row
validation, can guide which, if either, of these behaviors we should implement.

## `CASCADE` and other behaviors
ORM compatibility and user feedback will likely guide which, if any, of the
configurable behaviors like `CASCASE`, `SET NULL`, or `SET DEFAULT` we want to
implement as enhancements to the basic, `RESTRICT`-only implementation.
