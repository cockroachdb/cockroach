- Feature Name: Foreign Keys
- Status: in-progress
- Start Date: 2016-04-20
- Authors: David Taylor
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: 2968


# Summary
Support REFERENCES / FOREIGN KEY constraints to enforce referential integrity
between tables.

Specifically, if a column in a table (the "referencing" or "child" table)
declares that it `REFERENCES` a column in another table ("referenced" or
"parent" table), then:
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
those columns highly desireable.

Values in a referenced column(s) must be unique, otherwise it would be ambiguous
which  instance of a duplicated value was the one referenced and thus subject to
the foreign key restrictions. As uniqueness enforcement is already via indexes,
the referenced column(s) can thus already by assumed to be indexed.

Some databases allow specifying foreign key relationships with the referencing
columns unindexed, causing enforcement during operations on the parent table to
scan the child table while others implicitly create indexes. Both of these
approaches have drawbacks: table scans have obvious performance concerns and
implicitly changing the schema could easily lead to unpleasant surprises for a
user.

Explicitly requiring indexes exist ensures both that lookups are performance and
that administrators are not surprised by unexpected indexes appearing (or worse,
disappearing) when foreign key relationships are modified.

## Tracking FK Relationships
When a column references a column in another table, the indexes on both that
column(s) and on referenced column(s) are annotated to reflect that relationship.

```proto
message TableAndIndexID {
  optional uint32 table = 1 [(gogoproto.nullable) = false];
  optional uint32 index = 2 [(gogoproto.nullable) = false];
}

message IndexDescriptor {
...
  optional TableAndIndexID fk = 8;
  repeated TableAndIndexID referenced_by = 9;
```

When and how these descriptors are modified is discussed below.

On insert or update, if the descriptor for the written column indicates that
it references another table, presence of a matching value in that table is checked.

On delete or update, for each inbound reference noted in the descriptor for the
written table, the presence of a referencing record is checked (or a matching
delete is added to the batch, depending on the configured action).

Batched inserts or modifications of many rows could potentially be validated via
a single range lookup or even a JOIN with the referenced table as a performance
optimization.

### When to Evaluate References
In the simplest case, which will be implemented first, as each row is written,
any columns it references or is referenced by are checked for violations. This
however means that other writes  in the same statement may or may not be visible
to those checks.

Postgres allows referencing a value inserted in the same statement, even it it
appears later in the statement, meaning it must stage all the writes for a
statement before attempting validation.

The `DEFERRED` keyword goes even further, and allows delaying checks until the
transaction is committed, thus including the effects of any other statements in
the transaction (and is thus required for handling circularly dependent values).

## Creating and Modifying FK constraints
There are three rough classes of modification to FK constraints:
A) Creating a new table containing a column with a constraint.
B) Adding a new column with a constraint to an existing table.
C) Adding a new constraint to an existing column.

In all three cases, the operation succeeds iff there are no existing
violations and no write can later succeed that would create a violation.

In the cases that add a new element to the schema (A and B), that element must
only become public (i.e. accept reads/writes) once the referential integrity is
ensured -- thus all nodes must be aware of and be evaluating integrity checks
to operations on the referenced table. Similar to how indexes are added, this is
ensured by using an intermediate step during which the new element exists for
the purposes of validation but is not yet public. As the schema lease system
ensures that at-most-two versions of the schema is in used across the cluster,
it can therefore be assumed that all nodes are aware of and validating the
constraint before any node considers it public.

Unlike the process described in the F1 paper, Cockroach leases individual table
descriptors instead of whole schemas. For isolated changes on individual tables
this difference is minor, but it complicates FK changes, which make related
alterations to multiple tables at once, and if the change fails before, some
but not all of the changes may have been applied, thus each step for each table
but be valid even if no further steps apply successfully.

### Creating a new table containing a column with a constraint. (A)
1. Create child table, including the outbound reference, in a non-public ADD
state.
1. Wait for all nodes to be aware of the (hidden) table.
1. Update referenced parent table to include the child in the `ReferencedBy`, to
  start checking delete or update operations.
1. Wait for all node to be aware of the new parent table version.
1. Update the child table to the public state.

### Adding a column with a constraint to an existing table (C)
1. Start an mutation to add the new column and index do not make it public.
1. Wait for backfill of default values, then scan for violations.
1. Wait for all nodes to be aware of the new (hidden) column.
1. Update the referenced table to be include the new element in its `ReferencedBy`.
1. Wait for all nodes to be aware of the new version of the parent table.
1. Update the new column to be public.

### Adding a constraint to an existing column (C)
1. Optionally scan for violations and fail early.
1. Add the outbound reference to the child table, referencing the parent table,
but mark the reference non-public.
1. Update the referenced parent table to include the child in the correct
`ReferencedBy` and wait for all nodes to become aware of that change.
1. With validation of new operations in place on both tables, scan to find
existing violations.
1. Update the child table to make the constraint public.

# Drawbacks
The addition of the checks before scanning for existing violations means those
checks may reject some writes but later be reverted when the scan finds a
violation, and the rejected writes would mention a constraint that was never
(successfully) created. (more discussion in Alternatives).

# Alternatives
Transactional modification of multiple table descriptors makes modifying FK
relationships much more difficuly, since it needs to be denormalized to both
of the referencing and referenced table. Managing updates to a single record
per-database might be easier to reason about, but would require substantial
refactoring for DB-level leasing.

An alternative to requiring the indexes exist would be implicitly creating them
when a constraint is added. As with any implicit behavior though, could be
surprising to a user. Also ambiguous is what to do if, e.g. the constraint is
dropped -- automatically dropping the index as well could lead to a serious
performance regression or outage.
