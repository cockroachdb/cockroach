  - Feature Name: Foreign Keys
  - Status: draft
  - Start Date: 2016-04-20
  - Authors: David Taylor
  - RFC PR: (PR # after acceptance of initial draft)
  - Cockroach Issue: 2968


  # Summary
  Support REFERENCES / FOREIGN KEY constraints to enforce referential integrity
  between tables.

  # Motivation
  One of the major advantages to a transactional DB is the ability to maintain
  consistency between separate but related collections.
  Server-enforced referential integrity takes this even further, by preventing
  even misbehaving applications from creating inconsistency.

  # Detailed design

  ## Enforcing FKs in steady-state
  In the steady-state, the column descriptor for a column that references a column
  in another table would include that reference and the referenced column's
  descriptor would  also note that it is referenced.

  When and how these descriptors are modified is discussed below.

  On insert or update, if the descriptor for the written column indicates that
  it references another table, presence of a matching value in that table is checked.

  On delete or update, for each inbound reference noted in the descriptor for the
  written table, the presence of a referencing record is checked (or a matching
  delete is added to the batch, depending on the configured action).

  As mentioned below, FK relationships will only be allowed between indexed
  columns -- postgres and other DBs require that the column referenced have a
  unique index and strongly suggest the column referencing it be indexed as well,
  to make update and delete checks from the referenced column performant.
  Making the latter also mandatory avoids the potential for misuse.

  Batched inserts of `k` rows can do a single query with a k-term `IN` clause
  (or a similar multi-key lookup) to check the referenced table whereas
  row-by-row inserts will need to do `k` queries.

  ### DEFERRED
  Initially all checks will be at statement execution while support for `DEFERRED`
  checks, to enforce the constraint at transaction commit instead, can be added
  later.
  DEFFERED is required for circular FK relationships and can potentially
  allow batching checks for multiple statements.

  ## Creating and Modifying FK constraints
  There are three rough classes of modification to FK constraints:
  A) Adding a new constraint to an existing column.
  B) Creating a new table containing a column with a constraint.
  C) Adding a column with a constraint to an existing table.

  In all three cases, the operation succeeds iff there are no existing
  violations and no write can later succeed that would create a violation.

  In the cases that add a new element to the schema (B and C), that element must
  only become public (i.e. accept reads/writes) once integrity is ensured,
  meaning all nodes have started applying the integrity checks to operations on
  the referenced table. As in the F1 process, this is ensured by using an
  and intermediate step where the new element exists for the purposes of the
  checks but is not yet public.

  The existing column addition process includes these intermediate steps. Table
  creation currently does not, but tables do have a `deleted` state currently
  only used during drop. Changing the boolean deleted to a direction -- add or
  drop --  should allow it to be used during addition too.

  Finally, unlike the process described in the F1 paper, Cockroach leases
  individual table descriptors instead of whole schemas. For isolated changes on
  individual tables this does not matter as much, but it complicates FK changes,
  which must alter multiple tables at once -- e.g. the change adding the new,
  non-public table must be completed by all nodes before the change adding
  checks to the referenced table can begin.

  ### Indexes
  In all cases the referenced column(s) must also have a unique index, and the
  constrained column(s) must be indexed as well. The indexes' descriptors are
  updated to note that they are depended on by the constraint, preventing them
  from being dropped.

  ### Adding a constraint to an existing column (A)

  1. Optionally scan for violations and fail early.
  1. Schema-change to add constraint in check-only (non-public) state.
  1. Scan to find existing violations. If any are found revert the change and fail.
  1. Schema-change again to make the constraint public.

  ### Creating a new table containing a column with a constraint. (B)
  This can essentially be reduced to the first case by just creating the table
  first, except that the table cannot be usable by other clients (public) until
  it completes. The checks will need to handle checking a non-public table.

  1. Schema-change to add the table and check but not make them public:
    1. Create the new table including the constraint.
    1. Update parent table to start checking on delete or update operations
       (The check should never fail, as the child table not yet public and thus empty).
  1. Wait for all nodes to pick up change.
  1. Schema-change again to make the new table (and the constraint) public.

  ### Adding a column with a constraint to an existing table (C)
  This is the trickiest case. It can be essentially reduced to a regular column-add
  followed by addition of a constraint to an existing column (case A) and a
  failure of the constraint addition would rollback the schema mutation. Again
  though, care must be taken to hide the schema change until the constraints are
  in place to prevent rolling back an accepted write:

  1. Start an AddColumn mutation, and wait for it to backfill default values, but do not make it public.
  1. Follow (A) except that checks and scans will be checking a non-public column.
  1. Apply another schema change to make the column, and constraint, public.

  # Drawbacks
  The addition of the checks before scanning for existing violations means those
  checks may reject some writes but later be reverted when the scan finds a
  violation, and the rejected writes would mention a constraint that was never
  (successfully) created. (more discussion in Alternatives).

  # Alternatives
  The bulk of the complexity is around transactional modification of multiple
  table descriptors since the FK relationship needs to be denormalized to both
  of them. Managing updates to a single record might be easier, e.g. list of FK-
  relationships stored on the DB descriptor, but this would require adding lease-
  management infrastructure to that as well.

  A more complex approach to adding constraints (FK and otherwise) to existing
  columns would be to first install a check that simply noted violations, scan
  values, check if any violations were logged by the check, then swap the check
  to reject. The extra states add complexity, but avoid returning errors related
  to a check that never successfully applies.

  An alternative to requiring the indexes exist would be implicitly creating them
  when a constraint is added. As with any implicit behavior though, could be
  surprising to a user. Also ambiguous is what to do if, e.g. the constraint is
  dropped -- automatically dropping the index as well could lead to a serious
  performance regression or outage.

  # Unresolved questions
