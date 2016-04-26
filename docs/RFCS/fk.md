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
  One of the major advantages to a tansactional DB is the ability to maintain
  consistency between separate but related collections.
  Server-enforced referential integrity takes this even further, by preventing
  even misbehaving applications from creating inconsistency.

  # Detailed design

  ## Enforcing FKs in steady-state
  In the steady-state, the column descriptor for a column that references a column
  in another table would include that reference and the referenced column's
  descriptor would  also note that it is referenced.

  When and how these are these descriptors are modified is discussed below.

  On insert or update, if the descriptor for the written column indicates that
  it references another table, presence of a matching value in that table is checked.

  On delete or update, for each inbound reference noted in the descriptor for the
  written table, the presence of a referencing record is checked (or a matching
  delete is added to the batch, depending on the configured action).

  ## Creating and Modifying FK constraints
  There are three rough classes of modification to FK constraints:
  A) Adding a new constraint to an existing column.
  B) Creating a new table containing a column with a constraint.
  C) Adding a column with a constraint to an existing table.

  In all three cases, the operation succeeds iff there are no existing
  violations and no write can later succeed that would create a violation.
  B and C are more difficult through, as the associated schema change cannot
  appear to succeed (i.e. accept writes to the new column or table) while there
  is still a possibility of the change failing.

  ### Adding a constraint to an existing column (A)

  1. Optionally scan for violations and fail early.
  1. Update both table descriptors to start rejecting writes and wait until all nodes have new version.
  1. Scan to find existing violations. If any are found revert descriptor
  changes and fail.

  ### Creating a new table containing a column with a constraint.
  This can essentially be reduced to the first case by just creating the table
  first, except that the table cannot be usable by other clients until it
  completes, thus the addition of a hidden state:<br>
  1. Create the new table including the constraint, but mark it hidden to prevent writes.
  1. Update parent table to start checking on delete or update operations (The check should never fail, as the child table is hidden and thus empty).
  1. Unhide the child table.

  ### Adding a column with a constraint to an existing table
  This is the trickiest case. It can be essentially reduced to a regular column-add
  followed by addition of a constraint to an existing column (case A) and a
  failure of the constraint addition would rollback the schema mutation. Again
  though, care must be taken to hide the schema change until the constraints are
  in place to prevent rolling back an accepted write:

  1. Start an AddColumn mutation, and wait for it to backfill default values, but do not update the table descriptor yet to prevent use.
  1. Follow (A) except that checks and scans will need to find the proposed column in the table's pending `modifications` as well rather than in the descriptor's columns.
  1. Apply another descriptor change to commit the addition of the column.

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

  # Unresolved questions
