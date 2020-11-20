# Compiler exploration

## Example 1

```sql
CREATE TABLE foo (i INT PRIMARY KEY, j INT);
CREATE TABLE bar (i INT PRIMARY KEY);
```

```sql
ALTER TABLE foo ADD COLUMN k INT UNIQUE;
ALTER TABLE foo ADD CONSTRAINT fk_bar_k FOREIGN KEY (k) REFERENCES bar(i);
```

* Builder
   * statement 1:
      * Add column in `DELETE_ONLY`
      * Add index in `DELETE_ONLY`
      * Elements:
         * addColumn{state: elemDeleteOnly}
         * addIndex{state: elemDeleteOnly, unique: true}
   * statement 2:
      * Add constraint mutation in `ADDING`
         * addForeignKeyOutbound{state: elemAdding, referencing: ..., columns: ...}
```go


// elem: addColumn{state: elemDeleteOnly}
// ops:
//  - addColumnChangeStateOp{nextState: elemDeleteAndWriteOnly}
//  - columnBackfillOp
//  - addColumnChangeStateOp{nextState: elemPublic}

	// elem: addUniqueIndex{state: addIndexDeleteOnly}
	// ops:
	//  - addIndexChangeStateOp{nextState: elemDeleteAndWriteOnly}
	//  - indexBackfillOp
	//  - addIndexChangeStateOp{nextState: elemAdded}
	//  - uniqueIndexValidateOp
	//  - addIndexChangeStateOp{nextState: elemPublic}
```

 * Elements will get created during statement execution.
 
```
CREATE TABLE bar (i INT PRIMARY KEY); -- (1)
BEGIN;
CREATE TABLE foo (i INT PRIMARY KEY); -- (2) 
ALTER TABLE foo ADD COLUMN j INT UNIQUE; -- (3) 
ALTER TABLE foo ADD COLUMN k INT REFERENCES bar(i); -- (4)
COMMIT; -- (5)
```

* (1) Synthesize descriptor. Write it to desc collection. Write it to store. Commit.
* (2) Synthesize descriptor. Write it to desc collection.
* (3) 
   * (a) Builder builds elements from descriptors and AST in initial state.
   * (b) Session's schema changer has a bunch of elements in their initial
         state (not on descriptors).
   * (c) CompileOps(oneStep, newlyCreatedTables{...}) and then run with
         descriptor mutation dependencies that just write in-memory
         and with backfill/validate dependencies which use the user transaction
         (or child which sees writes)
   * (d) After running, the index and column are public.
 * (4)
   * (a) Builder builds elements from descriptors for fk
   * (b) ...
   * (c) CompileOps(oneStep, newlyCreatedTables{...}) but the fk element
         references a pre-existing table so can only step it one time.
   * (d) At this point the fk forward and backwards references exist in ADDING.
 * (5) Detect whether there are ops left to run by CompileOps(allSteps) and
       seeing if it is non-empty.
       (a) Create job (details to follow)
 * (6) Write descriptors to store.

```
BEGIN;
CREATE TABLE foo (i INT PRIMARY KEY);
CREATE TABLE bar (i INT PRIMARY KEY);
ALTER TABLE foo ADD COLUMN j INT UNIQUE;
ALTER TABLE foo ADD COLUMN k INT REFERENCES bar(i);
COMMIT;
```

* (1) Synthesize descriptor. Write it to desc collection.
* (2) Synthesize descriptor. Write it to desc collection.
* (3) 
   * (a) Builder builds elements from descriptors and AST in initial state.
   * (b) Session's schema changer has a bunch of elements in their initial
         state (not on descriptors).
   * (c) CompileOps(oneStep, newlyCreatedTables{...}) and then run with
         descriptor mutation dependencies that just write in-memory
         and with backfill/validate dependencies which use the user transaction
         (or child which sees writes)
   * (d) After running, the index and column are public.
 * (4)
   * (a) Builder builds elements from descriptors for fk
   * (b) ...
   * (c) CompileOps(oneStep, newlyCreatedTables{...}) and both tables are new
         so you can compile all of the ops.
   * (d) At this point the fk forward and backwards references are public and
         validated.
 * (5) Detect whether there are ops left to run by CompileOps(allSteps) and
       seeing if it is non-empty.
       (a) Create job (details to follow)
 * (6) Write descriptors to store.

```sql
create table t(a int);
begin;
alter table t drop column a;
alter table t add column b int unique default 1; -- 2
commit;
-- a ID 2, b ID 3
```

* (1)
    * Builder:
        * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1}, State: elemRemoved, ReplacementOf: 1, Primary: true}`
        * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
        * `DropColumnTarget{ColumnID: 2, State: elemPublic}`
    * Post-statement execution step:
        * `a` in `delete-and-write-only`
    * Targets after step:
        * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1}, State: elemRemoved, ReplacementOf: 1, Primary: true}`
        * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
        * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
* (2)
    * Builder:
        * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemRemoved, ReplacementOf: 1, Primary: true}`
        * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
        * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
        * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemRemoved}`
        * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemRemoved, Unique: true}`
        * `AddUniqueConstraintTarget{IndexID: 3, State: elemRemoved}`
    * Post-statement execution step:
        * nothing
    * Targets after step:
        * same as above
* Pre-commit:
    * Post-commit execution step:
        * new primary index becomes `delete-and-write-only`
        * new column, index in `delete-only`
    * Targets after step:
        * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemDeleteOnly, ReplacementOf: 1, Primary: true}`
        * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
        * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
        * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemDeleteOnly}`
        * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemDeleteOnly, Unique: true}`
        * `AddUniqueConstraintTarget{IndexID: 3, State: elemRemoved}`
* Post-commit:
    * 1
        * Step
            * (descriptor changes below)
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemDeleteAndWriteOnly, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
            * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
                * If this column had constraints, they'd remain in the adding state for the duration of the schema change, so that they'd continue to be enforced through the duration of a rollback.
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemDeleteAndWriteOnly}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemDeleteAndWriteOnly, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemRemoved}`
    * 2
        * Step
            * backfill primary index (2)
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemBackfilled, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
            * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemDeleteAndWriteOnly}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemDeleteAndWriteOnly, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemRemoved}`
    * 3
        * Step
            * backfill unique index (3)
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemBackfilled, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
            * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemDeleteAndWriteOnly}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemBackfilled, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemAdding}`
    * 4
        * Step
            * validate unique index (3)
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemBackfilled, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemPublic}`
            * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemDeleteAndWriteOnly}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemBackfilled, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemValidated}`
    * 5
        * Step
            * make everything public, cut over to the new index
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemPublic, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemDeleteAndWriteOnly}`
            * `DropColumnTarget{ColumnID: 2, State: elemDeleteAndWriteOnly}`
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemPublic}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemPublic, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemPublic}`
    * 6
        * Step
            * drop the old PK
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemPublic, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemDeleteOnly}`
            * `DropColumnTarget{ColumnID: 2, State: elemDeleteOnly}`
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemPublic}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemPublic, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemPublic}`
    * 7
        * Step
            * drop the old PK
        * Targets after step:
            * `AddIndexTarget{IndexID: 2, ColumnIDs: []descpb.ColumnID{1, 3}, State: elemPublic, ReplacementOf: 1, Primary: true}`
            * `DropIndexTarget{IndexID: 1, ReplacedBy: 2, State: elemRemoved}`
            * `DropColumnTarget{ColumnID: 2, State: elemRemoved}`
            * `AddColumnTarget{ColumnID: 3, <column metadata>, State: elemPublic}`
            * `AddIndexTarget{IndexID: 3, ColumnIDs: []descpb.ColumnID{3}, ExtraColumnIDs: []...{1}, State: elemPublic, Unique: true}`
            * `AddUniqueConstraintTarget{IndexID: 3, State: elemPublic}`
