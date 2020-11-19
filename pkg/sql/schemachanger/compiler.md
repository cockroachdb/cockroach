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
