- Feature Name: ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE (requiring a CAST
or expression)
- Status: WIP
- Authors: Richard Cai
- Cockroach Issues: [#9851](https://github.com/cockroachdb/cockroach/issues/9851)
- Note that there is an [RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180411_alter_column_type.md) talking about ALTER TABLE ... ALTER COLUMN SET DATA TYPE more generally

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [High-level summary](#high-level-summary)
- [Detailed Explanation](#detailed-explanation)
    - [Column Backfill Method](#column-backfill-method)
    - [Index Backfill Method](#index-backfill-method)
- [Problems](#Problems)
- [Other considerations](#Other-considerations)
- [Alternatives](#alternatives)

# Summary
Support changing the type of a column for conversions requiring casts or an 
expression. These conversions require a rewrite of the data on disk.

# Motivation
Changing a column's data type is a common operation. 
It's a highly requested feature especially for ORMs.

# High-level Summary
The idea for a column conversion requiring a cast is to create a new computed 
column by applying the cast/function to the original column. 
For each index that contains that column, create a new index *i* that uses all 
the columns in *i* but replaces the old column with the new computed column. 
Swap the old indexes with the new indexes, 
swap the old column with the new column. Then drop the old columns and indexes.

# Detailed Explanation
# Column backfill method
## A. Converting a column that is not part of an index (simple case)
If we're trying to convert column **c** to type **t**
1. Create a new non-public computed column c' using c::t'
2. Enqueue a column swap mutation, for c and c' 
which performs the following set of actions:
    1. Make c a computed column of c'
        - This is necessary since if one node writes to to the table with the 
        updated TableDescriptor (with c' and c swapped) while one node reads from 
        the version with the old TableDescriptor, the read should see the write.
    2. Make c a non-public column, make c' a public column
        - This is so both columns aren't visible to the user at the same time. 
        During the state where the old column has not been dropped.
    3. Swap the names of c and c'
    4. Swap positions of c and c' in TableDescriptor.Columns
        - This is where physical order of columns is determined
    5. Update the new column's LogicalColumnID to the old column's ID.
        - This maintains the ordinal position of the column in virtual tables.    
    6. Enqueue drop for c'
   
## B. If the column is part of an index (not part of primary key)
- This includes the column swap process defined in A, the steps for the column 
swap are explained in less detail here.
1. Create a new non-public computed column c' using c::t'
2. For each index c' is part of, create a new index i' as a duplicate of i but 
replace each reference to c with c'.
3. Enqueue index swap mutations.
    - Includes enqueuing index drop mutations for the old indexes.
4. Enqueue column swap mutation. 
5. Enqueue drops for c' and old indexes.

## C. If the column is part of the primary key
- This includes the column swap and index swap processes defined in A and B, 
the steps are explained in less detail here.
1. Create a new non-public computed column c' using c::t'
2. Make a copy of the primary key pk and replace each reference to c with c'
    - pk' must be encoded as a primary index, cannot switch secondary index to 
    primary index
3. Each index i that references pk, create an index i' that references pk'
4. Enqueue primary index swap with pk and pk', and index swaps with i and i'. 
    - Includes enqueuing primary key drop mutation for the old primary key.
5. Enqueue column swap mutation. 
6. Enqueue drops for c' and old indexes.
 
## Check Constraints
During the addition of the new column, we may also have to rewrite constraints. 
Furthermore, while the new column exists and the old column has not been dropped
yet, we have to validate constraints on both the new and the old column.
1. On creation of new column, create constraints for new column by applying 
casts wherever necessary to the old constraint. 
Each comparison to the column has to be casted to the new type. 
Comparisons that have the column inside a nested expression must also be converted.
    - For example, if (x INT) has check (x + 5 > 10), 
    after converting x's type to float, 
    the constraint would be (x + 5::float > 10::float).
    - [Postgres source code for re-adding constraints](https://github.com/postgres/postgres/blob/master/src/backend/commands/tablecmds.c#L11852)
2. Check if constraint is valid (casts and comparisons are valid), 
if the constraint is invalid, fail the schema change and rollback.
3. After the column swap, when the old column is dropped, 
drop the constraints of the old column.
4. Rename the constraints after the swap since we can't have duplicate 
constraint names if we want to keep the constraint name the same.

## Note about USING EXPRESSION
When an expression is provided to alter the column type, 
Postgres does not apply the expression to the column's default expression, 
constraints and indexes.

    
## Failure during schema change
When converting column c (type t) to c' (type t'). 
During the creation of c' as a computed column, 
if a value x in c cannot be converted to type t' using the conversion function, 
the column creation fails and we stop the schema change. 
This state is easy to roll back from as we simply drop the new column c'. 
This case happens before the swap mutation.

For example, if we try converting a column from string -> int, 
and the column has value 'hello', 
this results in a runtime error in creating the new column. 
The schema change will fail and be rolled back.
This is similar to any other case of a schema change running into a runtime error.
    
# Index backfill method
- **This method is contingent on changing the index backfiller to support 
column mutations which is currently blocked due an issue with supporting 
interleaved tables**
    - Read more in the [Github Issue](https://github.com/cockroachdb/cockroach/issues/47989)
- The index backfill method is logically the same as the column backfill
method in how the column is added.
- The only difference is that, we would always recreate a new primary key
and replace all references of the old column with the new one in the primary 
key, then use the index backfiller to backfill the new column along with the
new primary key.
- Advantages of using the index backfiller to backfill columns:
    - The index backfiller is faster since it batches the KVs and writes SSTs 
    directly.
    - The index backfiller is easier to rollback, can revert state by switching 
    back to the old index.
    
### Rollback using the index swap method
- To rollback the schema change on failure, if we hit a runtime error while 
backfilling the old column, we can simply drop the new primary index.

### Column backfill vs index backfill method
- We must decide how to proceed with implementation as the schema changer 
currently doesn't support the index backfill method.

#### Options:
1. First implement using column backfiller, re-implement once column mutations 
are supported index backfiller.
    - Unclear how much code would have to be reimplemented.
    - To avoid being blocked on the index backfiller, 
    this is the approach as of May 4th, 2020.
    - The first implementation will use the current column backfiller 
    and thus require two column backfills.
2. Wait until column mutations are supported by the index backfiller.
3. Continue with the column backfill method.


# Problems
## Out of order ColumnIDs/OrdinalPositions/attnum
Fixed in [#46992](https://github.com/cockroachdb/cockroach/pull/46992)

Due to ColumnIDs being a part of the value encoding for columns, 
we cannot swap the ColumnIDs of the columns. 
For the proposed method of performing a swap by adding a new column and 
dropping the old one, the ColumnIDs will be "out of order". 
This results in unexpected behaviour for code that currently depend on 
ColumnIDs for ordering. 
For example in information_schema.columns, ordinal_position is ColumnID.
This results in SHOW COLUMNS being out of order as it currently depends on 
ordinal_position which is currently ColumnID.

One solution is adding a "ordinal position" slice field to the TableDescriptor 
to handle the "logical id" of a column for virtual tables. 
As it currently stands, 
this ordinal position value can correspond to the Column ID, 
it would only change when doing this column swap, so when a swap is performed, 
the ordinal positions of the columns change. 
We can store ordinal position in the ColumnDescriptor or TableDescriptor. 
If we use a slice for OrdinalPositions in table descriptor, 
each index can correspond to the Columns field in TableDescriptor, 
thus when doing a swap, between two columns, 
we wouldn't have to change the OrdinalPositions.

[This page](https://wiki.postgresql.org/wiki/Alter_column_position) 
mentions how ALTER COLUMN POSITION could be implemented in Postgres. 
The popular method suggests using a separate logical and physical identifier
for the columns. 
The ordinal number of the columns would correspond to the logical identifier.

## Mixed Table Version Clusters
During this state, both the pre-swap and post-swap versions of the 
TableDescriptor are valid, meaning writes to the new column have to be 
reflected in the old column.
Until all nodes have the version of the TableDescriptor where the swap has 
happened, we have to do inserts on both the old and new columns. 
To solve this problem, we make the old column c a computed column of the new 
column c'.

This brings up another issue where c is a computed column of c', 
but a value inserted into c' may not necessarily be able to be casted back to 
the data type of c.

Example: When a column is converted from int -> string, 
but the value inserted cannot be converted back to int.
- If we insert 'hello' into the converted column from int -> string while 
the old int column has not been dropped yet, 
it will give a parse error since it will try to parse 'hello' as an int.

### USING EXPRESSION inverse problem
The two TableDescriptor version problem is even trickier the user does 
ALTER COLUMN ... TYPE USING EXPRESSION.

If the user used an expression to alter the type, 
it may be impossible to convert back to the old type (need an inverse function).
If one node performs a read from the table with the old TableDescriptor after 
one node does an insert into the table with a new version of the 
TableDescriptor, what value can we give for the read?

For example, if we're converting a column from int to bool and using the 
expression f(x) = true if x > 0 and false otherwise, 
how do we get an inverse for this function?

### Possible Solutions
Note that the following solutions can be used together. 
If an inverse expression is provided, then we do not have to take the column 
down during this state, otherwise we can fall back to option 2 and disable 
writes to the column.

- Providing an inverse expression
    - We can allow the user to provide a second expression which converts 
    the new type back to the old type.
    - During this state, the old column will be in write-only 
    but with a computed expression that is a function of the new type.
    - This would also allow the user to provide a placeholder value.
    - **Pros**
        - User can define what behaviour they want so they can have more control
         during this state.
        - More code complexity, need to apply inverse expression and validate 
        inserts into the old column.
    - **Cons**
        - Differs from PG behaviour / syntax. Not necessarily bad since we can 
        make providing an inverse optional.
        - Adds more complexity onto the user.
- Disallowing writes
    - We could disallow writes that aren't validated by both columns until the 
    schema change is finalized.
    - **Pros**
        - Simple to implement. 
        - Less unexpected behaviour.
    - **Cons**
        - Takes the column offline during this state.

# Other considerations
Have to update back references for foreign keys and interleaved tables if an 
index is changed.
How we deal with this likely depends on [#47989](https://github.com/cockroachdb/cockroach/issues/47989).

# Alternatives
One alternative is to create a whole new table, 
this simplifies the cases as we redefine the new column, indexes and primary key.

- It's not clear we currently have the schema change primitives to perform an 
operation like this.
