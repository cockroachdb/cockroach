- Feature Name: ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE (requiring a CAST or expression)
- Status: WIP
- Authors: Richard Cai
- Cockroach Issues: [#9851](https://github.com/cockroachdb/cockroach/issues/9851)
- Note that there is an [RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20180411_alter_column_type.md) talking about ALTER TABLE ... ALTER COLUMN SET DATA TYPE more generally

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [High-level summary](#high-level-summary)
- [Detailed Explanation](#detailed-explanation)
- [Problems](#Problems)
- [Other considerations](#Other-considerations)
- [Alternatives](#alternatives)

# Summary
Support changing type of a column. For implicit casts and USING EXPRESSION that require re-encoding the data on disk.

# Motivation
Fairly common use case to change a column's data type, especially in ORMs. Highly requested feature.

# High-level Summary
The idea for a column conversion requiring a cast is to create a new computed column by applying the cast/function to the original column. For each index that contains that column, create a new index that uses all the columns in i but replaces the old column being converted with the new computed column. Swap the old index with the new index, swap the old column with the new column. Then drop the old columns and indexes.

# Detailed Explanation
## A. Converting a column that is not part of an index (simple case)
If we're trying to convert column **c** to type **t**
1. Create a new non-public computed column c' using c::t'
2. Enqueue a column swap mutation, for c and c' where performs the following set of actions:
    1. Make c a computed column of c'
        - This is necessary since if one node writes to to the table with the updated TableDescriptor (with c' and c swapped) while one node reads from the version with the old TableDescriptor, the value should still exist.
    2. Make c a non-public column, make c' a public column
        - This is so both columns aren't visible to the user at the same time. During the state where the old column has not been dropped.
    3. Swap the names of c and c'
    4. Swap positions of c and c' in TableDescriptor.Columns
        - This is where physical order of columns is determined
    5. Enqueue drop for c'
    6. Note that this current process leaves the ColumnIDs and thus the ordinal positions of the columns out of order. Explained in out of order column ids section.

## B. If the column is part of an index (not part of primary key)
- This includes the column swap process defined in A, the steps for the column swap are explained in less detail here.
1. Create a new non-public computed column c' using c::t'
2. For each index c' is part of, create a new index i' as a duplicate of i but replace each reference to c with c'.
3. Enqueue index swap mutations.
    - Includes enqueuing index drop mutations for the old indexes.
4. Enqueue column swap mutation. 
    - Includes enqueuing column drop mutation for the old column.

## C. If the column is part of the primary key
- This includes the column swap and index swap processes defined in A and B, the steps are explained in less detail here.
1. Create a new non-public computed column c' using c::t'
2. Make a copy of the primary key pk and replace each reference to c with c'
    - pk' must be encoded as a primary index, cannot switch secondary index to primary index
3. Each index i that references pk, create an index i' that references pk'
4. Enqueue primary index swap with pk and pk', and index swaps with i and i'. 
    - Includes enqueuing primary key drop mutation for the old primary key.
5. Enqueue column swap mutation. 
    - Includes enqueuing column drop mutation for the old column.

## Failure during schema change
When converting column c (type t) to c' (type t'). During the creation of c' as a computed column, if a value x in c cannot be converted to type t' using the conversion function, the column creation fails and we stop the schema change. This state is easy to roll back from as we simply drop the new column c'. This case happens before the swap mutation.

For example, if we try converting a column from string -> int, and we run into a value 'hello', then the computed column cannot be created and a rollback will happen (dropping the column).
This is similar to any other case of a schema change running into a runtime error.

If the swap mutation has happened - the type of the column is effectively changed, the only thing left to do is drop the old column.


# Problems
## Out of order ColumnIDs/OrdinalPositions/attnum
Due to ColumnIDs being a part of the value encoding for columns, we cannot swap the ColumnIDs of the columns. 
For the proposed method of performing a swap by adding a new column and dropping the old one, the ColumnIDs will be "out of order". 
This results in unexpected behaviour for code that currently depend on ColumnIDs for ordering. 
For example in information_schema.columns, ordinal_position is ColumnID.
This results in SHOW COLUMNS being out of order as it currently depends on ordinal_position which is currently ColumnID.

One solution is adding a "ordinal position" slice field to the TableDescriptor to handle the "logical id" of a column for virtual tables. As it currently stands, this ordinal position value can correspond to the Column ID, it would only change when doing this column swap, so when a swap is performed, the ordinal positions of the columns change. We can store ordinal position in the ColumnDescriptor or TableDescriptor. If we use a slice for OrdinalPositions in table descriptor, each index can correspond to the Columns field in TableDescriptor, thus when doing a swap, between two columns, we wouldn't have to change the OrdinalPositions.

[This page](https://wiki.postgresql.org/wiki/Alter_column_position) mentions how ALTER COLUMN POSITION could be implemented in Postgres. THe popular method suggests using a separate logical and physical identifier for the columns. The ordinal number of the columns would correspond to the logical identifier.

## State when the columns have been swapped but the old column hasn't been dropped.
During this state, we still have two columns and either two versions of the TableDescriptor can exist - one where the columns have been swapped and one where they haven't. 
In this case, we still need to support reads from the old TableDescriptor. Thus we make the old column c a computed column of the new column c'.
We run into here where c is a computed column of c', but a value inserted into c' may not necessarily be able to be casted back to the data type of c.

Example: When a column is converted from int -> string, but the value inserted cannot be converted back to int.
- i.e., if we insert 'hello' into the converted column from int -> string while the old int column has not been dropped yet, it will give a parse error since it will try to parse 'hello' as an int to support insert into the old column.

### Possible solution
We could possibly disallow writes that don't have an inverse when trying to write to the column during this stage. For the example above, we can return an error stating that the column is under a schema change and this insert is not supported.

## USING EXPRESSION for conversion
This problem is similar to the previous one.
If we’re planning to support USING EXPRESSION for altering a column data type, we run into the issue where if one node can see the old version of a TableDescriptor and if one node has the new version. If insert happens on the new version into the column with the new data type, and a read happens from the node with the old TableDescriptor.

It’s alright if we’re able to cast back and forth between the two types, however if we used an expression to alter the type, it may be impossible to convert back to the old type (need an inverse function.)

Similarly to the previous problem, if one node performs a read from the table with the old TableDescriptor after one node does an insert into the table with a new version of the TableDescriptor, what value can we give for the read?

For example, if we're converting a column from int to bool and using the expression f(x) = true if x > 0 and false otherwise, how do we get an inverse for this function?

Inserts need to be validated by both column types until the schema change is complete - the original column must be dropped.

### Possible solution
A solution to this is disallowing inserts to the column while the schema change is happening - however this is effectively an offline schema change (for the column).

# Other considerations
Have to update references for foreign keys, interleaved tables.
Likely out of scope for this project - can disable changing type columns that are part of FK or interleaved for now.

# Alternatives
One alternative is to create a whole new table, this simplifies the cases as we redefine the new column, indexes and primary key.
Problem is how to backfill the table.
- It's not clear we currently have the schema change primitives to perform an operation like this.
