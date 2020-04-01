- Feature Name: ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE
- Status: WIP
- Authors: Richard Cai
- Cockroach Issues: #9851

# Table of Contents
- [Summary](#summary)
- [Motivation](#motivation)
- [High-level summary](#high-level-summary)
- [Detailed Explanation](#detailed-explanation)
- [Problems](#Problems)
- [Other considerations](#Other-considerations)
- [Alternatives](#alternatives)

# Summary
Support changing type of a column. (For nontrivial type conversions using a cast).

# Motivation
Fairly common use case to change a columns data type, especially in ORMs. Highly requested feature.

# High-level Summary
The idea for a column conversion requiring a cast is to create a new computed column by applying the cast/function to the original column. For each index that contains that column, create a new index that uses all the columns in i but replaces the old column being converted with the new computed column. Swap the old index with the new index, swap the old column with the new column. Then drop the old columns and indexes.

# Detailed Explanation
## Converting a column that is not part of an index (simple case)
If were trying to convert column **c** to type **t**
1. Create a new hidden computed column c' using c::t'
2. Enqueue a column swap mutation, for c and c'
    - Make c a computed column of c'
        - Only works if we have an inverse - this makes alter column type using an expression extremely tricky
    - Make c hidden, make c' not hidden
    - Swap the names of c and c'
    - Swap positions of c and c' in TableDescriptor.Columns
        - This is where physical order of columns is determined
    - Enqueue drop for c'
    - Note that this current process leaves the ColumnIDs and thus the ordinal positions of the columns out of order. Explained in out of order column ids section.

## If the column is part of an index (not part of primary key)
1. Create a new hidden computed column c' using c::t'
2. For each index c' is part of, create a new index i' as a duplicate of i but replace each reference to c with c'.
3. Enqueue index swap mutations, enqueue column swap mutation. 
    - This has to be atomic, swapping and dropping the old column as well as swapping and dropping the old indexes.

## If the column is part of the primary key
1. Create a new hidden computed column c' using c::t'
2. Make a copy of the primary key pk and replace each reference to c with c'
    - pk' must be encoded as a primary index, cannot switch secondary index to primary index
3. Each index i that references pk, create an index i' that references pk'
5. Enqueue primary index swap with pk and pk', and index swaps with i and i'



# Problems
## Out of order ColumnIDs/OrdinalPositions/attnum
When performing a column swap, ColumnIDs cannot be swapped due to encodings. For the current listed method of performing a swap by adding a new column and dropping the old one, the ColumnIDs will be "out of order". This results in places that currently depend on ColumnIDs to also be out of order. For example in information_schema.columns, ordinal_position is ColumnID. (Similarly attnum and adnum in pg_catalog also correpsond to ColumnID).
This results in SHOW COLUMNS being out of order as it currently depends on column id. 

One solution is adding a "ordinal position" slice field to the TableDescriptor to handle the "logical id" of a column for virtual tables. As it currently stands, this ordinal position value can correspond to the Column ID, it would only change when doing this column swap, so when a swap is performed, the ordinal positions of the columns change. We can store ordinal position in the ColumnDescriptor or TableDescriptor. If we use a slice for OrdinalPositions in table descriptor, each index can correspond to the Columns field in TableDescriptor, thus when doing a swap, between two columns, we wouldn't have to change the OrdinalPositions.

Interesting page on ALTER COLUMN POSITION idea for Postgres.
https://wiki.postgresql.org/wiki/Alter_column_position

This blurb mentions using separate logical and physical identifiers for the column.
```
Adding alter column syntax into postgres
Since the above methods have a number of issues, it has often been expressed that we would like to add capabilities for postgres to allow reordering of columns. The flip side of this is that it would also be desirable for postgres to automatically order columns physically for optimum layout, regardless of the logical order they are given in. The current problem with implementing this lies in that currently postgres uses the same identifiers for both the logical and physical position within a table. The current hot plan for solving this would be to change the system to reference three identifiers... a permanent identifier for the column, as well as a separate logical and physical identifier. This would allow places that need to deal specifically with column order at the logical level (ie. select *) to reference the logical number, while places that interact with disk system can access the physical number, and all other places just use the column's permanent id.

The above plan is taken from the following email http://archives.postgresql.org/pgsql-hackers/2006-12/msg00983.php.
The TODO list references this email, http://archives.postgresql.org/pgsql-hackers/2006-12/msg00782.php, which is a fun read if you want more back story.
The other bit is that we would also need to determine syntax for how users would interact with this new functionality. The most common implementations typically use a set of BEFORE/AFTER keywords, followed by an existing column name.
```
## USING EXPRESSION for conversion
If we’re planning to support USING EXPRESSION for altering a column data type, we run into the issue where if one node can see the old version of a TableDescriptor and if one node has the new version, if insert happens on the new version into the column with the new data type, and a read happens from the node with the old TableDescriptor.

It’s alright if we’re able to cast back and forth between the two types, however if we used an expression to alter the type, it may be impossible to convert back to the old type (need an inverse function.)

# Other considerations
Foreign keys, interleaved tables

# Alternatives
One alternative is to create a whole new table, this simplifies the cases as we redefine the new column, indexes and primary key.
Problem is how to backfill the table.