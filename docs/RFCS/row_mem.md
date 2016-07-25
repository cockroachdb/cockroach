- Feature Name: Compact rows in memory
- Status: draft
- Start Date: 2016-07-23
- Authors: Radu & knz
- RFC PR: 
- Cockroach Issue: (none known yet?)

# Summary

Row data is currently inefficiently stored in memory. We can do
better. There are multiple possible orthogonal optimizations:

1. avoid replicating the interface pointer on every row
2. fixed-width columns next to each other in memory
3. pass-through from rocksdb to pgwire
4. lazy decode 
5. work on batches of rows, not single rows at a time
6. use column storage in-memory

# Motivation

The current data structure for row data in memory is `DTuple`,
i.e. `[]Datum`.  `Datum` is an interface. In Go, declaring an object
(or an array item) to be of interface type forces its representation
in memory to always be 2 words in size, one for a pointer to the
actual data, and one to the interface struct (a sort of vtable
pointer).

So suppose we have a table with 3 `int` columns, with the current
encoding we have *for every row*:

- 3 `int` values in memory
- + 3 pointers to `int` values 
- + 3 pointers to interface vtables

That's a 3x mandatory data amplification. 

Since we can observe that all rows in the table have the same type,
this warrants looking at a possible optimization where we share stuff
across rows.

# Idea 1: avoid replicating the interface pointer

So the idea here is that we replace `DTuple` as the type to represent a row
by something else; for example an array of unsafe pointers to the actual values;
then re-create the `Datum` object whenever it is needed (i.e accessors).

For example:

```go
type colInterfaces []func(*raw_pointer) Datum
type rowValues []*raw_pointers

func GetColumn(common colInterfaces, rowData rowValues, col int) Datum {
  return common[col](rowData[col])
}
/* ... */
func ptrToDInt(p *raw_pointer) Datum {
  return unsafe_cast(... p ...)
}
```

Saving: 30% reduction in memory usage, possibly also from fewer allocations.

# Idea 2: fixed with columns next to each other

(Best combined with idea 1): have a n array of bytes to hold
row data for rows with fixed-width columns. 
Then the `Datum` accessor for a column creates the pointer to the
data together with the interface on-the-fly.

Saving: 30-60% reduction in memory usage.

# Idea 3: pass-through from rocksdb to pgwire

Keep the value or key encoding as fetched from RocksDB in memory until
it's needed as Datum. For every column that is not involved in SQL
expressions this is beneficial, as we then can decode these columns
directly from db format to pgwire, without going to Datum encoding at all.

This needs to extend the query analyzer to annotate which columns are
passed through from table readers to the pgwire client without being
involved in computations.
(comparisons could be computed directly from the key encoding without going to Datum)

Saving: encoding overhead, xx% memory depending how often the db
encoding is smaller than the datum encoding (it often is, we use
compression for many types).

# Idea 4: lazy decode

Most useful in combination with distsql.  With distsql the data is
encoded when it goes through the wire between nodes.  If some data
goes through a node without being involved in computations locally
(even if it may be used in a computation on a different node) we could
avoid decoding it entirely.

To do this we could mark each column on each node with a bit that
states whether the data is decoded in memory or not. Accessor would
decode on the fly; and the re-encode stage to pass the data to another
node could bypass reencoding if the original encoding is still there
and unmodified.

# Idea 5: batches of rows

Instead of re-allocating a row array and row bytes on each iteration
of planNode or distsql processors, instead have Next() work on batches
of rows at a time that are allocated together.

Best used in combination with the other ideas.

# Idea 6: column storage in memory

All values of a given column have the same Go type. So we could have
a Go array for that column across multiple rows as backing store, and extract
Datum pointers from this using safe casts.

Best used in combination with row batches.

# Drawbacks

More complexity

# Unresolved questions

???
