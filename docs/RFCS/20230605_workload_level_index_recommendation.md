- Feature Name: Workload Level Index Recommendation
- Status: draft
- Start Date: 2023-06-05
- Authors: Aiden He
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue:

# Summary

This document is about the “workload level index recommendation”. As introduced in 
[Index Recommendation Engine](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20211112_index_recommendation.md), 
CockroachDB has already enabled single statement recommendations. But it is limited in the scope of that single 
statement. When we want to build a bunch of indexes to optimize the whole workload, it will not be a good idea to create
or drop indexes according to the index recommendations for all sql statements in that workload. Since there must be many 
overlapped indexes, some of them may be contained by other indexes. The workload level index recommendations will be 
done by collecting the previous indexes recommendations and then using the finding coverage algorithm to extract a small 
subset of indexes. The impact of this project is boosting the workload efficiency without too much space and time cost.

# Motivation

Audience: PMs, end-users, CockroachDB team members.

The motivation is to enhance the index recommendation engine with considering all the other statements in the same
workload. Adding certain indexes can have drastic impacts on the performance of a query, but adding overlapped indexes 
or adding indexes included by other indexes will waste time and space with ineligible benefits. Workload level index
recommendation will only contain those representative indexes with little or nearly empty overlap, meaning that this
project could improve query performance without too much cost.

# Technical design

Audience: CockroachDB team members, expert users.

For the existing index recommendation engine, the general idea is collecting an index set that could improve performance
and then using the optimizer to determine the best one. Similarly, we also split the whole process into two parts, 
finding index candidates and extracting the most valuable representatives. As for the workload level, the existing 
insight system run in the background can only find those costly queries and provide index recommendations for that 
query. Thus, it is incapable of finding shared indexes among many queries. The difference between this project and the 
index recommendation engine are finding external indexes connection and the input and output become an array of sql 
statements instead of one.

As for the input of the workload, user can specify the time range (e.g. last 6 hours, last 2 weeks) or input a series of 
sql statements. We can filter from the candidates source correspondingly.

## Find index candidates

In the background, there is a cache for index recommendations, then the cache will be saved to in-memory tables, next it 
will be flushed to the system tables `system.statement_statistics`. To begin with, we can collect all the index 
recommendations (wiht time filter if necessary) from this table and use them as an index candidate set since they are 
the optimal ones chosen by the optimizer before.

Since partial indexes, inverted indexes, or hash-sharded indexes are not similar to the regular indexes, it is hard to
deal with them together, the algorithm below will only cover regular indexes.

There 3 kinds of index recommendations:
```sql
1. CREATE INDEX ON ... (CREATE)
2. CREATE INDEX ... DROP INDEX ... (REPLACE)
3. ALTER INDEX ... (ALTER)
```
All of them are represented as a sql statement. Finally, we will get an array of sql statements.

## Extracting valuable index representatives
All the examples below is based on the table `t(i INT, j INT, k INT)`.

The `ALTER INDEX` is used if there exists a suitable invisible index. Its underlying content is another 
`CREATE INDEX …`. Thus, we can categorize all the index recommendations into two types: `CREATE INDEX …` and
`DROP INDEX …` (splitting the `REPLACE` type into two parts).

Let us consider all the `CREATE INDEX …` at first. `CREATE`-based index recommendations has two forms:
```sql
CREATE INDEX ON t(i, j)
CREATE INDEX ON t(i) STORING (j)
```
The first one creates indexes on all the columns (i, j) involved. The second one creates index only on column i but
storing column j, meaning there is no order guarantee on column j.

### Indexed Columns

Without considering the storing part, all the indexes can be represented by a list of ordered columns. Since the order
matters a lot, finding their similarities is just like comparing the prefix among strings. Consider the following index
candidates:
```sql
1. CREATE INDEX ON t(i)
2. CREATE INDEX ON t(i, j)
3. CREATE INDEX ON t(j)
```

For thees 3 indexes, we can regard them as strings i, ij, j, then let us build a trie tree for them associated with
table `t`, all the `t` below means the trie tree is built for table `t`, you can regard it as an empty root for indexes:
```
    t
  /   \
j       i
        |
        j
```
As you can find that, the first index `(i)` is completely contained by the second one `(i, j)`, in order to cover all
the indexes, we only need to build indexes `(j)` and `(i, j)`. One interesting thing we can find is that they are
all the leaf nodes for this trie tree. According to the definition of trie, any string represented by the leaf node
covers all the strings represented by any nodes in the path from the leaf node to the root. Therefore, the initial idea 
for finding representative indexes are selecting all the indexes corresponding to the leaf nodes. The algorithm above 
works only when the space and time cost is not that constrained. Further optimization will be covered later.

### Stored Columns
As for the storing part, it becomes tricky, since the order of storing does not matter. Let us consider the following 
index:
```
4. CREATE INDEX ON t(i) storing (j)
```
After inserting the indexed columns to the trie tree and keep the storing part, the tree is like:
```
    t
  /   \
j       i store (j)
        |
        j
```
Then, for the node `i store (j)`, there will be two cases for the storing part depending on whether we can find
a path from a leaf node to the current node in its subtree covering the that storing part (it means some existing chosen 
bofore has already covered the storing part). If we can find, just like the case for the 4th index in the example, then 
we do not need to add an extra index for the storing part since index `(i, j)` will cover the need for index 
`(i) storing (j)`. Otherwise, let us consider the indexes as follows:
```sql
5. CREATE INDEX ON t(j) storing (i)
6. CREATE INDEX ON t(j, k)
```
Insert them to the trie, we can get:
```
            t
      /           \
i store (j)   j store (i)
      |           |
      j           k
```
There is no path covering the storing (i), so we need to add an extra index to store it or store it in an existing
index. Storing it to an existing index is more efficient due to the implementation of key-value layer (will be covered
in Reference). As for existing indexes, not to make the large index larger, the storing part will be added to the 
shallowest leaf node (which is more likely to be the smallest index) in the j's subtree (under t, not the leaf node). In 
our example, there is only one leaf node j, thus i will be stored there. To sum up, for the index 1-6, we will build 
indexes `(i, j)` and `(j, k) storing (i)`.

Now that we have discussed the cases for one storing part per node, when there are multiple storing parts, we can merge 
them in advance and then treat them as one storing part, think about the following indexes:
```sql
7. CREATE INDEX ON t(k)
8. CREATE INDEX ON t(k) storing (i)
9. CREATE INDEX ON t(k) storing (j)
```
The trie tree will become:
```
                  t
      /           |           \
i store (j)  j store (i)  k store (i); store (j)
|                 |
j                 k
```
Thus, it is straightforward to think about merging all the storings. In this example, i and j will be stored with index
`(k)`. In total, we will merge all the storing parts in one node and find the shallowest leaf node to put them if there 
is no covering path is found. Next I will explain why we use this simple idea. First of all, the space cost of putting 
them together storing in one leaf one and splitting them into multiple groups and storing them in multiple leaf nodes is
almost the same. Next, if we accidentally split the storing part of one original index into two, then the corresponding
query cannot be optimized. In addition, we do not want to make the index too large (the size of key and value). Thus, we
decide to merge all the storing parts in one node together and put them in one leaf node, the shallowest one.

To sum up, we build a trie tree on the indexed columns from previous index recommendations amd attach the storing parts
to each tree node. Then, select all the leaf nodes to build representative indexes. For each node with attached storing
parts, they will be merged. Next, if we do not find a path from the one node to the leaf node in its subtree, meaning
that there does not exist an index storing all the columns we need, then we will choose the shallowest leaf nodes to 
mount all the storing columns. 

## Future Work

### Find index candidates

All the index recommendation candidates collected from the table `system.statement_statistics` are out of date since
they are generated before, the data may change after that, meaning the optimal index chosen by the optimizer (those we
collect) may not the optimal one. To get a wider range of index recommendation candidates, we can collect index
candidates from the raw sql statements stored by the `sql.opt.indexrec.FindIndexCandidateSet()`. Then, use this much
larger set of candidates to find representative indexes to optimize the workload. In this way, since the exploring range 
becomes much larger, the potential results should be better, but at the expense of time and space consuming. One idea to 
deal with it is issue two level workload index recommendations. The first one is a lightweight one using the idea 
collecting existing index recommendations as candidates running comparatively frequent, the other one using the idea 
above running less often.

### Extracting valuable index representatives

Whether attaching all the storing columns to the shallowest leaf node is the optimal solution remains unclear. As you 
can see in the example below:
```
              t
              |   
              i store (j, k)     
         /         \
j store (k)         k
```
The optimal solution should be indexes `(i, j) storing (k)` and `(i, k)`. If we randomly attach storing columns `(j, k)`
to the right child node, then the output will be indexes `(i, j) storing (k)` and `(i, k) storing (j)`, which is a 
suboptimal solution. If we attach them to the left child, we need to merge duplicate storing columns from different 
ancestor nodes. Basically, we can regard this problem as push-down for storing columns, we need to choose a subtree of 
one node to push down all the storing columns. If we push them down too shallowly, there may be more opportunities to 
share more storing parts among indexes. Otherwise, we may add it to a very large leaf node index, which may make the 
optimization not that obvious since visiting a large index is costly.

Another problem is whether we need to merging all the storing parts originally from different index candidates in one 
tree. Think about the example as follows:
```
    t
    |
    i store (j); store (k)
  /   \
j       k
```
If we merge them together, the storing part becomes `(j, k)`, then there is no leaf node can cover them. The final 
output of the algorithm will be indexes `(i, j) storing (k)` and `(i, k)`, or `(i, j)` and `(i, k) storing (j)`. 
However, the optimal solution should be `(i, j)` and `(i, k)` since the storing parts from different original indexes 
are covered by them respectively. 

To deal with this problem, we need to process the storing part from multiple original indexes one by one, which could 
increase the workload extremely. I suggest that we can keep a counter for the number of original indexes one storing 
part of a node, if it exceeds some limit, we will merge them.

### Further Optimization

The second point is for further optimization. If we do not have enough time or space budget to support all the leaf node
indexes, what should we do? Along the idea of trie tree, we can use some internal node to build index, here is one 
example:
```
    t
    |
    k
  /   \
i       j 
```
We can build a index `(k)` instead of two `(k, i)` and `(k, j)` but still get a good performance. The idea behind is 
merging leaf nodes. As for the example above of two leaf nodes, we can find the 
[lowest common ancestor (LCA)](https://en.wikipedia.org/wiki/Lowest_common_ancestor) to represent them, since we reduce 
the number of indexes by 1 and keep the longest share between them. If we need to merge more than two indexes, we can 
borrow the idea from [Huffman coding](https://en.wikipedia.org/wiki/Huffman_coding), each time we merge the most 
expansive index with its nearest indexes (may from the leaf node or internal node) and then repeat this process until 
the budget satisfied. Just like the example above, we merge the indexes `(k, i)` and `(k, j)` to `(k)`. If there is no 
sharing between existing indexes, we can get rid of those expansive one.

In order to complete the algorithm above, we need to think of some objective function to estimate the cost for different 
indexes based on the number of indexed columns, the number of storing columns, different types of columns, usage in the 
whole workload, etc. 

## Rationale and Alternatives

...

# Unresolved questions

## References

### KV layer representations for indexes
Generally, key representation for the index is `/tableID/indexID/indexColumns.../primaryKey`, the value will be 
`[/StoreColumns]`. Suppose there are 2 tuples inserted to the table `t`: `{1, 2, 3}, {4, 5, 6}, {7, 8, 9}`. If we 
create the following index:
```sql
CREATE INDEX ON t(j, k)
```
Assume the tableID is 1 and the indexID is also 1. The key-value pairs for this index will be:

| Key        | Value | 
|------------|-------|
| /1/1/2/3/1 | Ø     |
| /1/1/5/6/4 | Ø     |
| /1/1/7/8/9 | Ø     | 
As for an index with storing:
```sql
CREATE INDEX ON t(j) storing (k)
```
Assum the indexID is 2. The key-value pairs are:

| Key      | Value | 
|----------|-------|
| /1/1/2/1 | 3     |
| /1/1/5/4 | 6     |
| /1/1/7/9 | 9     | 

As you can see, the storage cost for the two indexes above are almost the same, but since the SSTable is ordered by the 
key, thus longer key may cost more. 

For the question about adding an extra index to store the storing part or attaching them to an existing index, the 
answer should be the latter one since adding an extra index means adding the tableID, indexID, indexColumns again. What 
is more, adding more keys means higher search latency, thus attaching them to an existing index is a better idea.

For the question about storing the storing part of one node on multiple leaf nodes or one leaf node, the answer should 
be one leaf node, adding one storing column to the index just means adding it to the value, thus the space cost for 
these two methods are almost the same, but storing all of them in one leaf node will decrease the number of vists for kv
stores, thus that will be a efficiency improvement.
