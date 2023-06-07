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
query. Thus, it is incapable of finding shared indexes among many queries. This project is aimed to make full use of the 
shared parts to recommend space efficient and effective indexes. 

The input and output become an array of sql statements instead of one. Besides those sql statements, user can specify 
the time range (e.g. last 6 hours, last 2 weeks) they want to optimize the workload and the budget they can afford (e.g.
15GB, 500MB, 1TB). We can filter from the candidates source correspondingly or find smaller indexes to use. The details 
will be covered in the UI section.

Since partial indexes, inverted indexes, or hash-sharded indexes are not similar to the regular indexes, it is hard to
deal with them together, the algorithm below will only cover regular indexes.

There 3 kinds of index recommendations for regular indexes:
```sql
1. CREATE INDEX ON ... (CREATE)
2. CREATE INDEX ... DROP INDEX ... (REPLACE)
3. ALTER INDEX ... (ALTER)
```
The `ALTER INDEX` is used if there exists a suitable invisible index. Its underlying content is another
`CREATE INDEX …`. Thus, we can categorize all the index recommendations into two types: `CREATE INDEX …` and
`DROP INDEX …` (splitting the `REPLACE` type into two parts).

Let us consider all the `CREATE INDEX …` at first.

## Find index candidates

In the background, there is a cache for index recommendations, then the cache will be saved to in-memory tables, next it 
will be flushed to the system tables `system.statement_statistics`. To begin with, we can collect all the index 
recommendations (with time filter if necessary) from this table and use them as an index candidate set since they are 
the optimal ones chosen by the optimizer before. All of them are represented as a sql statement. Finally, we will get an 
array of sql statements.

As for the cache mentioned before, there is a space limit for all the index recommendations. Thus, all the index 
recommendations stored in that cache is from those sql statements run frequently and recently. You can check the 
detailed requirement in the method 
[`pkg.sql.idxrecommendations.idx_recommendations_cache.ShouldGenerateIndexRecommendation()`](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/idxrecommendations/idx_recommendations_cache.go#L77). 
Meanwhile, it will also clear those index recommendations which was not used for more than one day in method
[`pkg.sql.idxrecommendations.idx_recommendations_cache.clearOldIdxRecommendations()`](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/idxrecommendations/idx_recommendations_cache.go#L239). 
Thus, all the index recommendations we select from the table `system.statement_statistics` are those which are run more 
frequently and are crucial for the performance.

## Extracting valuable index representatives
All the examples below is based on the table `t(i INT, j INT, k INT)`.

`CREATE`-based index recommendations has two forms:
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

### Stored columns
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
a path from a leaf node to the current node in its subtree covering the storing part (it means some existing indexes 
chosen before has already covered the storing part). If we can find, just like the case for the 4th index in the 
example, then we do not need to add an extra index for the storing part since index `(i, j)` will cover the need for 
index `(i) storing (j)`. To check whether the storing part of one internal one is covered by some leaf nodes, we will
exhaustively search all the leaf nodes inside the subtree of the that node, if we find it, we will remove the storing
part of the node (see "Extracting valuable index representatives" under "Future work" for further optimization).
Otherwise, let us consider the indexes as follows:
```sql
5. CREATE INDEX ON t(j) storing (i)
6. CREATE INDEX ON t(j, k)
```
Insert them to the trie, we can get:
```
            t
      /           \
i store (j)   j store (i)
     |             |
     j             k
```
There is no path covering the storing (i) for the right node `j store (i)` in the first level since it has only one leaf 
node `k`, so we need to add an extra index to store it or store it in an existing index. Storing it to an existing index 
is more efficient due to the implementation of key-value layer (will be covered in References). As for existing indexes, 
not to make the large index larger, the storing part will be added to the shallowest leaf node (which is more likely to 
be the smallest index) in the `j store (i)`'s subtree (under t, not the leaf node). In our example, there is only one 
leaf node for node `j store (i)`, thus i will be stored there (the leaf node `k`). To sum up, for the index 1-6, we will 
build indexes `(i, j)` and `(j, k) storing (i)`.

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
     |            |
     j            k
```
Thus, it is straightforward to think about merging all the storings. In this example, i and j will be stored with index
`(k)`. In total, we will merge all the storing parts in one node and find the shallowest leaf node to put them if there 
is no covering path is found. Next I will explain why we use this simple idea. First of all, the space cost of putting 
them together storing in one leaf one and splitting them into multiple groups and storing them in multiple leaf nodes is
almost the same. Next, if we accidentally split the storing part of one original index into two, then the corresponding
query cannot be optimized. In addition, we do not want to make the index too large (the size of key and value). Thus, we
decide to merge all the storing parts in one node together and put them in one leaf node, the shallowest one.

To sum up, we build a trie tree on the indexed columns from previous index recommendations and attach the storing parts
to each tree node. Then, select all the leaf nodes to build representative indexes. For each node with attached storing
parts, they will be merged. Next, if we do not find a path from the one node to the leaf node in its subtree, meaning
that there does not exist an index storing all the columns we need, then we will choose the shallowest leaf nodes to 
mount all the storing columns. 

## Drop index
Let us consider all the `DROP INDEX` for the next step. It only exists in the index recommendation for replace where the 
index to drop shares the same index columns as the index to create, but the storing columns is a subset compared to the 
index to create. If we think about it from the perspective of trie tree, all the dropped indexes are covered by all the 
indexes represented by the leaf nodes. Therefore, we can drop all the `Drop`-based indexes.

## User Interface

### How to use
Initially, several built-in function will be provided command line interface. `workload_index_recs`, 
`workload_index_recs(timestamp)`, `workload_index_recs(budget)`, and 
`workload_index_recs(timestamp, budget)`. User can enter `SELECT workload_index_recs();`, then they will see
the workload level index recommendations. For the parameter, the type of the `timestamp` can be TIMESTAMP/TIMESTAMPZ. 
We will use it as a filter and only consider those indexes after the timestamp from the table 
`system.statement_statistics`. The `budget` is a string like "15GB", "500MB", "1TB" meaning the space the user can 
afford so that we will apply some algorithm to reduce the space cost, you can find details in the cost optimization 
section later.

### Insight delivered to users
Since we have the statistics for the frequency about the queries, so that we can deliver the "impact" for each 
index, like, the number of fingerprints, the number of executions (some fingerprint may be executed many times). In 
addition, we can also deliver the space cost for all the index recommendations.

For the command line interface (CLI), we show all the index recommendations in the descending order of the number of 
executions one can optimize by default. In the future, we can deliver more information, e.g. all the fingerprints one 
index can optimize and the trie tree to represent the relationship among indexes. 

It will be too overwhelming to deliver too much statistics in CLI. But we will still provide built-in functions 
`workload_index_recs_with_insights()` which returns an array of strings (representing the index recommendation) and 
jsons containing all the statistics for each index recommendation. It will be used for DBConsole or debugging purposes.

## Future work

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

Whether attaching all the storing columns to the shallowest leaf node is the optimal solution remains controversial. As 
you can see in the example below:
```
              t
              |   
              i store (j, k)     
         /         \
j store (k)         k
```
The optimal solution should be indexes `(i, j) storing (k)` and `(i, k)`. If we assign storing columns `(j, k)`
to the right leaf node since the left and right leaf nodes are in the same level, then the output will be indexes 
`(i, j) storing (k)` and `(i, k) storing (j)`, which is a suboptimal solution. If we attach them to the left child, that 
will lead to the optimal result. One potential answer for this question is that we can compare the storing parts in the 
internal node with the path from the current node to its leaf nodes. But the statistics we need to keep while traversing 
the tree is too overwhelming.

If we prefer a lightweight method, we can convert this problem to deciding which child node to push down the storing 
columns of one internal node so that we can merge the storing parts with the one in the child node. But there remain 
some problems. If we push them down too shallowly, there may be more opportunities to share more storing parts among 
indexes. Otherwise, we may add it to a very large leaf node index, which may make the optimization not that obvious 
since visiting a large index is costly if we only want few columns.

Another problem is whether we need to merging all the storing parts originally from different index candidates in one 
tree. Think about the example as follows:
```
    t
    |
    i store (j, l); store (k, l)
  /   \
j       k
|       |
l       l
```
If we merge them together, the storing part becomes `(j, k, l)` for the node in the first level, then there is no leaf 
node that can cover them. Then we will assign them to one of the two leaf nodes since they are at the same level. The 
final output of the algorithm will be indexes `(i, j, l) storing (k)` and `(i, k, l)`, or `(i, j, l)` and `(i, k, l) 
storing (j)`. However, the optimal solution should be `(i, j, l)` and `(i, k, l)` if we do not merge the storing parts 
from different original indexes since `store (j, l)` is covered by `(i, j, l)` and `store (k, l)` is covered by 
`(i, k, l)`. Nevertheless, it will cost more space if not merging.

Thus, whether merging the storing parts from different original indexes is a trade-off between space cost and the 
quality of the final index recommendations. I suggest that we can keep a counter for the number of original indexes of 
the storing part from a node. If it exceeds a limit, we will merge them to save space and time for finding covering leaf 
nodes. Otherwise, we can deal with them one by one.

### Cost optimization

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
expensive index with its nearest indexes (may from the leaf node or internal node) and then repeat this process until 
the budget satisfied. Just like the example above, we merge the indexes `(k, i)` and `(k, j)` to `(k)`. If there is no 
sharing between existing indexes, we can get rid of those expensive one. Another choice is to eliminate the storing 
parts.

In order to complete the algorithm above, we need to think of some objective function to estimate the cost for different 
indexes based on the number of indexed columns, the number of storing columns, different types of columns, usage in the 
whole workload, etc. Besides those space cost, the mutation cost should also be considered, the larger the index, the 
more time consuming the mutations.

As mentioned in the finding index candidates, the frequency is an important aspect we should pay attention to, in 
`system.statement_statistics`, there is a column called `statistics` containing the times of on a type of query has 
been executed, we can use this to assign a weight for each node in the tree so that the merging algorithm will cover the 
importance of the indexes (by frequency). If we collect from the raw sql statements mentioned in the future work, we can 
re-compute the frequency from scratch and use such more precise information to assign the weight.

## Drop index

As for the `DROP INDEX`, since we select all the leaf nodes before, so we can directly drop them without hesitation 
(covered by all the leaf nodes). Nevertheless, we only select some internal nodes to build indexes  when we need to 
merge some leaf nodes for cost optimization so that not everything in the tree is covered, meaning that some dropped 
indexes are not covered by the indexes selected to create. Since the space and time budget is confined, we can also take 
dropping indexes into account, the cost function can be used to calculate the space released by dropping one index. 
Following the idea of the trie tree, we can add all the indexes to drop to the tree. When we decide to merge two 
indexes, there may exist some indexes to drop along the paths to their LCA, when we compute the space to save, we can 
minus those space occupied by the indexes to drop since we can't cover them once we merge these two indexes. Thus, our 
merging process will be more comprehensive.

## Rationale and alternatives

### OrderingChoice
[OrderingChoice](https://github.com/cockroachdb/cockroach/blob/1a339b712842a768d48bf658fa00c3f5af7b02bb/pkg/sql/opt/props/ordering_choice.go) 
is a struct implemented in CRDB to define the set of possible row orderings that are provided or required by an 
operator. If we consider about using it for workload index recommendations (as a replacement as index representation 
consisting of indexed columns and storing columns), there are some useful method implmented in the OrderingChoice: 
1. The `Columns` defined in OrderingChoice specify an order for columns involved, similar to the indexed columns in 
index representation.
2. There are some useful methods implemented in OrderingChoice, like 
[Intersection](https://github.com/cockroachdb/cockroach/blob/1a339b712842a768d48bf658fa00c3f5af7b02bb/pkg/sql/opt/props/ordering_choice.go#L375), 
[CommonPrefix](https://github.com/cockroachdb/cockroach/blob/1a339b712842a768d48bf658fa00c3f5af7b02bb/pkg/sql/opt/props/ordering_choice.go#L463), 
[PrefixIntersection](https://github.com/cockroachdb/cockroach/blob/1a339b712842a768d48bf658fa00c3f5af7b02bb/pkg/sql/opt/props/ordering_choice.go#L851), 
[Implies](https://github.com/cockroachdb/cockroach/blob/1a339b712842a768d48bf658fa00c3f5af7b02bb/pkg/sql/opt/props/ordering_choice.go#L289). 
They can be used to find shared part of indexed columns, judge whether two indexed columns are mergeable.

But there are also some limitations: 
1. Even if the each ordering slot in OrderingChoice can represent multiple options, e.g. `(a|b)` means we can order by 
column a or column b in this slot. But it cannot represente multiple options for a list of columns. Like the example 
below: `SELECT * FROM t WHERE a = 1 AND b = 2`. The two index candidates will be `(a, b)` and `(b, a)`. A single 
OrderingChoice cannot represent them. `(a|b)`, `(a|b)` is not a valid answer since it can also represent `(b, b)` and 
`(a, a)`. We cannot specify any dependency between the two slots (we must pick `b` in the second slot if `a` is picked 
by the first slot). Thus, the expressiveness of OrderingChoice is very limited for index representation.

2. The other drawback is the `Optional` which represents a set of optional columns that can be put anywhere or not put 
them in the final order, e.g. `+1 opt(2)` represents `(1, 2)` and `(2, 1)` and `(1)`. As for the indexes, it is ok that 
one column can be put among a range, just like the previous example, both `(a, b)` or `(b, a)` work. However, it is very 
seldom that one column can be put anywhere which means the `Optional` is not that usefull for index representation.

Consider these large limitaions and not that many useful methods, we decide not to use the OrderingChoice for workload 
level index recommendations.

### Partial Order
As introduced by the paper 
[AIM](https://research.facebook.com/micro_site/url/?click_from_context_menu=true&country=US&destination=https%3A%2F%2Fresearch.facebook.com%2Ffile%2F215595724407039%2FAIM_SRT_Update.pdf&event_type=click&last_nav_impression_id=049WEwN9gpSINlkGu&max_percent_page_viewed=37&max_viewport_height_px=758&max_viewport_width_px=1369&orig_request_uri=https%3A%2F%2Fresearch.facebook.com%2Fpublications%2Faim-a-practical-approach-to-automated-index-management-for-sql-databases%2F&region=noam&scrolled=true&session_id=0SECykrURLuPBYYoo&site=mc_research), 
the partial order shows its expressness in index representation. `({a, b})` is an partial order that simultaneously 
represents both `(a, b)` and `(b, a)`, offering a compact form of index representation. However, this brevity comes at a 
cost - a more complex, time-intensive merging process. As discussed in the section III.E of the paper, the merging rule 
is intricate. The whole problem will not be modeled as a trie based merging problem but a knapsack problem (proposed in 
section III.F of AIM). 

Another consideration is that CRDB currently does not support partial order-based index representation. It necessitates 
a final, defined order for all columns participating in the index. There are ideas outlined in the paper AIM, such as 
ordering columns by selectivity, that could serve as potential solutions. Consider the scenarios that we only store the 
fingerprint without constants, deciding on the order becomes increasingly challenging. 

In summary, while partial orders might present a more robust method of index representation, they also demand 
substantial future development and refinement.

# References

## KV layer representations for indexes
Generally, key representation for the index is `/tableID/indexID/indexColumns.../primaryKey`, the value will be 
`[/StoreColumns]`. Suppose there are 3 tuples inserted to the table `t`: `{4, 5, 6}, {7, 8, 9}, {10, 11, 12}` and i is 
the primary key. If we create the following index:
```sql
CREATE INDEX ON t(j, k)
```
Assume the tableID is 1 and the indexID is also 2. The key-value pairs for this index will be:

| Key           | Value | 
|---------------|-------|
| /1/2/5/6/4    | Ø     |
| /1/2/8/9/7    | Ø     |
| /1/2/11/12/10 | Ø     |

As for an index with storing:
```sql
CREATE INDEX ON t(j) storing (k)
```
Assume the indexID is 3. The key-value pairs are:

| Key        | Value | 
|------------|-------|
| /1/3/5/4   | 6     |
| /1/3/8/7   | 9     |
| /1/3/11/10 | 12    |

As you can see, the storage cost for the two indexes above are almost the same, but since the SSTable is ordered by the 
key, thus longer key may cost more. 

For the question about adding an extra index to store the storing part or attaching them to an existing index, the 
answer should be the latter one since adding an extra index means adding the tableID, indexID, indexColumns again. What 
is more, adding more keys means higher search latency, thus attaching them to an existing index is a better idea.

For the question about storing the storing part of one node on multiple leaf nodes or one leaf node, the answer should 
be one leaf node, adding one storing column to the index just means adding it to the value, thus the space cost for 
these two methods are almost the same, but storing all of them in one leaf node will decrease the number of vists for kv
stores, thus that will be a efficiency improvement.
