- Feature Name: Index Recommendation Engine
- Status: in-progress
- Start Date: 2018-10-18
- Authors: Neha George
- RFC PR: [#71784](https://github.com/cockroachdb/cockroach/pull/71784)
- Cockroach Issue:

# Summary

This document describes an "index recommendation engine" that would suggest table indexes for CockroachDB users to add. As of now, users do not have insight regarding the contribution of indexes to their workload's performance.
This will be done by strategically selecting index subsets that *could* improve performance, and then using the optimizer costing algorithm to determine the best overall subset.
The potential impact of this project is boundless, as every user could see a boost in their workload's performance.

# Motivation

The main motivation behind this project is to capitalize on CockroachDB's performance potential.
Adding certain indexes can have drastic impacts on the performance of a query, and if this said query is executed repeatedly, the performance discrepancy is all the more important.
Index recommendation is universally applicable, meaning that this project could be used by all customers. The expected outcome is improved query performance on average.

# Technical design
## User Stories

The PMs in this area are Kevin Ngo and Vy Ton. There are existing user stories, linked [here](https://cockroachlabs.atlassian.net/wiki/spaces/SQLOBS/pages/2285207635/Index+recommendations+draft).
The tentative plan is to start with manual single-statement index recommendations of indexes to add to the database, including index recommendations with STORING columns (to potentially avoid lookup joins).
This functionality will then be extended to workload recommendations of indexes to add. From here, automatic recommendations of indexes to add *and* indexes to drop can be considered.

## General Overview
### Single Statement Recommendations

To begin, we will have a single-statement index recommendation feature. This will be implemented first as the logic it uses can be expanded to support workload-level recommendations, discussed next in the RFC.
The feature will output recommendations of indexes to add that will optimize a given query, which lends well to having it included with the `EXPLAIN` syntax, below the query plan.
There will be index recommendations in a table format, including the index columns, their direction, and SQL command to run to add each index.

For a single statement, the flow is planned as follows:
- Run the optbuilder's build function and walk through the statement to determine potential candidates, ensuring they do not already exist as indexes and that there are no duplicates.
- Add hypothetical indexes for each of the potential candidates for which an index does not already exist.
- Run the optimizer with these hypothetical indexes included and determine which potential candidates (if any) are actually being used in the optimal plan to find the "recommendation set."
- Connect this flow to the `EXPLAIN` output, showing the final recommendation set.

These ideas will be extended for workload recommendations, with the fundamental recommendation set and hypothetical index concepts being reused.

### Workload Recommendations
In the background, we collect SQL statements that are executed by the user. This is stored in an existing table, `crdb_internal.statement_statistics`.
Information regarding execution count and latencies, which will be used to assess a statement's tuning priority, can be obtained from this table.
There are three proposed interfaces, which will be implemented in the given order.

1. There is a new built-in function for index recommendations that takes in no parameters, called `crdb_internal.generate_index_recommendations()`. It generates index recommendations for the current database.
   Using the collected SQL statements, we then run the index recommendation algorithm that takes the SQL statements as input and outputs index recommendations.
   The index recommendations will populate a new `crdb_internal.index_recommendations` virtual table, stored in memory, with the following columns: `create_date`, `table`, `columns`, and `stored_columns`, which can then later be queried.
   The data types of the columns would be as follows:
   - `create_date`: TIMESTAMPTZ
   - `table`: INT_8 (table ID)
   - `columns`: an array of JSONB with each entry storing a column's column ID (INT_8) and direction (boolean representing ascending or descending).
   - `stored_columns`: an array of integers with each column ID (INT_8)
2. We generate and surface index recommendations in the DB and CC console. There would be a UI showing the recommendations of indexes to create and drop in a table view, with an associated impact score or metric.
   How this metric is determined is uncertain, but it would be based on the frequency of that index's use in the workload and its cost-impact on the statements which use it.
3. We automatically run the index recommendation algorithm periodically in the background and tune the database without user input.
   This is an end goal, after we have refined our index recommendations and are confident in them. Similar to statistics creation, this would become a job that runs in the background.
   The frequency of this would be configurable, and would also depend on the activity levels of the database (i.e. only run when there is low activity).

For a given user database no matter what the interface, a sample workload *W* must be determined, from which index recommendations can be decided.
*W* contains the top *x* DML statements ordered by execution count times the statement latency, where *x* must be high enough to ensure that the sum of _statement_latency*execution_count_ is beyond some threshold.
If this is not possible, we return an error to the user stating that there is not enough information to recommend indexes.
This is operating under the claim that indexes can only benefit and adversely impact DML-type statements.
Next we determine each statement's corresponding index recommendation set, if the statement has one (otherwise it will just be the empty set).
These statement recommendation sets are sets of indexes that are tailored to the statement and will potentially be recommended to the user to improve overall workload performance.
A statement will have a recommendation set if and only if it benefits from the addition of indexes.
From here, the optimizer costing algorithm is used to determine which amalgamated index set should be recommended to the user.

## Determining the Recommendation Set for a Single Statement
There are a large number of possible indexes for a given statement *(S)* that uses one or more tables, so we choose a candidate set [[1]](http://www.cs.toronto.edu/~alan/papers/icde00.pdf) [[2]](https://baozhifeng.net/papers/cikm20-IndexRec.pdf).
Choose *S's* candidate set as follows:
- Separate attributes that appear in *S* into 5 categories:
  - **J**: Attributes that appear in JOIN conditions
  - **R**: Attributes that appear in range or comparison conditions
  - **EQ**: Attributes that appear in EQUAL conditions
  - **O**: Attributes that appear in GROUP BY or ORDER BY clauses
  - **USED**: Attributes that are referenced anywhere in the statement that are not in the above categories.
- Note that in order to access this information, we need to parse the statement string and build the canonical expression so that `tree.UnresolvedName` types are resolved.
- Using these categories, follow a set of rules to create candidate indexes. For succinctness, only some rules are listed.
These indexes are all ascending, other than multi-columned indexes created from an ORDER BY, where each column will depend on the direction of the ordering. By default, the first column will be ordered ascending.
If that contradicts its natural ordering, then all columns in the index will do the same, and vice versa. This is to avoid redundant indexes.
  - When **O** attributes come from a single table, create an index using all attributes from that ordering/grouping.
  - Create single-attribute indexes from **J**, **R**, and **EQ**.
  - If there are join conditions with multiple attributes from a single table, create a single index on these attributes.
- Inject these indexes as *hypothetical indexes* into the schema and optimize the single statement (more information about hypothetical indexes in the following section).
Take every index that was used in the optimal plan and put it in this statement's recommendation set.

Consider this sample SQL query:

```sql
SELECT a FROM s JOIN t ON s.x = t.x
WHERE (s.x = s.y AND t.z > 10 AND t.z < 20)
ORDER BY s.y, s.z;
```

From this query and the rules listed, we would have for table *s* indexes:

```
J: (x)
EQ: (x), (y)
O: (y, z)
```

For table *t*:

```
J: (x)
R: (z)
```

From here, we would construct the recommendation set of the query, which could result in indexes on either table, on no table, or on both tables.
The reason not all candidate indexes are included is due to the fact that we only choose indexes used in the optimizer's best query plan.
To reiterate, the recommendation set depends on the plan chosen by the optimizer, while the candidate set does not.

## Using the Optimizer Costing Algorithm with Workload Recommendations

### Overview
The next step is applying the optimizer costing algorithm to determine the best set of indexes for the given workload *W*. That is, find a set of indexes *X* such that *Cost(W, X)* is minimized.
For each statement's recommendation set, determine the optimizer cost of *W* if that index subset were to be applied. Choose the statement recommendation sets with the lowest *Cost(W, X)*.
We must then check for index overlap and remove similar/identical indexes to avoid redundancy. An example of two similar indexes is having an index on `(x, y)` and then also having an index on `(x, y, z)`.
A strategy to determine which index to remove would be running *W* again with our chosen indexes, and of the redundant indexes choose the one that has the highest worth.
Meaning, the sum of the frequencies of the statements in which the index is used is the highest. When we re-run *W*, we should also remove any chosen indexes that are unused.

At this time, potential indexes can be compared with existing indexes. If indexes we want to recommend include existing indexes, we omit those recommendations.
In the case that no indexes remain, it means that no indexes will be recommended to the user to add.
In a similar way, if the addition of our hypothetical indexes caused some existing indexes to become unused or rarely used, we would recommend that these indexes be deleted.
If the index is still occasionally used, we need to ensure that removing it does not negatively affect overall performance. There would be some heuristics we make use of to do this.
To fully ensure that this has not caused a regression however, we should re-run *W* with the index hypothetically dropped.
Additionally, before we delete an index, we should ensure that no queries are regularly using hints to force that index.
After this step, we have our optimal *X* that will be recommended to the user.

In terms of the final output, we will have recommended indexes, if these recommendations exist. In addition, we should have a quantifiable "impact score" associated with an index recommendation that we can use to justify why the index would be beneficial to users.
We can also include further information with this, such as which queries are affected and/or affected the most. For "drop" recommendations, we should have a similar metric.

### Issues and Considerations
One issue with this approach is that it can become a feedback loop where adding new indexes affects the existing query plans, so we remove them, and then that allows for new potential indexes to be useful.
The existence of this feedback loop means that the final recommendation set may not be the most optimal.
This is a tradeoff that must be accepted - otherwise the algorithm could run infinitely. Plus, even if the recommendation set is not the *most* optimal, it will have still been proven to improve the sample workload performance, which is beneficial.
Another heuristic that could be added is random swapping of a small subset of indexes being tried with other indexes that were found in *W's* recommendation sets. If the total cost of *W* is lower with this random swapping, we keep this configuration and continue as described above.
The number of times this would be tried would be limited, to avoid having an inefficient algorithm.
Implementation will begin with recommending potential indexes to add, followed by recommending indexes to remove. As an aside, we can also independently use index usage metrics to determine if there are any unused indexes that we should recommend be deleted.

An additional issue is the lack of histograms on non-indexed columns. This will impact the plan that is chosen by the optimizer. Since statistics collection is a long and involved task, there is no clear way of mitigating this.
Instead, this is a limitation that we must accept for now, especially since this will not stop us from making recommendations (it will just potentially impact their quality).

It might be beneficial to also factor in the cost of index writes and index creation in our recommendation algorithm, which is not done by the optimizer's costing algorithm. For database reads, indexes can only have positive impact, whereas for writes, they can have a negative impact.
Also, creating an index has a storage cost. Deciding a *fair* cost to associate with creating an index and maintaining an index for database writes is a pending task.
This is largely dependent on user preference, as some users might prioritize read performance over write performance, and vice versa.
To handle this, we could have user settings that allow the user to indicate their preference, which will then affect the cost we use internally.
These user settings should be specific to each "application name", to deal with the fact that some applications may be more latency-sensitive than others.

Furthermore, creating and removing indexes has significant overhead, so we will use hypothetical indexes instead. There is an existing [prototype PR](https://github.com/cockroachdb/cockroach/pull/66111/) for this. However, these indexes persist to disk, and for our purposes, we only need the indexes in memory.
We will need to additionally create fake tables that we can tamper with, without interfering with planning on concurrent queries. The implementation idea is to create a struct that wraps `optTable` with additional information pertaining to the table's hypothetical indexes.
Our hypothetical indexes will be added and removed from this table, that is otherwise identical to the regular table.

Moreover, when recommending the removal of indexes we must be cautious with `UNIQUE` indexes. If a unique index already exists, we have to ensure that we do not remove the unique constraint. This can easily be done by keeping unique indexes.
For additional flexibility, however, we could consider adding a `UNIQUE WITHOUT INDEX` constraint, which would allow the unique index's removal.

Running the costing algorithm so many times is another hurdle in terms of computational cost.
We run the algorithm for each statement in *W*, for each statement's recommendation set, which is O(cardinality of *W*, squared) time.
Since the queries we are concerned with are a subset of the statements in *W*, the time complexity is not a tight upper bound. However, it shows that this algorithm has roughly quadratic time complexity, which is quite slow.
A way of mitigating this is by only allowing the recommendation algorithm to be run if database utilisation is below a certain threshold, similar to what Azure does [here](https://cockroachlabs.atlassian.net/wiki/spaces/SQLOBS/pages/2252767632/Index+recommendations#:~:text=it%E2%80%99s%20postponed%20if%2080%25%20resource%20utilizated.).
Another way of mitigating this is by ensuring the sample workload is a meaningful *sample* which is not too large. We do this by limiting the size of the sample workload when we fetch it.
If performance continues to be an issue, which is highly applicable for auto-tuning, this functionality can be disabled. Alternatively, a setting can be configured to tune the database less frequently.
When considering serverless' pricing based on RU consumption, this type of flexibility is vital.

Finally, a general issue with this project would be recommending indexes that slow down overall performance. In theory, with proper design, this should not be a major issue.
Albeit, there are cases in which this would happen. Since maintaining an index has an associated cost, it's not always beneficial to add more indexes. Thus, a certain middle ground must be achieved.
It is possible that this middle ground is most optimal for the SQL statements considered when choosing recommendations, but in practice the workload's demands could fluctuate.
Determining useful index recommendations in such a situation is a difficult task.
Still, in most cases, one can expect the workload's general characteristics to be consistent.
Also, index recommendations would only be made if there is enough sample data to do so. Meaning, index recommendations would always be based on patterns observed in a significant sample size.

## Rationale and Alternatives

For the single-statement recommendations, another suggested interface was adding index recommendations to a separate `EXPLAIN` option, as opposed to adding it to a vanilla `EXPLAIN`.
An advantage of this is it avoids cluttering the `EXPLAIN` output with unexpected information.
However, this would add new syntax that could confuse users. It would also reduce the visibility of the feature, and since users who run `EXPLAIN` often want to see how the plan can be improved, having index recommendations in the same view would be helpful.
Thus, it was decided that this would be included in the vanilla `EXPLAIN`.

To determine the statement recommendation set, a simpler heuristic could easily be used.
For example, the candidate set could be all single column indexes for attributes that appear in the query.
The recommendation set would still be determined by running the optimizer with all indexes in the candidate set added.
The reason this more involved method is chosen is that it considers more complex indexes that could potentially further improve performance.

Another portion of the algorithm is the optimizer costing. A viable alternative to this, seen in modern literature, would be using ML-based modelling to choose indexes from statement recommendation sets.
However, this seemed like overkill for our purposes. Although an impressive feat in academia, a simpler algorithm using our existing optimizer infrastructure can achieve largely the same goal.
Thus, it made sense to use our optimizer costing algorithm.

The impact of not doing this project at all is significant since established databases offer index recommendation. In not doing so, we are missing an important feature that some consumers expect.

# Unresolved questions

- **Will the engine recommend partial indexes, inverted indexes, or hash-sharded indexes?**
This algorithm will not consider these types of indexes to begin with as determining heuristics to recommend them is more difficult (notably partial and hash-sharded indexes). This could be an extension in the future.
A known limitation with not recommending hash-sharded indexes is the potential creation of hotspot ranges, see this [blog post](https://www.cockroachlabs.com/blog/hash-sharded-indexes-unlock-linear-scaling-for-sequential-workloads/).
- **How will we cost writes and index creation?**. This is TBD. General ideas can be determined experimentally, as development of this feature is underway.
One can create test data, SQL write statements and SQL queries in order to determine an index creation and update costing mechanism that makes sense.
- **Could we recommend changes other than adding or removing indexes?** Although this RFC deals with index recommendation specifically, there are other ways to tune the database to optimize performance.
For example, in a multi-region database, we could recommend the conversion of regional tables to global tables (and vice versa). These types of additional recommendations can be explored in the future, in a separate RFC.

## References
[1] http://www.cs.toronto.edu/~alan/papers/icde00.pdf

[2] https://baozhifeng.net/papers/cikm20-IndexRec.pdf
