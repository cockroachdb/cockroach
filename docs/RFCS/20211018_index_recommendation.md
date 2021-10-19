- Feature Name: Index Recommendation Engine
- Status: draft
- Start Date: 2018-10-18
- Authors: Neha George
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: 

# Summary

This document describes an "index recommendation engine" that would suggest table indexes for CockroachDB users to add. As of now, users do not have insight regarding the contribution of indexes to their workload's performance.
This will be done by strategically selecting index subsets that *could* improve performance, and then using the optimizer costing algorithm to determine the best overall subset.
The potential impact of this project is boundless, as every user could see a boost in their workload's performance. 

# Motivation

The main motivation behind this project is to capitalize on CockroachDB's performance potential.
Adding certain indexes can have drastic impacts on the performance of a query, and if this said query is executed repeatedly, the performance discrepancy is all the more important.
Index recommendation is universally applicable, meaning that this project could be used by all customers. The expected outcome is improved query performance on average.

The PMs in this area are Kevin Ngo and Vy Ton. There are existing user stories, linked [here](https://cockroachlabs.atlassian.net/wiki/spaces/SQLOBS/pages/2285207635/Index+recommendations+draft).
The tentative plan is to start with manual single-statement index "add" recommendations, then extend to workload "add" recommendations.
After this functionality for regular indexes is complete, we will have index recommendations with STORING columns, to potentially avoid lookup joins. From here, automatic add/drop recommendations can be considered.

# Technical design
## General Overview

In the background, we collect SQL statements that are executed by the user. This is stored in an existing table, `crdb_internal.statement_statistics`.
Information regarding execution count and latencies, which will be used to assess a statement's tuning priority, can be obtained from this table.
There are three proposed interfaces.

1. There is a new built-in function for index recommendations that takes in no parameters. It generates index recommendations for the current database.
   Using the collected SQL statements, we then run the “index recommendation” algorithm that takes the SQL statements as input and outputs index recommendations that the user can be prompted to accept or reject.
   If the user accepts, the necessary SQL statements to follow that recommendation will be run.
   Otherwise, no changes will be made. Alternatively, we can populate a new `crdb_internal` table with the recommendations and a timestamp, which can then later be queried.
   This choice is more likely so that users can go back and look at their index recommendations.
2. We generate and surface index recommendations in the DB and CC console. There would be a UI showing the recommendations to create and drop in a table view, with an associated impact score or metric.
   How this metric is determined is uncertain, but it would be based on the frequency of that index's use in the workload and its cost-impact on the statements which use it.
3. We automatically run the index recommendation algorithm periodically in the background and tune the database without user input.
   This is an end goal, after we have refined our index recommendations and are confident in them. Similar to statistics creation, this would become a job that runs in the background.
   The frequency of this would be configurable, and could also depend on the activity levels of the database (i.e. only run when there is low activity). 

For a given user database no matter what the interface, a sample workload *W* must be determined, from which index recommendations can be decided. 
*W* contains the top *x* DML statements ordered by execution count times the statement latency, where *x* must be high enough to include many DML statements.
If this is not possible, we return an error to the user stating that there isn't enough information to recommend indexes.
This is operating under the claim that indexes can only benefit and adversely impact DML-type statements.
Next we determine each statement's corresponding index candidate set, if the statement has one (otherwise it will just be the empty set).
A statement will have a candidate set if and only if it benefits from the addition of indexes. 
These candidate sets are potential sets of indexes that will be recommended to the user.
From here, the optimizer costing algorithm is used to determine which amalgamated index set should be recommended to the user.

Additionally, we can have a separate single-statement index recommendation feature. This will be implemented first as the logic it uses can be expanded to support workload-level recommendations.
A suggested interface for this is having special SQL syntax indicating that index recommendations are desired and the statement for which they are wanted.
We could have this as an option for the `EXPLAIN` syntax, or just add index recommendations to the output of the regular `EXPLAIN` of a statement. The latter is what will be implemented to avoid adding a new option that could confuse users.
Similar to the built-in function, the user can view the recommendations. We should also similarly populate a table with the recommendations, so they are persisted beyond the session.

For a single statement, the flow is planned as follows:
- Run the optbuilder's build function and walk through the statement to determine potential candidates, ensuring they do not already exist as indexes and that there are no duplicates.
- Run the optimizer's costing function on the statement. Add sort index candidates if they are not redundant (because sort is only added to the memo here).
- Add hypothetical indexes for each of the potential candidates for which an index does not already exist.
  Run optimize again and determine which potential candidates (if any) are actually being used in the optimal plan to find the final candidate set.
- Add this flow to `EXPLAIN`'s output, showing the final recommended candidate set.

These ideas will be extended for workload recommendations, with the fundamental candidate set and hypothetical index ideas being reused.

## Determining the Candidate Set for a Single Statement
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
Take every index that was used in the optimal plan and put it in this statement's candidate set.

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

From here, we would construct the candidate set of the query, which could result in indexes on either table, no table, or both tables.
This is because we only choose indexes that are used in the best query plan, and so some or all of these indexes may not be considered further.
To reiterate, the candidate set depends on the plan chosen by the optimizer.

## Using the Optimizer Costing Algorithm and Workload Recommendations

The next step is applying the optimizer costing algorithm to determine the best set of indexes for the given workload *W*. That is, find a set of indexes *X* such that *Cost(W, X)* is minimized.
For each statement's candidate set, determine the optimizer cost of *W* if that index subset were to be applied. Choose the statement candidate sets with the lowest *Cost(W, X)*.
We must then check for index overlap and remove similar/identical indexes to avoid redundancy. An example of two similar indexes is having an index on `(x, y)` and then also having an index on `(x, y, z)`.
A strategy to determine which index to remove would be running *W* again with our chosen indexes, and of the redundant indexes choose the one that has the highest worth.
Meaning, the sum of the frequencies of the statements in which the index is used is the highest. When we re-run *W*, we should also remove any chosen indexes that are unused.
At this time, potential indexes can be compared with existing indexes. If indexes we want to recommend include existing indexes, we omit those recommendations.
In the case that no indexes remain, it means that no indexes will be recommended to the user to add.
In a similar way, if the addition of our hypothetical indexes caused some existing indexes to become unused or rarely used, we would recommend that these indexes be deleted.
If the index is still occasionally used, we need to ensure that removing it doesn't negatively affect overall performance. A good way to check this could be contrasting the writes to this index with the reads.
As in, if an index is read from more than it is written to, it's better than the opposite. To fully ensure that this has not caused a regression however, we should re-run *W* with the index hypothetically dropped.
Additionally, before we delete an index, we should ensure that no queries are regularly using hints to force that index.
After this step, we have our optimal *X* that will be recommended to the user.
One issue with this approach is that it can become a feedback loop where adding new indexes affects the existing ones, so we remove them, and then that allows for new potential indexes to be useful.
This is a tradeoff that must be accepted - otherwise the algorithm could run infinitely. Implementation will begin with recommending potential indexes to add, followed by recommending indexes to remove.
Another heuristic that could be added is random swapping of a small subset of indexes being tried with other indexes that were found in *W's* candidate sets. If the total cost of *W* is lower with this random swapping, we keep this configuration and continue as described above.
The number of times this would be tried would be limited, to avoid having an inefficient algorithm. As an aside, we can also independently use index usage metrics to determine if there are any unused indexes that we should recommend be deleted.

We need to also factor in a cost for database writes in the costing algorithm, which is not done by the optimizer. For database reads, indexes can only have positive impact, whereas for writes, they can have a negative impact.
Also, creating an index has a storage cost. Deciding a *fair* cost to associate with creating an index and maintaining an index for database writes is a pending task.

Furthermore, creating and removing indexes has significant overhead, so we will use hypothetical indexes instead. There is an existing [prototype PR](https://github.com/cockroachdb/cockroach/pull/66111/) for this.
We will also need to create a fake catalog that we can tamper with, without interfering with costing on concurrent queries. A current implementation idea is to create a struct that wraps `optCatalog` with additional information pertaining to each table's hypothetical indexes.
Our hypothetical indexes will be added and removed in this catalog, that is otherwise identical to the database's regular catalog.

Running the costing algorithm so many times is another hurdle in terms of computational cost.
We run the algorithm for each statement in *W*, for each statement's candidate set, which is O(cardinality of *W*, squared) time.
Since the queries we are concerned with are a subset of the statements in *W*, the time complexity is not a tight upper bound. However, it shows that this algorithm has roughly quadratic time complexity, which is quite slow. 
A way of mitigating this is by only allowing the recommendation algorithm to be run if database utilisation is below a certain threshold, similar to what Azure does [here](https://cockroachlabs.atlassian.net/wiki/spaces/SQLOBS/pages/2252767632/Index+recommendations#:~:text=it%E2%80%99s%20postponed%20if%2080%25%20resource%20utilizated.).
Another way of mitigating this is by ensuring the sample workload is a meaningful *sample* which is not too large. We do this by limiting the size of the sample workload when we fetch it. 
If performance continues to be an issue, which is highly applicable for auto-tuning, this functionality can be disabled. Alternatively, a setting can be configured to tune the database less frequently.

In terms of the final output, we will definitely have recommended indexes. In addition, we should have a quantifiable "impact score" associate with an index recommendation that we can use to justify why the index would be beneficial to users.
We can also include further information with this, such as what queries are affected and/or affected the most. For "drop" recommendations, we should have a similar metric. If an index is being frequently written to and infrequently accessed, the score would be higher than if only the latter was true.

## Drawbacks

An important potential drawback of this project would be recommending indexes that slow down overall performance. In theory, with proper design, this should not be a major issue. 
Albeit, there are cases in which this would happen. Since maintaining an index has an associated cost, it's not always beneficial to add more indexes. Thus, a certain middle ground must be achieved. 
It is possible that this middle ground is most optimal for the SQL statements considered when choosing recommendations, but in practice the workload's demands could fluctuate.
Determining useful index recommendations in such a situation is a difficult task.

Still, in most cases, one can expect the workload's general characteristics to be consistent. Also, index recommendations would only be made if there is enough sample data to do so. Meaning, index recommendations would always be based on patterns observed in a significant sample size.

If the database is "auto-tuned," with index recommendations being periodically applied in the background, it could become computationally expensive, as discussed above.
However, this is a tradeoff that needs to be made in order to make the best use of indexing. Plus, should we implement auto-tuning, we would allow the user to turn the tuning off or make it less frequent, if they desire.

## Rationale and Alternatives

To determine the candidate set, a simpler heuristic could easily be used.
For example, trying single column indexes for any attributes that appear in the queries.
The reason this more involved method is chosen is that it considers more complex indexes that could potentially further improve performance. 

The other portion of the algorithm is the optimizer costing. A viable alternative to this, seen in modern literature, would be using ML-based modelling to choose indexes from a candidate set.
However, this seemed like overkill for our purposes. Although an impressive feat in academia, a simpler algorithm using our existing optimizer infrastructure can achieve largely the same goal.
Thus, it made sense to use our optimizer costing algorithm.

The impact of not doing this project at all is significant since established databases offer index recommendation. In not doing so, we are missing an important feature that some consumers expect.

# Unresolved questions

- **Will the engine recommend partial indexes and inverted indexes?**
This algorithm does not consider these types of indexes as determining heuristics to recommend them is more difficult. This could be an extension in the future.
- **How will we cost writes and index creation?**. This can be determined experimentally, 
as development of this feature is underway. One can create test data, SQL write statements and SQL queries in order to determine an index creation and update costing mechanism that makes sense.

## References
[1] http://www.cs.toronto.edu/~alan/papers/icde00.pdf

[2] https://baozhifeng.net/papers/cikm20-IndexRec.pdf
