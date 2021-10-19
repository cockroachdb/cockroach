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

The PMs in this area are Kevin Ngo and Vy Ton. 

**TODO: User stories discussion. Pending meeting with Kevin.**

# Technical design
## General Overview

In the background, we collect SQL statements that are executed by the user. This is stored in an existing table, `crdb_internal.statement_statistics`.
Information regarding execution count and latencies, which will be used to assess a statement's tuning priority, can be obtained from this table.
There are two proposed interfaces. 

1. There is a new built-in function for index recommendations that takes in no parameters. It generates index recommendations for the current database. Using the collected SQL statements, we then run the “index recommendation” algorithm that takes the SQL statements as input and outputs index recommendations that the user can be prompted to accept or reject.
   If the user accepts, the necessary SQL statements to follow that recommendation will be run.
   Otherwise, no changes will be made. Alternatively, we can populate a new `crdb_internal` table with the recommendations and a timestamp, which can then later be queried.
2. We automatically run the index recommendation algorithm periodically in the background and tune the database without user input.
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

Additionally, we can have a separate single-statement index recommendation feature.
A suggested interface for this is having special SQL syntax indicating that index recommendations are desired and the statement for which they are wanted.
This will be an additional option in the existing `EXPLAIN` syntax. Similar to the built-in function, the user can view the recommendations, and then accept or reject them.

## Determining the Candidate Set for a Single Statement
There are a large number of possible indexes for a given statement *(S)* that uses one or more tables, so we choose a candidate set [[1]](http://www.cs.toronto.edu/~alan/papers/icde00.pdf) [[2]](https://baozhifeng.net/papers/cikm20-IndexRec.pdf).
Choose *S's* candidate set as follows:
- Separate attributes that appear in *S* into 5 categories:
  - **J**: Attributes that appear in JOIN conditions
  - **R**: Attributes that appear in RANGE conditions
  - **EQ**: Attributes that appear in EQUAL conditions
  - **O**: Attributes that appear in GROUP BY or ORDER BY clauses
  - **USED**: Attributes that are referenced anywhere in the statement that are not in the above categories.
- Note that in order to access this information, we need to parse the statement string and build the canonical expression so that `tree.UnresolvedName` types are resolved.
- Using these categories, follow a set of rules to create candidate indexes. For succinctness, only some rules are listed.
  - When **O** attributes come from a single table, create an index using all attributes from that ordering/grouping.
  - Create single-attribute indexes from **J**, **R**, and **EQ**.
  - If there are join conditions with multiple attributes from a single table, create a single index on these attributes.
- Inject these indexes as *hypothetical indexes* into the schema and optimize the single statement (more information about hypothetical indexes in the following section).
Take every index that was used in the optimal plan and put it in this statement's candidate set.

Consider this sample SQL query:

```sql
SELECT a FROM s JOIN t ON s.x = t.x
WHERE (s.x = s.y AND t.z BETWEEN 10 AND 20)
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

## Using the Optimizer Costing Algorithm

The next step is applying the optimizer costing algorithm to determine the best set of indexes for the given workload *W*. That is, find a set of indexes *X* such that *Cost(W, X)* is minimized.
For each statement's candidate set, determine the optimizer cost of *W* if that index subset were to be applied. Choose the statement candidate sets with the lowest *Cost(W, X)*.
We must then check for index overlap and remove similar/identical indexes to avoid redundancy. An example of two similar indexes is having an index on `(x, y)` and then also having an index on `(x, y, z)`.
A strategy to determine which index to remove would be running *W* again with our chosen indexes, and of the redundant indexes choose the one that has the highest worth.
Meaning, the sum of the frequency times latency of the statements in which the index is used is the highest. When we re-run *W*, we should also remove any chosen indexes that are unused.
At this time, potential indexes can be compared with existing indexes. If indexes we want to recommend include existing indexes, we omit those recommendations.
In the case that no indexes remain, it means that the user's existing configuration is optimal. After this step, we have our optimal *X* that will be recommended to the user. 
As an aside, we can also use index usage metrics to determine if there are any unused indexes that we should recommend be deleted.

We need to additionally factor in a cost for database writes in the costing algorithm, which is not done by the optimizer. For database reads, indexes can only have positive impact, whereas for writes, they can have a negative impact.
Also, creating an index has a storage cost. Deciding a *fair* cost to associate with creating an index and maintaining an index for database writes is a pending task.

Furthermore, creating and removing indexes has significant overhead, so we will use hypothetical indexes instead. There is an existing [prototype PR](https://github.com/cockroachdb/cockroach/pull/66111/) for this.
We will also need to create a fake catalog that implements `opt.Catalog` that we can tamper with, without interfering with costing on concurrent queries.
Our hypothetical indexes will be added and removed in this catalog, that is otherwise identical to the database's regular catalog.

Running the costing algorithm so many times is another hurdle in terms of computational cost.
We run the algorithm for each statement in *W*, for each statement's candidate set, which is O(cardinality of *W*, squared) time.
Since the queries we are concerned with are a subset of the statements in *W*, the time complexity is not a tight upper bound. However, it shows that this algorithm has roughly quadratic time complexity, which is quite slow. 
A way of mitigating this is by only allowing the recommendation algorithm to be run if database utilisation is below a certain threshold, similar to what Azure does [here](https://cockroachlabs.atlassian.net/wiki/spaces/SQLOBS/pages/2252767632/Index+recommendations#:~:text=it%E2%80%99s%20postponed%20if%2080%25%20resource%20utilizated.).
Another way of mitigating this is by ensuring the sample workload is a meaningful *sample* which is not too large. We do this by limiting the size of the sample workload when we fetch it. 
If performance continues to be an issue, which is highly applicable for auto-tuning, this functionality can be disabled. Alternatively, a setting can be configured to tune the database less frequently. 


**TODO: Discuss user experience, refer to user stories.**

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

- **What will the user interface look like?** 
Ideas have been suggested in this RFC, but this is subject to change as UX is of utmost importance.
It is possible that there is some kind of GUI in the future to be non-developer friendly, or that we do auto-tuning as mentioned.
The focus to start, however, will be building the engine to generate index recommendations from a sample workload. 
- **Will the engine recommend partial indexes and inverted indexes?**
This algorithm does not consider these types of indexes as determining heuristics to recommend them is more difficult. This could be an extension in the future.
- **How will we cost writes and index creation?**. This can be determined experimentally, 
as development of this feature is underway. One can create test data, SQL write statements and SQL queries in order to determine an index creation and update costing mechanism that makes sense.

## References
[1] http://www.cs.toronto.edu/~alan/papers/icde00.pdf

[2] https://baozhifeng.net/papers/cikm20-IndexRec.pdf
