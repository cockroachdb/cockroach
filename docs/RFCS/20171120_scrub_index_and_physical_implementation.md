- Feature Name: SCRUB index and physical check implementation
- Status: completed
- Start Date: 2017-10-11
- Authors: Joey Pereira
- RFC PR: [#19327](https://github.com/cockroachdb/cockroach/pull/19327)
- Cockroach Issue: [#10425](https://github.com/cockroachdb/cockroach/issues/10425) (Consistency checking), [#19221](https://github.com/cockroachdb/cockroach/issues/19221) (Using distSQL to do index checks)

# Summary

This RFC presents a method of running a check on a secondary index in an
efficient manner, as well as other physical checks which are closely in
implementation.

This check can be run through the SQL statement `SCRUB`, using the
options to specifically check indexes or check the physical data of the
database.


# Motivation

See the [SCRUB command RFC] for motivation.

In addition to the motivations for the check, this work may also benefit
schema changes. The last unresolved question of the
[online schema change RFC] mentions a method of distributing backfill
work. While this is currently done through distSQL, there are problems
where it's currently unable to backfill an index at a specific
timestamp. This has become prevalent as it may be the source of issues
such as [#18533].

The methods for collecting index data introduced in this RFC are similar
to that which is needed to create an alternative backfilling mechanism.

[#18533]: https://github.com/cockroachdb/cockroach/issues/18533
[SCRUB command RFC]: https://github.com/cockroachdb/cockroach/issues/18675
[online schema change RFC]: https://github.com/cockroachdb/cockroach/blob/94b3f764ead019c529f14c7aab64724991f3ac67/docs/RFCS/20151014_online_schema_change.md


# Guide-level explanation

After implementing the methods proposed by this RFC, the `SCRUB` command
will have the means to check for physical data errors and for index
errors. In particular, the errors we are checking for are:
- `missing_secondary_index`
- `dangling_secondary_index`
- `invalid_encoding`
- `invalid_composite_encoding`
- `noncanonical_encoding`

The first two are index errors. Missing secondary indexes occur when an
index entry is not correctly made or updated with the primary k/v.
Conversely, a dangling index entries occur if the primary key referenced
in the index k/v is not valid. This happens if the primary k/v being
incorrectly deleted while the index k/v remains, in addition to the
reasons for missing index entries.

To detect missing index entries, we need to do a full scan of the
primary k/v. Then for each primary k/v, we expect to find a
corresponding index entry. If no index entry is found, we have a missing
index entry. This does not capture any dangling index entries though, as
we are only looking up index entries which have a valid primary key
reference. In order to find dangling indexes, we need to do a similar
process where we do a full scan of the secondary index k/v.

As a result, a full outer join is a natural solution for this problem.
Taking this approach, we limit the scans done to 2, as opposed to
potentially scanning both the primary k/v and the secondary index k/v
and doing expensive point-lookups to the other set, twice.

Lastly, we want to propagate the errors to be the SQL results of
`SCRUB`, including the information of:
- Database and table involved
- Error type
- Primary key and secondary index value involved
- Potentially the raw key value

The following changes are introduced for in order to do the entirety of
the index check:

- Add logic plan creation (`scrubNode`[0] initialization) to construct a
  logical plan for checking an index, then plan and run it through the
  distSQL execution engine. This plan will accomplish the full scan and
  join in one go.

  The solution instead constructs multiple logical plans
  upon execution of our original plan, and proceeds to run them.

The latter three errors related to encoding happen due to errors in the
row data or our functions manipulating the row data. These can be tested
just by doing a full scan of the data to check (primary k/v and index
entry k/v). Similarly to the above, we will also want the error results
to be propagated up as the SQL results.

The following changes are introduced for running physical checks:

- Transform the `scrubNode`[0] a logical plan that fully encapsulates
  the logic to scan relevant tables. This plan will run through distSQL,
  similar to any regular plan.

- Add a `IsCheck` enum to `scanNode` and plumb it through the
  `TableReader` for `RowFetcher` initialization during distSQL planning.
  This enum would only be meaningfully set during planning of `SCRUB`
  when doing a physical check.

- Modify the `RowFetcher` to conditionally run check code in existing
  row fetching code paths, using the `IsCheck` indicator. For example,
  we want to return an error instead of panic when NULL is encountered
  on a non-NULL column.

- Add a new iterator function to `RowFetcher`, `NextRowWithErrors`, for
  just scanning for errors. In here we can execute additional checking
  code on a separate code path than the critical fetching one. This lets
  us consume check errors instead of row data.

- Add logic to `TableReader` to consume `NextRowWithErrors` and handle the
  difference between wanted rows and returned rows (check error row
  type). This may potentially require a new `TableChecker`. With this
  change, we are able to retrieve the error results desired.

[0]: `scrubNode` is the root planning node when using the `SCRUB`
     command.


# Reference-level explanation

The details of implementation are broken down into a few parts. NB:
the optimizations section is just ideation. The goal of it is to discuss
the feasibility and time-effort of the possibilities.

1. Index checks and how they will be implemented.

2. Physical checks and how they will be implemented.

3. Generating and executing the logical plan in the distSQL execution engine

4. Optimizations

   4.a. Multiple simultaneous index checks.

   4.b. Executing index and physical checks simultaneously.


## 1. Index checks and how they will be implemented.

Checking the index can be expressed through a rather simple logical SQL
plan. The final result will return all erroneous index entries -- both
dangling references and missing entries. In short, this can be broken
down into a few stages of processing:

1) Scan through the primary k/v to fetch relevant index data[1] and
   filter rows where the secondary index value is `NULL`.
2) Scan through the secondary k/v to fetch all data stored in the index.
3) Execute a natural full outer join on both streams to pair primary k/v
   with their secondary index k/v. This filters any entry from both
   streams that don't have a corresponding entry in the other stream.

For example, if we have the following schema:

```sql
CREATE TABLE t1 (
   pkey int primary key,
   name int,
   INDEX name_idx (id ASC),
)
```

Then checking the indexes can be done through the logical plan
represented by:

```sql
SELECT p.pkey, p.name, s.pkey, s.name
FROM
  (SELECT pkey, name FROM t1@{FORCE_INDEX=primary,NO_INDEX_JOIN} ORDER BY pkey, name) AS p
FULL OUTER JOIN
  (SELECT pkey, name FROM t1@{FORCE_INDEX=name_idx,NO_INDEX_JOIN} ORDER BY pkey, name) AS s
ON
   p.pkey = s.pkey OR (p.pkey IS NULL AND s.pkey IS NULL)) AND
  (p.name = s.name OR (p.name IS NULL AND s.name IS NULL))
WHERE
  (p.pkey IS NULL AND p.name IS NULL) OR
  (s.pkey IS NULL AND s.name IS NULL)
```

In short, this query is:
1) Scanning the primary index and the secondary index.
2) Ordering both of them by the primary then secondary index columns.
   This is done to force a distSQL merge join.
3) Joining both sides on all of the columns contained by the secondary
   index key-value pairs.
4) Filtering to achieve an anti-join. It looks for when the primary
   index values are null, as they are non-nullable columns. The first
   line of the predicate takes rows on the right for the anti-join.
   The second line of the predicate takes rows on the left for the
   anti-join.

Because this is an anti-join, the results are as follows:
- If any primary index column on the left is NULL, that means the
  right columns are present. This is because of the invariant that
  primary index columns are never null.
- Otherwise, the left columns is present.

Now, a problem comes up because of the join predicate. The query above
will be run as a hash joiner, with only one bucket. This will be very
slow. As a workaround, the `p.col = s.col OR (p.col IS NULL AND s.col IS NULL)`
predicates are replaced with just `p.col = s.col` so that the query can
be run using a merge joiner, hence the explicit orders and step 2 in the
query. The distSQL plan generated from this query has the
MergeJoinerSpec.NullEquality set to true, to make NULL = NULL evaluate
to true, making the two predicates equivilant while allowing for a far
more efficient execution/

[1]: Relevant index data includes the columns, extra columns, and store
     columns of the index.

## 2. Physical checks and their implementation

The **physical table** is the underlying k/v behind tables. This is
where we find primary and secondary indexes (as distinct keys),
groupings of certain data (column families), how index data is organized
(index storing conditions), and the encoding of the data.

Most of the checks that involve the physical table are encoding and
related data validity. This data is already partially checked during
scanning in `RowFetcher`. Because of this, adding checks is a matter of
a few additive changes, and handling the error to instead return row
results.

The checks we can implement in `RowFetcher` include:
- Tables using [FAMILY][] have the data organized into families
  correctly
- Invalid data encodings
- Non-canonical encodings (i.e. data round-trips)
- [Composite encodings][] are valid
- Key encoding correctly reflects the ordering of the decoded values
- `NOT NULL` constraint violation (not physical table, but checked at
  this layer)

The underlying scanning is in the `RowFetcher`. Inside the current
`RowFetcher` the method `processKv` is responsible for encoding
specific work as it iterates over the k/v, which is where the checks
will be added, including:
- Check composite encodings are correct and round-trip
- Check key encodings correctly reflect ordering of decoded values
- Check the KV pair is well-formed (everything is present, no extra
  data, checksum matches)
- For primary keys, verify that the column family 0 (sentinel) is
  present

We can also throttle the speed of the check by modifying the `limitHint`
provided to `KVFetcher`, and sleeping between batches.

Thus, to run the check we just need to do a scan over the whole table,
and the returned values will be the `CheckError` as found in the
[SCRUB interface RFC]. Ideally, this is executed through the distSQL, as
there will be very few results relative to the data scanned.

Invalid data encoding (and others) already return an `error` while
scanning. We want to capture errors in `RowFetcher` that come from
any external function calls and wrap them in a `scrub.Error` struct with
an error code annotated.

Those error structs will be captured further upstream inside the
TableReader and translated into the row results. In the existing
scanning code path, this same error struct will simply be unwrapped.

For example, consider this excerpt from `RowFetcher.processValueSingle`:

```go
value, err := UnmarshalColumnValue(rf.alloc, typ, kv.Value)
if err != nil {
  return "", "", err
}
// Becomes
value, err := UnmarshalColumnValue(rf.alloc, typ, kv.Value)
if err != nil {
  return "", "", scrub.NewError(
    scrub.InvalidEncodingError,
    err,
  )
}
```

Another chunk of the work will then be plumbing through the check error
results out as the table schema we are scanning will differ from the
result rows representing errors. To get around this we must manually
construct the distSQL physical plan. This will also take some extra code
to wrap the call from TableReader to RowFetcher where it will transform
any caught errors.

[SCRUB interface RFC]: https://github.com/cockroachdb/cockroach/pull/18675

## 3. Generating and executing the logical plan in the distSQL execution engine

During local engine execution, we will generate the necessary check
plans, then execute the plans in sequence on the distSQL execution
engine. These plans will scan the data in a distributed fashion and the
only data leaving the nodes are any error results. This strategy is
similar to schema change index backfilling.

The advantage of this over the alternative method [3], creating the full
logical plan during query planning, is that we can easily run the checks
sequentially. We can block waiting for the distSQL results of each check
before running the next check. This also pulls the work of aggregating
results from the distSQL execution engine to simpler Go code that
accumulates the results.


## 4. Optimizations

Note that this section is largely just the ideas behind optimizations
and are not required to be resolved. The goal of this section is to
gauge feasibility, and whether or not it's worth it to go down those
routes.


### 4.a. Multiple simultaneous index checks

A challenge here is producing both check errors and the necessary data
to run index checks simultaneously. It is not currently known if
multiple different schemas can be streamed out of one table reader.

In the `RowFetcher`, this may look like a `NextRowOrCheckError` iterator
that may return either row data or check error data, and we emit those
to the corresponding outputs.

A workaround may be to have an intermediary processor for each mirrored
stream which filters out only the columns desired for each consumer --
either a specific index check (a joiner), or the final result errors (in
final aggregation).

Alternatively, a crazy idea may modify the RowFetcher or TableReader to
produce two separate schemas, branching off the idea of RowFetchers
made for the adjustments for interleaved tables.


### 4.b. Executing index and physical checks simultaneously

While checking multiple indexes sequentially is discussed above, but a
problem is that a naive solution involves doing `N` scans of the primary
k/vs for checking `N` indexes. The following approaches are both
non-trivial changes, so this idea is largely suggested as an
optimization for future work.

The extra scans can be avoided if we scan all the necessary data for all
indexes being check in one table reader, then use a mirror router to
mirror the data to multiple join consumers. Unfortunately, this would
require making one of the changes:

- Manual surgery of a distSQL physical plan to independently plan each
  index check, then combine them with:

   - Slicing out the primary k/v scan in each, replacing it with a
     single mirrored scan that fetches all the data.

   - Remove each plan's final aggregator, and have them all stream to a
     single new final aggregator.

- Or, adding the mechanisms to be able to do a `UNION` of all the
  results and then elide redundant table readers based on processors'
  column requirements. This is the same final aggregation `UNION`
  mentioned in section 3.a. but we also need to solve the problem of
  scanning the same k/v data multiple times.


# Rationale and Alternatives

## [1] Use a `JOIN READER` aggregator for index checking

Instead of using a `JOINER` aggregator, we could use a `JOIN READER`.
How this would work is that instead of the 3 stages introduced in 1.a,
the process for checking would involve 2 steps:
1) Scan the primary k/v for a table.
2) Use a `JOIN READER` aggregator that does point-lookups against the
   secondary index.

This has two major problems:
- Producing lots of RPCs for the secondary index entries, potentially
  being a performance problem.
- Does not check the secondary index for dangling entries. This requires
  a second scan which does the reverse point-lookup.


## [2] Do point-lookups directly during RowFetching for index checking

This is by far the easiest method to do as it only involves:
1) Collect the required secondary index data. The first code path that
   accesses required data is in `RowFetcher`.
2) Do point-lookups for the secondary index entry, and compare.

Step 1 is ideally done in the `RowFetcher` as this is also where we can
avoid passing decoded data further upstream, when we only need check
errors.

Step 2 can be optimized by batching the k/v lookups, which would be a
trivial mechanism to add inside `RowFetcher`. In this case, having an
alternative `NextRow` called `NextRowWithErrors` makes more sense to not
add this code to the critical code path.

A [proof of concept] has been done where this does not batch lookups or
propagate errors to the client. Errors encountered are just logged.

This is very similar to the alternative proposal [1]. This approach has
the same problems.

[proof of concept]: https://github.com/lego/cockroach/blob/0f022a8e0ef97d8233ccc8aa31466090ef3dee7e/pkg/sql/sqlbase/rowfetcher.go#L651-L673

## [3] Creating the full logical plan during query planning -- transforming SCRUB planNodes into the check logic.

As an alternative to section 3, during query planning we can transform
the scrub planNode into a plan that would execute all of the check
logic. The goal of this would be to use existing planNodes to express
the check logic, as some of the checks such as index checks can be
expressed completely through SQL as seen in section 1. This was not
picked as the strategy to implement because there were more challenges
with this approach.

For this option, during the planning phase, we will expand a `Scrub`
planning node to include plans that encompass all of the checking work.
Because of this, the plan will not need to run additional plan and
execution phases.

This may be easier to implement as we don't need to be concerned with
creating a new distSQLPlanner, it will be limited in a few ways. If any
part of the `SCRUB` involves plan nodes that cannot be executed in the
distSQL execution engine, we have no choice but to implement those
features or come up with a workaround.

One example of that problem is the need for a form of `UNION` or special
case workaround to aggregate the error results for multiple index
checks. To do this, we will need to add some form of `UNION` or special
case workaround to do the aggregation and sequential execution step for
this.

A special case where we have new plan nodes specifically to union the
errors might look like:

```go
type ScrubCheckNode {
  // Sub-plan which checks a particular aspect.
  Checks []ScrubCheck
}

type ScrubCheck {
  Check CheckType
  // Plan for running the check.
  plan *plan
}
```

Then during distSQL physical plan creation, we will plan each
`ScrubCheck.plan` and route their streams into a final aggregator. This
may be a problem though, as this will run all checks in parallel and not
in sequence, which will adversely affect foreground traffic.

Lastly, we may need to have an alternative to a `TableReader`, call it
`TableChecker`, where we call the iterator `NextRowWithErrors` instead of
`NextRow` to produce all physical errors.

# Unresolved questions
