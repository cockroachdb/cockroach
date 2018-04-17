# Guiding principles for the SQL middle-end and back-end in CockroachDB

This tech nodes outlines some general guiding principles for future
work, during the period 2017-2019.

Reminder from "[The SQL layer in CockroachDB](sql.md)" - the SQL layer
in CockroachDB contains 5 main component groups, including:

- the SQL **front-end**, responsible for parsing,
  desugaring, free simplifications and semantic analysis; this
  comprises the two blocks "Parser" and "Expression analysis" in the
  overview diagram.

- the SQL **middle-end**, responsible for logical planning and
  optimization; this comprises the two blocks "(plan constructors)"
  and "Logical optimizer" in the overview diagram.

- the SQL **back-end**, of which there are actually two:
  - the distSQL back-end, itself comprised of its own physical
    planner, optimizer and runner, able to process any statement/query
    *for which a distSQL plan can be constructed*;
  - the **local runner**, responsible for everything else.

This tech note focuses on the latter two, although the general
principles introduced here may be applicable through the entire
CockroachDB project.

Original author: Raphael 'kena' Poss

## Overview

CockroachDB must:

1) catch up with and innovate beyond, over the course of five years,
what hundreds of database experts have built in tens of projects over
a period of thirty years.

2) do so incrementally: function, orthogonality (new!), then performance.

3) do so openly, transparently and dependably. Openness is about
readiness to interact with a community beyond Cockroach Labs;
transparency is about developing and sustaining trust of the community
in the quality of the code; dependability is about delivering what we
promised.

## Catching up and beyond

The main challenge to the first goal is that published articles, books
and documentation about the state of the art *describe* the work that
has been performed by other SQL implementors but is insufficient to
*reproduce* it.

There are two obstacles really.

The first obstacle is that the descriptions are largely
incomplete. This is an historical artifact of the community where the
work was performed: SQL engines have been mostly proprietary affairs,
where good technology is still hidden from study by secrecy and
software patents.

Reproducing a SQL engine using database literature is like trying to
[picture an elephant without ready access to
one](https://www.google.com/search?tbm=isch&q=medieval+drawings+of+elephants). The
experience is very different from e.g. what programming language
experts are used to, where published literature often contains
complete instructions sufficient to reproduce the results, or links to
source code to achieve the same. These luxuries are simply absent from
most significant published works on databases.

The main issue that a reader of database literature has to contend
with is *the lack of concrete description of the data structures used
to represent the concepts described*. As Fred Brookes puts it in the
Mythical Man-Month: *"show me your flowcharts and conceal your tables,
and I shall continue to be mystified. Show me your tables, and I won’t
usually need your flowcharts; they’ll be obvious"* later translated
by Linus Torvalds: *"Bad programmers worry about the code. Good
programmers worry about data structures and their relationships."*

To bring CockroachDB to the state of the art and beyond, it will thus
be insufficient to look at published artifacts and hope to be able to
follow the example of good performers. A large amount of
*reverse-engineering*, both of the literature and competing technology,
will be needed as well.

The second obstacle to catching up with the state of the art is that
the architecture of CockroachDB fundamentally deviates from the
assumptions that have driven SQL research in the past 30 years. To
understand why this matters, recall that the main question that drives
the design and implementation of a SQL optimization engine is the
following:

*what are the parameters that influence the execution latency, row
throughput and CPU/memory usage of a query, and how to decide which
SQL query plan combines those parameters in a way that minimizes
latency, maximizes throughput and minimizes CPU/memory usage?*

There are 3 moving pieces in this question:

- which parameters influence the performance of a query?
- for each candidate query plan, how to estimate performance for that plan?
- when considering multiple candidates, how to decide the best plan(s)?

Now, there has been a lot of research into the latter two of these
three items. But there's one thing that has barely changed since the
late 1960's: understanding of the low-level parameters that influence
performance.

Of these, we can make two groups: those parameters that are different
in CockroachDB for no good reason, and for which we can work to
converge with the state of the art; and those parameters that are
really fundamentally different for a good reason.

Some examples in the first group:

- Traditional relational engines know how to perform every SQL
  operation in a fixed, small RAM budget. CockroachDB does not know
  how to work with restricted memory budgets. This causes RAM usage to
  remain a significant parameter to query planning in CockroachDB,
  whereas it is largely irrelevant in the state of the art. We will
  work on removing this parameter.
- Traditional relational engines always assume up-to-date row counts
  in tables and cardinality estimates for indexed columns. These are
  the bread and butter of most costing functions to decide the best
  query plan candidate. CockroachDB currently maintains neither, but
  will soon.

Some examples in the second group:

| What happened in research before 1980, and remains since then. | What happens in CockroachDB |
|---------------------------|-----------------------------|
| Data is stored in equally sized pages; pages contain approximately equal numbers of rows. | Prefix compression for keys, Snappy compression for values. |
| Parallelism, if at all used in query optimization, only appears in data scans. | CockroachDB already contains a moderately complex distributed execution engine. |
| Parallelism, if at all used in query optimization, assumes a fixed number of processors. | CockroachDB node counts evolve dynamically, even possibly during query execution. |
| The overall latency of a query is solely decided by the number of rows processed in memory and the number of page accesses on disk. | Network effects and implicit retries dominate. |
| The costing estimates use a fixed ratio between disk and CPU performance. | Each node in a cluster can have a different CPU/disk performance ratio. Network performance evolves over time. |
| Each area of contiguous storage contains data from at most one table or one index. Storage locality maps 1-to-1 to query locality. | [Interleaved tables.](../RFCS/20160624_sql_interleaved_tables.md) |
| A logical range of values, in a SQL query, corresponds to a contiguous area of storage when the columns are indexed. | [Partitioned tables.](../RFCS/20170921_sql_partitioning.md) |

For this second group, original research will be needed to construct,
from the ground up, CockroachDB-specific replacements to the best
practices available in the literature and most competing products.

## Function, orthogonality, then performance

Regarding the relationship between function and performance: this is
an axiomatic *modus operandi* of this particular engineering
organization.

- "Make it work correctly quickly, only then make it work correctly and increasingly fast."
  a.k.a. "Make a MVP early, then use adoption to drive further development."
- Functional behavior and correctness (which outputs are produced for
  which inputs and why) comes before non-functional behavior (latency,
  throughput, jitter, resource costs).

Reminder: the motivation to prioritize function over performance is
that correctness would be much more difficult to obtain and
demonstrate otherwise.

I am adding
[*orthogonality*](https://en.wikipedia.org/wiki/Orthogonality_(programming))
in the mix, between function and performance.

Orthogonality is a general design principle for a software project
where the designers strive to evolve a small set of basic building
blocks (and actively eliminating redundancy over time whenever it is
discovered) while maximizing the number of their allowable
combinations.

From a management perspective, orthogonality is something that is
essential, but invisible in the short term. Introducing a focus on
orthogonality in a team tends to naturally push for modularity, DRY,
reusability, good abstractions and creative discovery of new useful
features for end-users, without having to train these individual
traits separately. It is the means by which we ensure we can keep
growing CockroachDB without making each future change incrementally
expensive.

## On openness, transparency and dependability

The challenges and goals set forth above create valid concerns for
external observers:

- will CockroachDB deliver on both performance and quality? Can we
  "catch up on 30 years of database research" without creating a messy
  product hacked together too fast?

- at every point in time when the team decides to transition from a
  functional, correct feature to a faster implementation of the same,
  how is it guaranteed that the faster implementation is as correct as
  the base, functional one?

These concerns are already highly relevant:

- the code base is already hard to understand! I posit that it is so
  because it was hacked together too fast, without general guiding
  principles and directions (such as those proposed in this
  document). Openness, transparency and dependability will offer our
  community a way to understand CockroachDB above and beyond what is
  currently implemented at each point in time.

- we have already made mistakes by introducing new bugs while
  "optimizing" the code to make it faster. Openness, transparency and
  dependability on the plan to implementation, and releasing
  intermediate functional features before they are optimized, will
  help building trust by establishing baselines for testing and validation.

The nuance between "openness", "transparency" and "dependability"
relates to the wants of the community:

- transparency is our ability to communicate clearly about "why" and "when".
- openness is our ability to answer "how" and "why not".
- dependability is our ability to match what we announce we will do, and what we end up doing.
