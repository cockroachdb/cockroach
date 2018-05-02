- Feature Name: Auto-auto-parallelization of mutations (auto-RETURNING-NOTHING)
- Status: draft
- Start Date: 2018-05-02
- Authors: knz
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

# Summary

- A new SQL session setting `asynchronous_mutations`
- When set, all mutations without any RETURNING clause behave as if RETURNING NOTHING was specified.
- Results (row counts + errors) accumulated in session.
- New statement `WAIT` waits for completion of pending mutations and
  returns a table containing statements, row counts and errors.
- If a client closes a connection without issuing WAIT, then mutations complete in the background (i.e.
  effective session finalization delayed until execution completes).
- Optional: new session var set automatically based on `application_name` (see discussion below)

For example:

```
-- at start of session
> SET asynchronous_mutations = TRUE;

-- later, client txn
> INSERT INTO blah VALUES (1,2), (2,3);
> INSERT INTO blah VALUES (3,4), (1,4);

> WAIT;
+---+----------------+------+---------------------+
| n | statement type | rows | error               |
+---+----------------+------+---------------------+
| 1 | INSERT         |   2  |  NULL               |
| 2 | INSERT         | NULL | duplicate value     |
+---+----------------+------+---------------------+
```

Expected impact: more performance of mutations for apps where the dev
is willing to make a tweak (but see also discussion below).

# Motivation

Overheard from Spencer: "the fact you have to specify RETURNING
NOTHING boggles my mind. Parallelization should be enabled by
default. Then I hear someone say 'wait we can't do that because of SQL
semantics', well, {censored} SQL semantics!"

So, compatibility with clients written for pg _is_ a thing but we'd
like to avoid this getting in the way of apps getting good (better)
performance.

# Guide-level explanation

When this feature is implemented, CockroachDB will function in
"compatibility mode" by default for mutations and process all
mutations synchronously.

However an application developer can set a session variable to
activate a "fast mode" (which session variable is discussed further
below). Once that mode is enabled, CockroachDB accepts mutation
statements (INSERT/UPDATE/etc) faster than they are processed, so that
they can be processed in parallel in multiple cores/nodes.

(TODO: check the following paragraph. This may be an opt-in feature.)
When the fast mode is enabled however, a mutation statement may not
"see" the values inserted/updated by a previous statement
automatically -- i.e. CockroachDB may not automatically enforce
dependencies, for better performance.

Finally, clients can then either ignore the outcome (the mutations
will be eventually processed), or check for results (which errors have
happened or how many rows were mutated) with the new `WAIT` statement.

# Reference-level explanation

The implementation will be a tweak to the current statement queue, to
avoid the automatic synchronization that currently occurs.

To measure adoption of the feature, the statement collection
infrastructure would use a new flag (context: there's a flag column in
the statement stats) to annotate mutations issued asynchronously.

## Overlap with other proposals

There's a current proposal floated in the perf team to remove
RETURNING NOTHING entirely, and instead decide the outcome of mutation
statements by performing KV reads upfront.

One advantage would be to remove the concurrent goroutines currently
created in each sql session to process RETURNING NOTHING statements,
because the SQL/kv interface doesn't like concurrent batches in the
same kv txn.

That proposal and the one here are complementary.

To implement the auto-auto-parallelization described here, the
statements issued asynchronously could land in a _sequential_ queue of
mutations in a concurrent, independent SQL session that shadows the
session where the statements are issued. In that shadow session there
would be a distinct kv txn where mutations would be processed
sequentially. WAIT would then simply collect the sequential output of
the shadow session.

## Discussion of how to enable this

Main proposed mechanism: new session variable
`asynchronous_mutations`. Defaults to **false**

How to get the best opt-in UX? A new session variable means app _code_
need to be modified to use it.

Complement feature: set `asynchronous_mutations` automatically based
on the value of `application_name`. Benefits:

- can be enabled by a _config_ change (not code change) in most client apps.
- can be enabled via the pg connection URL (`&application_name=...`)
- typically reflects the reality of multi-app deployments: some apps
  will want to use the feature, other apps don't want
  to. `application_name` is the canonical discriminant between
  different apps.

How to achieve this:

- special value of `application_name` as a whole enables
- special character at the beginning of `application_name` enables
- auto-config of multiple session variables based on a per-app configuration table, i.e. **per-app defaults for all session vars**

I'd like to explore the last one. It's appealing for a different
reason: this would enable us to introduce various "postgres
compatibility settings" which default to "compatibility" but can be
overridden for specific apps in different ways. This is out of scope
for this RFC but the feature discussed here could pave the way / serve
as experiment.

## Drawbacks

Why should we *not* do this?  It adds a little more complexity to CockroachDB.

## Rationale and Alternatives

Alternatives that were considered:

- do nothing. Status quo. Users complain about mutation performance
- Make synchronous mutations faster. Sequential bottleneck.
- new ASYNC/AWAIT statements that create futures on statement results
  and allow client to wait asynchronously on them. This would provide
  maximum control to clients, but more complex to implement. It's also
  harder to opt in by client apps, more app changes needed.

## Unresolved questions

- Whether to do this for mutations outside of explicit BEGIN/COMMIT
  blocks, inside, or both. (I suggest: both. Not sure if there are
  blockers.)

- Whether COMMIT would automatically WAIT if the feature is enabled
  for a session. (I suggest: no. KV Txn completes in background. No
  DDL allowed in that case.)
