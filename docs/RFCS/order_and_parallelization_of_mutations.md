- Feature Name: Reordering and parallelization of multi-row mutations
- Status: draft
- Start Date: 2018-03-12
- Authors: knz
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #23698

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

# Summary

A short write-up of the restrictions to re-order and parallelize
INSERT/DELETE/UPDATE/UPSERT (mutation statements) when they operate
over multiple rows.

tl;dr: the data source of a mutation can always be distributed. UPDATE
and DELETE can be distributed too if there are no FK cascading
actions. In the other cases, "it depends". The RFC below details these
cases.

# Motivation

Optimization of queries that include mutation statements over multiple rows.

# Guide-level explanation

A mutation statement is characterized by the following attributes:

- its *data source*, e.g. VALUES, SELECT, or implicitly the data
  already in the table operated upon.

- which *operation*, e.g. deletion, update, insertion, upsertion. Note that
  a single SQL keyword can imply different operations depending on the
  operands and sub-clauses, for example INSERT can either operate an
  insert or an upsert.

- whether or not it has a *RETURNING clause*.

- whether or not it has a *mandated operation order* (ORDER BY). For example,

      DELETE FROM a ORDER BY x LIMIT 1; -- only delete first row by order x
      DELETE FROM a ORDER BY x RETURNING x; -- delete rows in order x, return results in that order

  The operation order constraints:
  - the final result in the database observable by a subsequent SELECT statement,
  - and also the values returned by the RETURNING clause, if any.

  Note that mandated orders can matter in non-obvious ways. For
  example, even the order of a deletion matters, even though the
  deleted rows are not there anymore in the end. For example, if there
  is an ON DELETE CASCADE SET DEFAULT on a FK relation, and the
  DEFAULT clause of the referenced column uses `now()` or a sequence,
  then the results of the cascade should be ordered in a way
  compatible with the mandated deletion order.

  Note that ORDER BY on the mutation statement itself is not the same
  as ORDER BY on the data source, for example:

      INSERT INTO a SELECT * FROM b ORDER BY x; -- ORDER BY on data source, no mandated operation order (*)
      INSERT INTO a (SELECT * FROM b) ORDER BY x; -- ORDER BY on INSERT, mandates operation order

- whether or not it *requires read-then-write* or not (write-only)
  *inside the mutation itself* (i.e. not the data source).

  The special restriction "inside the mutation itself" is applicable
  to e.g. DELETE. The data source of DELETE is the table being
  operated on, because DELETE needs to read the rows to determine
  their PKs. However once the data source has returned these PKs,
  there is no further read to be done by the delete operation itself
  in the absence of cascading operations.

  In general, a mutation statement that operates on a table without
  secondary indexes (and no FK relations), and doesn't need the
  current values to decide the final values, does not require
  read-then-write. For example:

      DELETE FROM kv; -- no FKs, no secondary indexes, just delete: does not require read-then-write
      UPSERT INTO kv(k) VALUES (1) -- requires read-then-write to read the current value of `v`
                                   -- in case there is a conflict (with no conflict, this inserts
                                   -- v = NULL into the new row).

  Note that despite its name, the UPDATE statement generally does not
  need read-then-write, because it is the data source that is in
  charge of extracting the current values for all the table columns.

- for mutations that do require read-then-write, whether or not it has
  a *carried dependency* from one row to the next, that is, the
  results from a mutation on one row must be read back before another
  row is written to. For example,

      INSERT INTO kv(k,v) (SELECT a,b FROM ab) ON CONFLICT DO UPDATE SET v = v + a;

  Note that the presence of a carried dependency does not mandate a specific
  operation order; however it does impact parallelizability, as discussed below.

(*) A separate discussion could cover whether it is desirable for UX to
specify that an ORDER BY in the data source "bleeds into" a mutation
order. This is out of scope here.

# Detailed design

The logical planning for a mutation first plans the data source, then plans the mutation itself.

The following separate questions must be answered:

- Can the **data source query be distributed**?

  Answer: **yes always**, regardless of carried dependency, mandated ordering, etc. of the mutation itself.

- Can the **results produced by the data source query be re-ordered**
  arbitrarily (e.g. by avoiding a sorter to re-order the distribution
  results)?

  Answer: yes, if and only if either:

  - there is no ORDER BY on the data source clause, or

  - there is no mandated mutation ordering and we're clear in
    docs that the source ordering and the mutation ordering are two
    different things.

    Again, a separate discussion can argue whether it is good UX to
    separate them, but this is out of scope here. If we decide that
    ORDER BY on the data source clause implies a mandated mutation
    ordering, then the presence of ORDER BY on the data source clause
    will pre-clude re-ordering of the data source results.

- **Can the mutation itself be distributed or parallelized**,
  regardless of whether or not the data source is distributed and/or
  reordered?

  Answer:

  - never if there is a carried dependency on the mutation. (otherwise the mutation can violate txn serializability)
  - (I think?) never if the mutation requires read-then-write, because two concurrent KV batches can performs the reads before
    any of the writes are performed.
  - otherwise, if there is a mandated ordering:
    - cannot distribute/parallelize if there are cascading actions that are order-sensitive. (otherwise the mutation can produce incorrect results)
    - otherwise, yes, but a sorter must be used to ensure the RETURNING rows are produced in the mandated order, if there is a RETURNING clause.
  - otherwise, yes, and no extra sorter is needed.

## Unresolved questions

Whether read-then-write also prevents distribution/parallelization of
the mutation when there is no carried dependency. (I think it does,
but I cannot clearly articulate why).

# Appendix: enumeration of supported mutations

Here are some example SQL mutations that CockroachDB supports,
demonstrating how attributes differ, that show case all the
combinations of mutation syntax supported by CockroachDB. We assume in
these examples there are no FKs and thus no CASCADE clauses on the
related tables.

| Syntax                                                                         | Supports arbitrary data sources | Operation | Requires read-then-write                 | Carried dependency                 |
|--------------------------------------------------------------------------------|---------------------------------|-----------|------------------------------------------|------------------------------------|
| `DELETE FROM a`                                                                | no (DS = always target table)   | deletion  | never for DELETE unless FKs              | never for DELETE unless FKs        |
| `UPDATE a SET v = v + 1`                                                       | no (DS = always target table)   | update    | never for UPDATE unless FKs              | never for UPDATE unless FKs        |
| `UPSERT INTO kv(k,v) VALUES (1,2)                                              | yes                             | upsertion | no (all columns written)                 | never for UPSERT unless FKs        |
| `UPSERT INTO kv(k) VALUES (1)                                                  | yes                             | upsertion | yes (to determine previous value of `v`) | never for UPSERT unless FKs        |
| `INSERT INTO a VALUES (1)`                                                     | yes                             | insertion | no                                       | no                                 |
| `INSERT INTO kv(k,v) VALUES (1,2) ON CONFLICT DO NOTHING                       | yes                             | upsertion | yes (to determine conflicting rows)      | no                                 |
| `INSERT INTO kv(k,v) VALUES (1,2) ON CONFLICT DO UPDATE SET v = excluded.v`    | yes                             | upsertion | yes iff there are secondary indexes      | no                                 |
| `INSERT INTO kv(k) VALUES (1) ON CONFLICT DO UPDATE SET k = excluded.k*10`     | yes                             | upsertion | yes (to determine previous value of `v`) | no                                 |
| `INSERT INTO kv(k) VALUES (1) ON CONFLICT DO UPDATE SET v = kv.v + excluded.v` | yes                             | upsertion | yes (to determine previous values)       | yes (sensitive to previous values) |

Another formatting for the same information, enumerated by combination
of attributes:

| Operation | Requires read-then-write             | Carried dependency | Example statement                                                       |
|-----------|--------------------------------------|--------------------|-------------------------------------------------------------------------|
| deletion  | no (always unless FKs say otherwise) | no (always)        | `DELETE FROM a`                                                         |
| update    | no (always unless FKs say otherwise) | no (always)        | `UPDATE kv SET v = v + 1`                                               |
| insertion | no (always unless FKs say otherwise) | no (always)        | `INSERT INTO a VALUES (1),(2),(3)`                                      |
| upsertion | no (when FKs don't say otherwise)    | no                 | `UPSERT INTO kv VALUES (1,2)`, or `INSERT INTO kv VALUES (1,2) ON CONFLICT DO SET v = excluded.v`, with no secondary indexes |
| upsertion | yes                                  | no                 | `UPSERT INTO kv VALUES (1,2)`, or `INSERT INTO kv VALUES (1,2) ON CONFLICT DO SET v = excluded.v`, with secondary indexes    |
| upsertion | yes                                  | yes                | `INSERT INTO kv VALUES (1,2) ON CONFLICT DO SET v = kv.v + excluded.v`    |

All the mutations can be combined arbitrarily with mandated orders or
the presence of RETURNING clauses.

## How to determine the various attributes of a mutation

- the data source, the operation, the mandated ordering and the
  presence of RETURNING are always specified in the syntax of the
  statement itself.

- whether or not read-then-write is required is determined as follows:

  - if there are FK relations and a CASCADE clause requires
    read-then-write in its resolution, then the mutation also
    requires read-then-write regardless of its syntax.

  - if there are no FK relations or no CASCADE clause on the related
    tables, then read-then-write is determined by the syntax and the
    schema of the table being operated upon -- the attribute depends
    on whether all the columns on the table are being written to.

- whether or not there is a carried dependency is determined both by
  the syntax and the schema of the table being operated upon -- the
  attribute depends on whether all the columns on the table are being
  written to.
