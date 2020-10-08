- Feature Name: Optimizer-driven foreign key cascades
- Status: in-progress
- Start Date: 2020-04-24
- Authors:
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #46674

# Summary and motivation

A foreign key (FK) constraint establishes a relationship between two columns (or
sets of columns) between two tables. There is a "child" side and a "parent" side
for each relationship; any value in the FK columns in the child table must have a
matching value in the parent table. This is enforced by the DBMS, through two
mechanisms:
 - checks: the DMBS verifies that the changed rows don't introduce FK
   violations. For example, when we insert a row in a child table, we check that
   the corresponding value in the parent table exists. When we delete a row from
   a parent table, we check that there are no "orphaned" rows in the child
   table. If a violation occurs, the entire mutation errors out.
   
 - cascades: for delete and update operations, a cascading action can be
   specified. This means that instead of erroring out during delete/update, a
   corresponding mutation is made in the child table. The most simple example is
   an `ON DELETE CASCADE`: in this case whenever we delete a row from a parent
   table, the DBMS automatically deletes the "orphaned" rows from the child
   table.

Foreign-key cascades use legacy execution code which assumes a foreign-key
relationship is a 1-to-1 mapping between indexes. We want to:
 - allow use of the optimizer intelligence (mostly locality-aware index
   selection) for foreign key operations;
 - remove the requirement of having an index on the child table side.

In 20.1 we have enabled optimizer-driven foreign key checks by default;
unfortunately without also supporting cascades, the legacy code is still needed
and as long as that's the case, the index-on-both-sides requirement is in
effect.

This RFC proposes a solution for implementing foreign key cascade logic in the
optimizer.

# Guide-level explanation

Functionally, we aim to reimplement semantics similar to the existing ones so it
should be mostly invisible to users (other than cascade queries being
significantly faster in some multi-region cases).

The feature will allow removal of the index-on-both-sides requirement (which
does have user-visible consequences) but that work is not specifically covered
by this RFC.


# Reference-level explanation

The feature builds upon the techniques used for implementing opt-driven foreign
key checks. For FK checks, the optimizer builds queries that look for foreign
key violations. A check query refers to a buffer that stores the input to the
mutation operator. Inside the optimizer, the check queries are part of the
larger relational tree for the mutation statement. In the execution layer, the
checks become separate "postqueries" that run after the main statement.

The FK cascades cannot directly use this model because cascading foreign key
relationships can form cycles; cycles allow a mutation to keep cascading an
arbitrary number of times.

The proposed solution is to plan one cascade at a time. At a high level, the
optimizer creates a plan for the mutation query along with cascades-related
metadata. If the mutation changed any rows, this metadata is used to call back
in the optimizer to get a plan for the cascade. The resulting plan can itself
have more cascades (and FK checks for that matter) which are handled in the same
manner.

Note that the notion of calling back in the optimizer is not new: it is used for
apply-join, and to some extent for recursive CTEs.

FK cascades are a special case of *triggers*: user-defined procedures that run
automatically when a certain table is being changed. Some DBMS (like Postgres)
use triggers to implement cascades. While we are not implementing triggers at
this time, we want the work for cascades to be easily reusable if we implement
triggers at a later time.


## Detailed design


### Optimizer side

#### Operator changes

We add a `FKCascades` field to `MutationPrivate`, which contains cascades
metadata:

```go
// FKCascades stores metadata necessary for building cascading queries.
type FKCascades []FKCascade

// FKCascade stores metadata necessary for building a cascading query.
// Cascading queries are built as needed, after the original query is executed.
type FKCascade struct {
  // FKName is the name of the FK constraint.
  FKName string

  // Builder is an object that can be used as the "optbuilder" for the cascading
  // query.
  Builder CascadeBuilder

  // WithID identifies the buffer for the mutation input in the original
  // expression tree.
  WithID opt.WithID

  // OldValues are column IDs from the mutation input that correspond to the
  // old values of the modified rows. The list maps 1-to-1 to foreign key
  // columns.
  OldValues opt.ColList

  // NewValues are column IDs from the mutation input that correspond to the
  // new values of the modified rows. The list maps 1-to-1 to foreign key columns.
  // It is empty if the mutation is a deletion.
  NewValues opt.ColList
}
```

The most important member of this struct is the `CascadeBuilder`:

```go
// CascadeBuilder is an interface used to construct a cascading query for a
// specific FK relation. For example: if we are deleting rows from a parent
// table, after deleting the rows from the parent table this interface will be
// used to build the corresponding deletion in the child table.
type CascadeBuilder interface {
  // Build constructs a cascading query that mutates the child table. The input
  // is scanned using WithScan with the given WithID; oldValues and newValues
  // columns correspond 1-to-1 to foreign key columns. For deletes, newValues is
  // empty.
  //
  // The query does not need to be built in the same memo as the original query;
  // the only requirement is that the mutation input columns
  // (oldValues/newValues) are valid in the metadata.
  //
  // Note: factory is always *norm.Factory; it is an interface{} only to avoid
  // circular package dependencies.
  Build(
    ctx context.Context,
    semaCtx *tree.SemaContext,
    evalCtx *tree.EvalContext,
    catalog cat.Catalog,
    factory interface{},
    binding opt.WithID,
    bindingProps *props.Relational,
    oldValues, newValues opt.ColList,
  ) (RelExpr, error)
}
```

This object encapsulates optbuilder code that can generate the cascading query
when needed.

#### Optbuilder

The main work in the optbuilder will be around adding `CascadeBuilder`
implementations for the various `ON CASCADE` actions. Note that the logic for
capturing the mutation input as a `With` binding already exists (it is used for
FK checks).

For testing, we introduce a `build-cascades` OptTester directive, which
recursively builds and prints out all cascade queries (with an optional "depth"
limit for cases with circular cascades).

#### Execbuilder

In addition to the query, subqueries, and postqueries, execbuilder produces a
list of `Cascade`s:

```go
// Cascade describes a cascading query. The query uses a BufferNode as an input;
// it should only be triggered if this buffer is not empty.
type Cascade struct {
  // FKName is the name of the foreign key constraint.
  FKName string

  // Buffer is the Node returned by ConstructBuffer which stores the input to
  // the mutation.
  Buffer BufferNode

  // PlanFn builds the cascade query and creates the plan for it.
  // Note that the generated Plan can in turn contain more cascades (as well as
  // postqueries, which should run after all cascades are executed).
  PlanFn func(
    ctx context.Context,
    semaCtx *tree.SemaContext,
    evalCtx *tree.EvalContext,
    bufferRef BufferNode,
    numBufferedRows int,
  ) (Plan, error)
}
```

The planning function is similar to the apply-join planning function (recently
reworked in [#47681](https://github.com/cockroachdb/cockroach/pull/47681)).
Internally, it uses a new Optimizer/Memo instance to plan the query and
execbuilds it against the same `exec.Factory` as the original execbuilder.

### Execution side

The execution side will have to handle the cascades and checks. The high-level
logic is to execute cascades using a queue, accumulating the checks along the
way:

```
 - While cascades list is not empty:
   - txn.Step() // create sequencing point in the transaction
   - pop off the first cascade in the list
   - add any new cascades to the cascades list
   - add any new checks to the checks list
   - execute the cascade
 - If there are checks to run:
   - txn.Step() // create sequencing point in the transaction
   - run all checks
```

## Drawbacks

In many common cases, the new logic will boil down to the exact same KV
operations; in these cases, the new logic only adds planning overhead. The
optimizer-driven FK checks have the same drawback, and we established that the
tradeoff is justified given the benefits. We expect this to be less of a concern
for cascades, as cascading queries tend not to be critical parts of realistic
workloads.

A drawback of the implementation is that we have to buffer mutation inputs when
there are cascades involved. Currently this happens in memory (though there's
no reason we can't use a disk-spilling structure in `bufferNode` if necessary).
Importantly, these buffers must be "kept alive" until all cascades have run.
Some optimizations are possible here (to figure out the earliest stage when a
buffer is no longer needed) but won't be explored in the initial implementation.

## Rationale and Alternatives

An alternate design was thoroughly considered; it involves planning all possible
cascades as part of the main query (similar to FK checks). The difficulty here
is that cascades can form cycles; for this to work:
 - the planning logic needs to "de-duplicate" cascades (that is, figure out when
   a similar cascade was already planned);
 - cascade queries must have a "parameterizable" input (since they can run
   multiple times, with different inputs);
 - we must have some kind of a representation of a "program" of how the cascades
   run (roughly a graph where an edge means that one cascade produces a buffer
   that should be used as an input to the other cascade).

These abstractions are difficult to reason about and would likely be difficult
to implement. The complexity would not be contained to the optimizer: the
execution layer would need to be able to execute the cascades "program". But
assuming we can do it, what are the tradeoffs of the two approaches?

##### Pros of the alternate "plan-all" approach

 - In simple cases (e.g. child table is not itself a parent of other tables),
   the planning process is more efficient because we plan everything at once and
   can cache the relational tree.

 - It is conceivable that having the cascades in the same relational tree would
   allow us to implement more optimizations (though we don't have significant
   ideas at this time).

##### Pros of proposed approach

 - In complex cases, the "plan-all" approach can end up planning many possible
   cascades that don't end up running. For a "dense" schema where most tables
   are involved in cascading FK relationships across all the tables, we will be
   forced to plan all these cascades that can could in principle trigger each
   other, even though in practice most queries may not result in any cascading
   modifications at all. The proposed solution would only plan the cascades that
   need to run.

 - The proposed approach is more conducive to implementing triggers. The problem
   explained above can be worse with triggers - we would be planning for all
   possible ways in which triggers can interact even when in practice these
   interactions happen very rarely.

A final, important point: the "plan-all" solution is almost a superset of the
proposed solution. The major parts of the proposed solution - planning the
various types of ON CASCADE actions, executor logic - would be required for the
alternative solution as well. Thus, even if we determine that we want the
plan-all solution at a later point, we will not have wasted a lot of work.
