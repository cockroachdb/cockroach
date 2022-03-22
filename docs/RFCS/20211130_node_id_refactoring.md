- Feature Name: Refactoring KV and SQL node iDs
- Status: draft
- Start Date: 2021-11-31
- Authors: Radu Berinde
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)


# Background

Originally, CockroachDB nodes always ran both SQL and KV; this assumption is
implicit in a lot of existing code but it is no longer true with multi-tenant
support, where a tenant can have SQL nodes that are not KV nodes.

Existing code used `roachpb.NodeID` everywhere to refer to a node, whether it is
in the context of a KV RPC or a SQL (eg DistSQL) RPC. With multi-tenant support,
a new type `base.SQLInstanceID` was added. Various pieces of the code were
refactored to use this ID instead. However, there is remaining code that uses
`NodeID` to refer to either a SQL instance or a KV node:
 - through the RPC code and the node-to-node (or SQL-to-SQL) external APIs. This
   code does not need to know the conceptual difference between them, and
   sometimes uses the `NodeID` type for both out of convenience (we want to avoid
   code duplication)
 - In DistSQL, we still use `NodeID` when distributing queries. We need to
   introduce a new concept for mapping from leaseholder KV nodes to
   DistSQL-capable SQL instances.

Allowing implicit use of `NodeID` for both concepts makes it easy for an
engineer who is new to this part of the codebase to add subtly incorrect code.

# Summary

This is a proposal for implementing these IDs in a type-safe way, which
disallows "unsafe" implicit conversions.

# Proposal

## Types

We propose using three types, as defined below. Note that the chosen names are
only for the purposes of this RFC (it is intentional that they are different
than any of the names currently used in the code). We will establish the final
names in the Naming section.

```go
// KVNodeID refers to a KV node.
// Valid IDs start with 1.
type KVNodeID int32

func (id KVNodeID) String() string {
  return fmt.Sprintf("n%d", id)
}

// SQLNodeID refers to a SQL instance. This can be an instance inside a single
// tenant or multi-tenant host cluster node or a standalone non-system tenant
// instance. Note that this type already exists as `SQLInstanceID`.
// Valid IDs start with 1.
type SQLNodeID int32

func (id SQLNodeID) String() string {
  return fmt.Sprintf("sql%d", id)
}


// GenericNodeID contains either a KVNodeID or a SQLNodeID. It is meant to be
// used in code that is used for both cases and is generally agnostic as to what
// the ID means (e.g. logging code).
// The zero value is not a valid ID. The internal integer value is positive for
// KVNodeIDs and negative for SQLNodeIDs.
type GenericNodeID int32

func (id GenericNodeID) String() string {
  switch {
  case id > 0:
    return KVNodeID(id).String()
  case id < 0:
    return SQLNodeID(-id).String()
  default:
    return "?"
  }
}

func (id KVNodeID) ToGenericNodeID() GenericNodeID {
  return id
}

func (id SQLNodeID) ToGenericNodeID() GenericNodeID {
  return -id
}
```

## Usage rules

Part of this proposal are also the following rules:
 - all KV RPCs use `KVNodeID` in their protos.
 - DistSQL RPCs and specs use `SQLNodeID`.
 - The only conversions allowed between these types are through the
   `ToGenericNodeID()` methods.
 - There are exactly two pieces of code which convert the numeric value from one
   type of ID to the other:
     * the server initialization code for single-tenant (or host) cluster node
       which sets up the `SQLNodeID` to equal the `KVNodeID`.
     * the DistSQL planner which knows that we need to deploy a flow to the
       `SQLNodeID` with the same numeric value as the leasehoder's `KVNodeID`.
       Note that this code does not run for non-system tenants; it will need to
       be generalized to more complex mappings between `KVNodeIDs` and
       `SQLNodeIDs`.

## Implementation proposal

A single change implementing the proposal might be too big. We propose to start
by introducing the new types, while still allowing use of the existing `NodeID`
(and allowing conversions between `NodeID` and all of the proposed types). Then
we can change code that uses `NodeID` piece-by-piece, with the eventual goal of
removing `NodeID` altogether.

## Naming

As mentioned, the names above were chosen only for the present discussion. We
can debate the actual names we will use in the code here. Current candidates:
 - `GenericNodeID`: `ServerID`
 - `SQLNodeID`: `SQLInstanceID`
 - `KVNodeID`: `KVNodeID`

# Alternatives

Using an interface (implemented by both `KVNodeID` and `SQLNodeID`) instead of
`GenericNodeID` was discussed. There is concern that the interface might regress
performance in important RPC paths. We could explore switching to an interface
later.
