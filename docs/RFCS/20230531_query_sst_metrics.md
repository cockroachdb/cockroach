- Feature Name: sstable_metrics
- Status: draft 
- Start Date: 2023-05-31
- Authors: Rahul Aggarwal
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/102604

# Summary

Storage Team engineers are often involved in support escalations from customers
that require inspection of SSTable-level statistics. Currently, these
statistics are difficult to obtain and require work from the customer and
support teams to find appropriate files to pull from the filesystem to send to
us. As a result, this RFC outlines how we will add the ability for operators to
query sstable metrics which is useful for debugging storage issues pertaining
to a specific key range. This will be implemented using a set-generating
function (SRF) and used as follows.

```SELECT * FROM crdb_internal.sstable_metrics('start-key', 'end-key')```

or 

```SELECT * FROM crdb_internal.sstable_metrics(node-id, store-id, 'start-key', 'end-key')```


# Technical design

Audience: CockroachDB team members

## Cockroach Side

The proposed solution is creating a new SRF which will be added to the existing
[built-in
generators](https://github.com/cockroachdb/cockroach/blob/3526c9ca65a94bd751b23a65b3c96a1513c961bc/pkg/sql/sem/builtins/generator_builtins.go#L107).
This SRF will have two overloads (for the two variants above). The latter one
only talks to one node, while the other will need to send an RPC to each node
in order to retrieve all the relevant SSTables. This can be achieved by calling
[Dial](https://github.com/cockroachdb/cockroach/blob/7d8e56533549abedd7ceceeafc469ca4e224e4ed/pkg/rpc/nodedialer/nodedialer.go#L101)
for each separate node inside of a function that is part of `evalCtx`. This
function will call out to the
[StorageEngineClient](https://github.com/cockroachdb/cockroach/blob/213da1f9fb591d90dcea6590b31da8c55b0756f9/pkg/kv/kvserver/storage_engine_client.go#LL23)
and be handled in the [stores
server](https://github.com/cockroachdb/cockroach/blob/5b6302b2ed2a83f49b55329ce3cac5f6135d0aea/pkg/kv/kvserver/stores_server.go#L24)
(see Pebble side). 

The SRF will be structured similar to
[json_populate_record](https://github.com/cockroachdb/cockroach/blob/f97d24aa661d8e1561f27a740325ebdabd62c926/pkg/sql/sem/builtins/generator_builtins.go#L383-L385)
i.e. using a generator to return each output row.

Columns to display: 

- node_id
- store_id
- level
- file_num
- metrics

## Pebble Side

Inside the store's server code is where Pebble will be used, specifically
[DB.SSTables](https://github.com/cockroachdb/pebble/blob/25a8e9bb8d9586e5090979f24dec11712e9f4b3c/db.go#L1912).
When calling `DB.SSTables` we will need to specify a `SSTableOption` which will
be a function allowing us to filter SSTables for the key range specified by the
user. Note that filtering can be performed based on the `FileMetadata` alone,
which allows us to skip unnecessary `getTableProperties` calls (which can read
metadata from storage and affect caches).

## Implementation Steps 

1. Define the required functions with dummy implementations
2. Create the new SSTableOption that filters by key ranges
3. Implement the generator function for single node queries
4. Add logic to support multiple node queries

## Unresolved questions

Audience: all participants to the RFC review.

1. How will we get a list of all the nodes we will need to send an RPC request? 

One option is to only support global keys in the variant in which the user does
not specify a specific node.
