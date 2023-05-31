- Feature Name: sstable_metrics
- Status: draft 
- Start Date: 2023-05-31
- Authors: Rahul Aggarwal
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/102604

# Summary

Looking to add the ability for users to query sstable metrics via their CockroachDB shell. This will be implemented using a set-genearting function (SRF) and will be used as follows

```
SELECT crdb_internal.engine_stats('start-key', 'end-key')

or 

SELECT crdb_internal.engine_stats('start-key', 'end-key', store-id)
```

# Technical design

Audience: CockroachDB team members


## Cockroach Side

The proposed solution is creating a new set-generating function (SRF) which will be added in [built-in generator](https://github.com/cockroachdb/cockroach/blob/3526c9ca65a94bd751b23a65b3c96a1513c961bc/pkg/sql/sem/builtins/generator_builtins.go#L107) will need to be added. This SRF will need to send an RPC to each node in order to retrieve all the relevant SSTables. This can be achieved by calling [Dial](https://github.com/cockroachdb/cockroach/blob/7d8e56533549abedd7ceceeafc469ca4e224e4ed/pkg/rpc/nodedialer/nodedialer.go#L101) for each seperate node inside of a function that is part of `evalCtx`. This function will call out to the [StorageEngineClient](https://github.com/cockroachdb/cockroach/blob/213da1f9fb591d90dcea6590b31da8c55b0756f9/pkg/kv/kvserver/storage_engine_client.go#LL23) and be handled in the [stores server](https://github.com/cockroachdb/cockroach/blob/5b6302b2ed2a83f49b55329ce3cac5f6135d0aea/pkg/kv/kvserver/stores_server.go#L24) (see Pebble side). 

The SRF will be structured similar to [json_populate_record](https://github.com/cockroachdb/cockroach/blob/f97d24aa661d8e1561f27a740325ebdabd62c926/pkg/sql/sem/builtins/generator_builtins.go#L383-L385) using a generator to return each output row. All necessary data should be available on `FileMetadata`.

## Pebble Side

Inside the store's server code is where Pebble will be used, specifically [DB.SSTables](https://github.com/cockroachdb/pebble/blob/25a8e9bb8d9586e5090979f24dec11712e9f4b3c/db.go#L1912). When calling `DB.SSTables` we will need to specify a `SSTableOption` which will be a function allowing us to filter SSTables for the key range specified by the user. Note for this step, we will use `FileMetadata` instead of `getTableProperties` to avoid reading metadata from storage and putting it in a cache. 

# Unresolved questions

Audience: all participants to the RFC review.

1. How will we get a list of all the nodes we will need to send an RPC request to (in the case the user does not specify the node-id).
2. Output row format (what columns do we want)
- node_id 
- store_id 
- file_num
- json_props
