- Feature Name: schema_gossip
- Status: completed
- Start Date: 2015-07-20
- RFC PR: [#1743](https://github.com/cockroachdb/cockroach/pull/1743)
- Cockroach Issue:

# Summary

This RFC suggests implementing eventually-consistent replication of the SQL schema to all Cockroach nodes in a cluster using gossip.

# Motivation

In order to support performant SQL queries, each gateway node must be able to address the data requested by the query. Today this requires the node to read `TableDescriptor`s from the KV map, which we believe (though we haven't measured) will cause a substantial performance hit which we can mitigate by actively gossiping the necessary metadata.

# Detailed design

The entire `keys.SystemConfigSpan` span will have its writes bifurcated into gossip (with a TTL TBD) in the same way that `storage.(*Replica).maybeGossipConfigs()` works today. The gossip system will provide atomicity in propagating these modifications through the usual sequence number mechanism.

Complete propagation of new metadata will take at most `numHops * gossipInterval` where `numHops` is the maximum number of hops between any node and the publishing node, and `gossipInterval` is the maximum interval between sequential writes to the gossip network on a given node.

On the read side, metadata reads' behaviour will change such that they will read from gossip rather than the KV store. This will require plumbing a closure or a reference to the `Gossip` instance down to the `sql.Planner` instance.

# Drawbacks

This is slightly more complicated than the current implementation. Because the schema is eventually-consistent, how do we know when migrations are done? We'll have to count on the TTL, which feels a little dirty.

# Alternatives

We could augment the current implementation with some of:
- inconsistent reads
- time-bounded local caching
This will be strictly less performant than the gossip approach but will be more optimal in memory as nodes will only cache schema information that they themselves need. Note that at the time of this writing every node will likely need all schema information due to the current uniform distribution of ranges to nodes.

We could have a special metadata range which all nodes have a replica of. This would probably result in unacceptable read and write times and induce lots of network traffic.

We could implement this on top of non-voting replicas (which we don't yet support). That would give us eventual consistency without having to go outside of raft, but enforcement of schema freshness remains an open question.

# Unresolved questions

How do we measure the performance gain? What should we set the TTL to?
