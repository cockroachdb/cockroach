- Feature Name: Distributed CSV Loading
- Status: draft
- Start Date: 2017-07-11
- Authors: Matt Jibson, Daniel Harrison
- RFC PR:
- Cockroach Issue:

# Summary

Add an `LOAD CSV` SQL statement that reads CSV files and converts them into enterprise BACKUP format. These can then be restored via the normal RESTORE statement.

# Motivation

Users desiring to try CockroachDB need a fast way to import their data. CSV appears to be the most common format between databases and it is the first we have decided to support.

As a first implementation, we are assuming that CSV files exist not in cloud storage but instead evenly on each node. For example, if there is some existing Cassandra cluster, each node exports whatever data it is in charge of into CSV on its local disk. We also assume there is enough spare disk space for the processing and importing of this data. A later feature will allow for CSV files to be in cloud storage. Some pre-processor will have to split up these CSV files into chunks (since it could also be just a single, large CSV file) and divide up those chunks among nodes. This would mean adding a seek or read-at-position method to our export store interface. However after this step, the remainder is the same.

# Detailed design

The backup file format is set of SST files. These files must be non-overlapping. The input CSV files may be in some order, but we need to put them into CockroachDB-sorted order so that we can create non-overlapping files. The difficulty then is sorting a large number and size of CSV files (where size is larger than the memory on a single or even all of the machines).

A coordinator node receives a SQL command to import a set of CSVs that represent a single table from a specific directory that is relative to each machine, for example:

`LOAD CSV FROM "/data/csv"`

The coordinator instructs (with some new RPCs) all nodes to read all CSV files from the `/data/csv` directory local to each of them. The nodes read all of their CSV files, convert each row into KVs, and write those KVs to a single RocksDB instance. The cockroach store temp directory is used as temp space for the RocksDB instance.

A sampling of the keys is taken and sent to the coordinator.

The coordinator uses the sampling to produce a set of splits, and one node is assigned to each split. The coordinator sends the list of all splits and the target node to all nodes (so that each node knows where to send any row they encounter).

The nodes iterate through the RocksDB data and send each KV to the appropriate node. Each node opens a second RocksDB instance. As KVs come in to the node, they are added to the second instance. When a node has read all files it sends a message back to the coordinator.

Care must be taken here to ensure that nodes acknowledge the receipt of KVs, otherwise there are race conditions where a node could respond to the coordinator that it is done reading CSVs, but the receiving node hasn’t gotten its final KVs before the coordinator sends the splits to all nodes, thus missing some KVs. In order to prevent this, nodes use a gRPC stream to send KVs. The sender closes the stream when done writing, and waits for a response acknowledging closure.

Once all nodes are done reading, the coordinator sends a message for the nodes to create SST files. Nodes iterate through their splits and scan the RocksDB instance for KVs in a split range. A single SST file is created and named for each split and copied to the destination export store. Nodes respond when done.

Once all nodes have completed writing SSTs, the coordinator knows where each split is and what it is named (since files are named based on the split). It creates the BACKUP descriptor from these names, and is then able to run a RESTORE.

This design also depends on a new kind of export storage that uses a subdirectory of cockroach’s store tmp directory that is specified in the host field. For example, the storage `cockroach-tmp://csv-backup` uses the directory `csv-backup` in the store’s tmp directory. The directory of file names in this export storage specify which node a file is on. For example, the file `cockroach-tmp://csv-backup/n1/example.sst` lives in the `csv-backup` directory of node 1 and is named `example.sst`.

So the full statement may be something like:

`LOAD CSV FROM "/data/csv" TO "cockroach-tmp://csv"`

## RPCs

The new RPCs outlined below.

```
// Initial message from coordinator to all nodes to read CSVs in a directory.
message ReadCSVRequest {
	string path = 1;
}

// After reading CSVs, nodes send a single message containing a sampling of the KVs from the CSVs.
message ReadCSVResponse {
	message Sample {
		//
	}

	repeated Sample samples = 1;
}

// After the coordinator has collected all samplings and calculated splits, the splits are sent to each node.
message RouteKVRequest {
	message Split {
		roachpb.Span span = 1;
		Int32 node_id = 2;
	}

	repeated Split splits = 1;
}

// An empty message is sent after each node has completed routing the KVs
message RouteKVResponse {
}

// KVs streamed between nodes.
message KV {
	repeated roachpb.KeyValue kvs = 1; // repeated so KVs already in order can be batched together
}

// Send to the KV sender after a receiver of KV messages has written the last KV message of the stream.
message WroteKVs {
}

// The coordinator sends this to all nodes once all nodes have completed routing.
message WriteSSTRequest {
	roachpb.ExportStorage dir = 1;
}

// An empty message is sent after each node has completed writing SSTs.
mesage WriteSSTResponse {
}
```

# Drawbacks

## Node failures

If any node fails, the entire process would have to be restarted. A fault-tolerant design will be addressed in a later design.

## New stuff

This design requires various new RPCs, a new export store, and possibly some flow control issues. There are some unknowns about adding this many new things that could go wrong.

## Scratch space

Assuming this is being done on a live cluster, this design requires at most about 5x the disk space:

The live data of the existing database
CSV data
Unsorted KV temp files
Sorted KV files for the backup
Restored Cockroach data

The data in any step can be deleted after the previous step is complete. However since verification of the Cockroach data will take some time, it seems that only step 3’s data can safely be deleted after step 4.

If needed, we could also skip step 3 by performing 2 passes over the CSV file. The first to take a sampling of keys and the second to convert each row into a key that is then routed correctly. This is possibly faster anyway since it removes an entire write step and doesn’t add an additional read step. CSV decoding is fast, and we don’t need convert each row into KVs twice, only the sampled rows would be converted twice. However this depends on correctly estimating CSV -> KV row sizes and the number of rows in the CSV file. We will keep this as a possible improvement for the future.

# Alternatives

DistSQL was investigated as a way to do some of these tasks, but was rejected because it would add too much special case handling of this work. DistSQL cares a lot about data ranges and where they live, but this problem just needs a bunch of available workers. Thus the DistSQL routing code would have to be taught about this kind of no-range workload. We think it will be better for both DistSQL and this work to implement solutions using our RPC layer directly. Although DistSQL does provide many flow control and error handling options, GRPC also provides many of those features, so it should be adequate.

# Unresolved questions
