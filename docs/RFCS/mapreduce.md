- Feature Name: MapReduce
- Status: draft
- Start Date: 2017-07-16
- Authors: Matt Jibson
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Add new mapreduce RPCs to enable arbitrary map reduce jobs to be performed
across the cluster. This first version’s goals are basic functionality,
and does not cover fault tolerance. However we hope to add that in the future,
so this design attempts to structure the RPCs in a way that would make adding
this straightforward.

# Motivation

Although DistSQL handles many kinds of distributed work, we are increasingly
finding use cases where having a distributed computation framework not limited
by DistSQL’s assumptions (which care about data locality wrt the running job)
would be useful. For example, some of the background work performed by schema
changes just needs nodes to run on: it doesn’t matter particularly which
node the worker is running on. Also, the upcoming distributed CSV loading
needs to perform a distributed sort of data not present in the cockroach
cluster, and so also just needs some nodes to do work.

# Detailed design

A new mapreduce package will be created to hold both the implementation of
each mapreduce job type and the coordination and communication logic. A
mapreduce job can be defined by a struct with an init, map, and reduce
field, each of which are functions implementing their given portions. The
mapreduce logic handles setting up the mappers, reducers, and the KV stream
from mappers to reducers.

A controller node creates a mapreduce job by specifying the job type (a
string) and arguments (a `[]byte`). Based on the job type, an init function is
run. The arguments, since they are a `[]byte`, can be decoded into whatever
data structure the init function expects.The init function creates a job
specification, which contains a list of the reducers and mappers (nodes),
and a partition function with arguments.

The controller contacts all mappers and reducers, instructing them to start
their work. Each reducer contacts all mappers to request their partition
of data. Mappers generate KVs and route them to the appropriate connected
reducer based on the partition function. Reducers shuffle by putting incoming
KVs into a RocksDB instance. Once all incoming KVs are done, the reducer
iterates over the KVs in order, performing the reduction function. Once all
reducers have returned to the controller, the mapreduce is done.

Each mapreduce job generates a cluster-wide unique job ID. Each mapper and
reducer generates a unique task ID (for the job). Any number (0, 1, or more)
mappers or reducers can be assigned to each node, so the combination of job and
task IDs enable the correct assigning of reducer stream requests to mappers.

Mappers, on creation, create a route map from reducer task ID to a KV
chan. When the map implementation produces a KV, the partition function
is invoked. It takes a key and produces a task ID. The mapper consults the
route map for the hashed task ID and sends the KV on that chan. If there is a
reducer stream consuming it, the KV will be picked up by the stream and sent
to the reducer. If not, the send will block until the stream is present. This
send will use a select so that we can also listen for a context close.

Reducers contact all mappers with a request to stream KVs. This stream call
blocks until the mapper’s route map is created, then listens on its chan,
based on the reducer’s task ID.

Most channel send and receive operations are paired in a select on a context
to allow for cancellation.

## Proposed protobuf

```
message KV {
  bytes key = 1;
  bytes value = 2;
}

message Task {
  int32 task_id = 1;
  string node_addr = 2;
}

message Params {
  bytes args = 1;
  repeated Task mappers = 2;
  repeated Task reducers = 3;
  string partition_func = 4;
  bytes partition_args = 5;
}

message Job {
  string type = 1;
  int64 job_id = 2;
  Params params = 3;
}

message MapRequest {
  Job job = 1;
  int32 task_id = 2;
}

message MapResponse {
}

message StreamRequest {
  int64 job_id = 1;
  int32 mapper_id = 2;
  int32 reducer_id = 3;
}

message ReduceRequest {
  Job job = 1;
  int32 task_id = 2;
}

message ReduceResponse {
}

service MapReduce {
  rpc Map (MapRequest) returns (MapResponse) {}
  rpc Stream (StreamRequest) returns (stream KV) {}
  rpc Reduce (ReduceRequest) returns (ReduceResponse) {}
}
```

# Drawbacks

There are many go routines, chans, and contexts here, and it is difficult to
guarantee that a failure on any step will cancel the context and correctly
shut down all go routines on all nodes without leaving any of them blocking
forever and leaking.

# Alternatives

Instead of having the reducers contact mappers requesting a stream, mappers
could push data to reducers. This is slightly more complicated since gRPC
doesn’t allow the initial message of a stream to be different than the
streamed data. Since a mapper needs to identify the reducer it is intended
for, communicating this is difficult, even if mappers pushing to reducers
is more desirable than the proposed poll method.

# Unresolved questions

Does this design allow for fault tolerance in the future without changing
any protos?
