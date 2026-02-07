# Backup Flow

Original Author: Kevin Cao

Last Update: June 2024

## Introduction

This document aims to describe the underlying process for performing a backup on
CockroachDB. 

## Initial Request

When a user initiates a backup with the `BACKUP INTO` command, the node
receiving the command, referred to as the coordinator node, will initialize a
new backup job and begin the process of planning the backup.

First, the backup targets are translated into the corresponding spans that need
to be backed up. These spans are then analyzed by
[backup_processor_planning.go](https://github.com/cockroachdb/cockroach/blob/bdae010c3704583677a80fc23c9e150dffc1ccd3/pkg/ccl/backupccl/backup_processor_planning.go)
and a distribued SQL plan is drawn up to determine which nodes are responsible
for the backup of the spans based on the replicas stored on the nodes.

Once the plan has been drafted, these assignments are forwarded to the SQL
servers on the nodes and [`BackupDataProcessor`s](https://github.com/cockroachdb/cockroach/blob/c7ef50846c992a99d5cd60bff8373c6df1c195a5/pkg/ccl/backupccl/backup_processor.go#L141-L164)
are created on each node to handle the backup of their assigned spans.

## Backup Processor

### Range-Scoped Spans

When each node receives their set of assigned spans, they first split up the
assignments into [*range-scoped spans*](https://github.com/cockroachdb/cockroach/blob/c7ef50846c992a99d5cd60bff8373c6df1c195a5/pkg/kv/kvpb/api.proto#L1785-L1859).
The initial spans are logical spans, with no regard for the physical distribution of the
key space defined by the spans. Converting these logical spans into spans defined by the range boundaries
will further optimize the subsequent KV export requests made later on, which we will cover in a
later section.

In other words, a logical span `[A, G)` may be partitioned by 3 ranges like so:

```
Span: [A------------------------------------G)
R1  : <---------D)
R2  :            [D------------------F)
R3  :                                 [F----->
```

After converting the original assigned span `[A, G)` into range-scoped spans, we
would instead have the set of spans `[A, D)`, `[D, F)`, and `[F, G)`.

### Workers

Once we have our range-scoped spans, we can begin the work of exporting the data
into SSTs. The `BackupDataProcessor` takes the set of spans and splits up the
work amongst some number of workers.

Each worker creates an SST file sink that they will begin writing SST data to as
they process each span. For each span, an
[`ExportRequest`](https://github.com/cockroachdb/cockroach/blob/c7ef50846c992a99d5cd60bff8373c6df1c195a5/pkg/kv/kvpb/api.proto#L1785-L1859) 
is made to the KV server to fetch the key-value pairs stored within that span.
The KV server will [process the
request](https://github.com/cockroachdb/cockroach/blob/c7ef50846c992a99d5cd60bff8373c6df1c195a5/pkg/kv/kvserver/batcheval/cmd_export.go#L95)
and then return an
[`ExportResponse`](https://github.com/cockroachdb/cockroach/blob/c7ef50846c992a99d5cd60bff8373c6df1c195a5/pkg/kv/kvpb/api.proto#L1882-L1915),
which contains an ephemeral SST file which stores the requested data within that
span.

Upon receiving the `ExportResponse`, the worker will decompress the ephemeral
SST and dump its contents into the SST file sink. The `ExportResponse` received
could either be a full response (all key-value pairs within the request span
have been returned), or a partial response (some of the key-value pairs have
been returned, and a resume key of the next missing key is included in the
response, similar to pagination). In the case of receiving a partial response,
then another `ExportRequest` is made using the resume key until the entire span
has been fetched. Once an entire span has been processed, the next span assigned
to that worker is processed.

When certain conditions are met (e.g. the SST file has reached the maximum size
of 128 MiB), the sink is flushed and a new SST file is opened. When the SST file
sink is flushed, that resulting file is sent to the backup cloud provider bucket
destination and a
[`BackupManifest_File`](https://github.com/cockroachdb/cockroach/blob/c7ef50846c992a99d5cd60bff8373c6df1c195a5/pkg/ccl/backupccl/backuppb/backup.proto#L38C17-L60)
is generated, which contains metadata about the file that was just written,
namely the file name as well as the key span it covers.

> Note that `BackupManifest_File` is a bit of a misnomer. Not only is it not a
backup manifest, it also is not a file.

These `BackupManifest_File`s are then sent back to the coordinator node, which
aggregates the `BackupManifest_File`s from each of the nodes into one official
backup manifest. Every 30 seconds, it writes the backup manifest, which will
contain a mapping of key spans to SSTs, sorted by the start key of the span.

## Flow Diagram

Here is a simplified diagram to demonstrate the overall flow of the backup
process:

![Backup Flow Diagram](../images/backup-flow.png)
