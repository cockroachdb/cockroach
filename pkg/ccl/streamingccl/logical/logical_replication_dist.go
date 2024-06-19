// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func constructLogicalReplicationWriterSpecs(
	ctx context.Context,
	streamAddress streamingccl.StreamAddress,
	topology streamclient.Topology,
	destSQLInstances []sql.InstanceLocality,
	initialScanTimestamp hlc.Timestamp,
	previousReplicatedTimestamp hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	tableDescs map[int32]descpb.TableDescriptor,
	jobID jobspb.JobID,
	streamID streampb.StreamID,
) (map[base.SQLInstanceID][]execinfrapb.LogicalReplicationWriterSpec, error) {
	spanGroup := roachpb.SpanGroup{}
	baseSpec := execinfrapb.LogicalReplicationWriterSpec{
		StreamID:                    uint64(streamID),
		JobID:                       int64(jobID),
		PreviousReplicatedTimestamp: previousReplicatedTimestamp,
		InitialScanTimestamp:        initialScanTimestamp,
		Checkpoint:                  checkpoint, // TODO: Only forward relevant checkpoint info
		StreamAddress:               string(streamAddress),
		TableDescriptors:            tableDescs,
	}

	writerSpecs := make(map[base.SQLInstanceID][]execinfrapb.LogicalReplicationWriterSpec, len(destSQLInstances))

	// Update stream ingestion specs with their matched source node.
	matcher := streamingest.MakeNodeMatcher(destSQLInstances)
	for _, candidate := range matcher.FindSourceNodePriority(topology) {
		destID := matcher.FindMatch(candidate.ClosestDestIDs)
		partition := candidate.Partition

		log.Infof(ctx, "logical replication src-dst pair candidate: %s (locality %s) - %d ("+
			"locality %s)",
			partition.ID,
			partition.SrcLocality,
			destID,
			matcher.DestNodeToLocality(destID),
		)
		spec := baseSpec
		spec.PartitionSpec = execinfrapb.StreamIngestionPartitionSpec{
			PartitionID:       partition.ID,
			SubscriptionToken: string(partition.SubscriptionToken),
			Address:           string(partition.SrcAddr),
			Spans:             partition.Spans,
			SrcInstanceID:     base.SQLInstanceID(partition.SrcInstanceID),
			DestInstanceID:    destID,
		}
		writerSpecs[destID] = append(writerSpecs[destID], spec)
		spanGroup.Add(partition.Spans...)
	}

	// TODO(ssd): Add assertion that the spanGroup covers all of the table spans we should be following.

	return writerSpecs, nil
}
