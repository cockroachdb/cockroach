// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	tableDescs map[string]descpb.TableDescriptor,
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
		PartitionSpecs:              make(map[string]execinfrapb.StreamIngestionPartitionSpec),
	}

	writerSpecs := make(map[base.SQLInstanceID][]execinfrapb.LogicalReplicationWriterSpec, len(destSQLInstances))

	// Update stream ingestion specs with their matched source node.
	matcher := streamingest.MakeNodeMatcher(destSQLInstances)
	for _, candidate := range matcher.FindSourceNodePriority(topology) {
		destID := matcher.FindMatch(candidate.ClosestDestIDs)
		log.Infof(ctx, "logical replication src-dst pair candidate: %s (locality %s) - %d ("+
			"locality %s)",
			candidate.Partition.ID,
			candidate.Partition.SrcLocality,
			destID,
			matcher.DestNodeToLocality(destID),
		)
		partition := candidate.Partition

		spec := baseSpec
		spec.PartitionSpecs = map[string]execinfrapb.StreamIngestionPartitionSpec{
			partition.ID: {
				PartitionID:       partition.ID,
				SubscriptionToken: string(partition.SubscriptionToken),
				Address:           string(partition.SrcAddr),
				Spans:             partition.Spans,
				SrcInstanceID:     base.SQLInstanceID(partition.SrcInstanceID),
				DestInstanceID:    destID,
			},
		}
		writerSpecs[destID] = append(writerSpecs[destID], spec)
		spanGroup.Add(partition.Spans...)
	}

	// TODO(ssd): Add assertion that the spanGroup covers all of the table spans we should be following.

	return writerSpecs, nil
}
