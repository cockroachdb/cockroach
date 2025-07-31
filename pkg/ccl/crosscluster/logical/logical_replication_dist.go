// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/physical"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
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
	streamAddress crosscluster.StreamAddress,
	topology streamclient.Topology,
	destSQLInstances []sql.InstanceLocality,
	initialScanTimestamp hlc.Timestamp,
	previousReplicatedTimestamp hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	tableMetadataByDestID map[int32]execinfrapb.TableReplicationMetadata,
	srcTypes []*descpb.TypeDescriptor,
	jobID jobspb.JobID,
	streamID streampb.StreamID,
	discard jobspb.LogicalReplicationDetails_Discard,
	mode jobspb.LogicalReplicationDetails_ApplyMode,
	metricsLabel string,
) (map[base.SQLInstanceID][]execinfrapb.LogicalReplicationWriterSpec, error) {
	spanGroup := roachpb.SpanGroup{}
	baseSpec := execinfrapb.LogicalReplicationWriterSpec{
		StreamID:                    uint64(streamID),
		JobID:                       int64(jobID),
		PreviousReplicatedTimestamp: previousReplicatedTimestamp,
		InitialScanTimestamp:        initialScanTimestamp,
		Checkpoint:                  checkpoint, // TODO: Only forward relevant checkpoint info
		StreamAddress:               string(streamAddress),
		TableMetadataByDestID:       tableMetadataByDestID,
		Discard:                     discard,
		Mode:                        mode,
		MetricsLabel:                metricsLabel,
		TypeDescriptors:             srcTypes,
	}

	writerSpecs := make(map[base.SQLInstanceID][]execinfrapb.LogicalReplicationWriterSpec, len(destSQLInstances))

	// Update stream ingestion specs with their matched source node.
	matcher := physical.MakeNodeMatcher(destSQLInstances)
	for _, candidate := range matcher.FindSourceNodePriority(topology) {
		destID := matcher.FindMatch(candidate.ClosestDestIDs)
		partition := candidate.Partition

		log.VInfof(ctx, 2, "logical replication src-dst pair candidate: %s (locality %s) - %d ("+
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
