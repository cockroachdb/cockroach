// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package crosscluster

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// StreamReplicationStreamLivenessTrackFrequency controls frequency to check
// the liveness of a streaming replication producer job.
var StreamReplicationStreamLivenessTrackFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"stream_replication.stream_liveness_track_frequency",
	"controls how frequent we check for the liveness of a replication stream producer job",
	time.Minute,
	settings.WithName("physical_replication.producer.stream_liveness_track_frequency"),
)

// StreamReplicationMinCheckpointFrequency controls the minimum frequency the stream replication
// source cluster sends checkpoints to destination cluster.
var StreamReplicationMinCheckpointFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"stream_replication.min_checkpoint_frequency",
	"controls minimum frequency the stream replication source cluster sends checkpoints "+
		"to the destination cluster",
	10*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("physical_replication.producer.min_checkpoint_frequency"),
)

// StreamReplicationConsumerHeartbeatFrequency controls frequency the stream replication
// destination cluster sends heartbeat to the source cluster to keep the stream alive.
var StreamReplicationConsumerHeartbeatFrequency = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"stream_replication.consumer_heartbeat_frequency",
	"controls frequency the stream replication destination cluster sends heartbeat "+
		"to the source cluster to keep the stream alive",
	30*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("physical_replication.consumer.heartbeat_frequency"),
)

// JobCheckpointFrequency controls the frequency of frontier checkpoints into
// the jobs table.
var JobCheckpointFrequency = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"stream_replication.job_checkpoint_frequency",
	"controls the frequency with which partitions update their progress; if 0, disabled",
	10*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("physical_replication.consumer.job_checkpoint_frequency"),
)

var ReplanThreshold = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"stream_replication.replan_flow_threshold",
	"fraction of nodes in the producer or consumer job that would need to change to refresh the"+
		" physical execution plan. If set to 0, the physical plan will not automatically refresh.",
	0.1,
	settings.NonNegativeFloatWithMaximum(1),
	settings.WithName("physical_replication.consumer.replan_flow_threshold"),
)

var ReplanFrequency = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"stream_replication.replan_flow_frequency",
	"frequency at which the consumer job checks to refresh its physical execution plan",
	10*time.Minute,
	settings.PositiveDuration,
	settings.WithName("physical_replication.consumer.replan_flow_frequency"),
)

var LagCheckFrequency = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"stream_replication.lag_check_frequency",
	"frequency at which the consumer job checks for lagging nodes",
	3*time.Minute,
	settings.PositiveDuration,
)

var InterNodeLag = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"physical_replication.consumer.node_lag_replanning_threshold",
	"the maximum difference in lag tolerated across two destination nodes; if 0, disabled",
	5*time.Minute,
	settings.NonNegativeDuration,
)

// DumpFrontierEntries controls the frequency at which we persist the entries in
// the frontier to the `system.job_info` table.
//
// TODO(adityamaru): This timer should be removed once each job is aware of whether
// it is profiling or not.
var DumpFrontierEntries = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"physical_replication.consumer.dump_frontier_entries_frequency",
	"controls the frequency with which the frontier entries are persisted; if 0, disabled",
	0,
	settings.NonNegativeDuration,
)

// ReplicateSpanConfigsEnabled controls whether we replicate span
// configurations from the source system tenant to the destination system
// tenant.
var ReplicateSpanConfigsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"physical_replication.consumer.span_configs.enabled",
	"controls whether we replicate span configurations from the source system tenant to the "+
		"destination system tenant",
	true,
)

var LogicalReplanThreshold = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"logical_replication.replan_flow_threshold",
	"fraction of nodes in the producer or consumer job that would need to change to refresh the"+
		" physical execution plan. If set to 0, the physical plan will not automatically refresh.",
	0.1,
	settings.NonNegativeFloatWithMaximum(1),
)

var LogicalReplanFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"logical_replication.replan_flow_frequency",
	"frequency at which the consumer job checks to refresh its physical execution plan",
	10*time.Minute,
	settings.PositiveDuration,
)
