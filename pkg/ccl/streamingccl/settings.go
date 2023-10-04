// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// CrossClusterReplicationEnabled enables the ability to setup and control a
// cross cluster replication stream.
var CrossClusterReplicationEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cross_cluster_replication.enabled",
	"enables the ability to setup and control a cross cluster replication stream",
	false,
	settings.WithName("physical_replication.enabled"),
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

// StreamReplicationJobLivenessTimeout controls how long we wait for to kill
// an inactive producer job.
var StreamReplicationJobLivenessTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"stream_replication.job_liveness_timeout",
	"controls how long we wait for to kill an inactive producer job",
	3*24*time.Hour,
	settings.WithRetiredName("stream_replication.job_liveness.timeout"),
	settings.WithName("physical_replication.producer.job_liveness.timeout"),
)

// StreamReplicationMinCheckpointFrequency controls the minimum frequency the stream replication
// source cluster sends checkpoints to destination cluster.
var StreamReplicationMinCheckpointFrequency = settings.RegisterDurationSetting(
	settings.SystemOnly,
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
	settings.ApplicationLevel,
	"stream_replication.job_checkpoint_frequency",
	"controls the frequency with which partitions update their progress; if 0, disabled",
	10*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("physical_replication.consumer.job_checkpoint_frequency"),
)

var ReplanThreshold = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"stream_replication.replan_flow_threshold",
	"fraction of nodes in the producer or consumer job that would need to change to refresh the"+
		" physical execution plan. If set to 0, the physical plan will not automatically refresh.",
	0,
	settings.NonNegativeFloatWithMaximum(1),
	settings.WithName("physical_replication.consumer.replan_flow_threshold"),
)

var ReplanFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"stream_replication.replan_flow_frequency",
	"frequency at which the consumer job checks to refresh its physical execution plan",
	10*time.Minute,
	settings.PositiveDuration,
	settings.WithName("physical_replication.consumer.replan_flow_frequency"),
)

// DumpFrontierEntries controls the frequency at which we persist the entries in
// the frontier to the `system.job_info` table.
//
// TODO(adityamaru): This timer should be removed once each job is aware of whether
// it is profiling or not.
var DumpFrontierEntries = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"physical_replication.consumer.dump_frontier_entries_frequency",
	"controls the frequency with which the frontier entries are persisted; if 0, disabled",
	10*time.Minute,
	settings.NonNegativeDuration,
)
