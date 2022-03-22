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

// StreamReplicationStreamLivenessTrackFrequency controls frequency to check
// the liveness of a streaming replication producer job.
var StreamReplicationStreamLivenessTrackFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"stream_replication.stream_liveness_track_frequency",
	"controls how frequent we check for the liveness of a replication stream producer job",
	time.Minute,
)

// StreamReplicationJobLivenessTimeout controls how long we wait for to kill
// an inactive producer job.
var StreamReplicationJobLivenessTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"stream_replication.job_liveness_timeout",
	"controls how long we wait for to kill an inactive producer job",
	time.Minute,
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
)

// StreamReplicationMinCheckpointFrequency controls the minimum frequency the stream replication
// source cluster sends checkpoints to destination cluster.
var StreamReplicationMinCheckpointFrequency = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"stream_replication.min_checkpoint_frequency",
	"controls minimum frequency the stream replication source cluster sends checkpoints "+
		"to the destination cluster.",
	10*time.Second,
	settings.NonNegativeDuration,
)
