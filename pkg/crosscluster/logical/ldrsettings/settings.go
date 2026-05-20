// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrsettings

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

var (
	// JobCheckpointFrequency controls the frequency of frontier
	// checkpoints into the jobs table.
	JobCheckpointFrequency = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"logical_replication.consumer.job_checkpoint_frequency",
		"controls the frequency with which the job updates their progress; if 0, disabled",
		10*time.Second)

	// HeartbeatFrequency controls frequency the stream replication
	// destination cluster sends heartbeat to the source cluster to keep
	// the stream alive.
	HeartbeatFrequency = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"logical_replication.consumer.heartbeat_frequency",
		"controls frequency the stream replication destination cluster sends heartbeat "+
			"to the source cluster to keep the stream alive",
		30*time.Second,
	)
)
