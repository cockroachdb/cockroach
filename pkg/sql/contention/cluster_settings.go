// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package contention

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// TxnIDResolutionInterval is the cluster setting that controls how often the
// Transaction ID Resolution is performed.
var TxnIDResolutionInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.contention.event_store.resolution_interval",
	"the interval at which transaction fingerprint ID resolution is "+
		"performed (set to 0 to disable)",
	time.Second*30,
)

// StoreCapacity is the cluster setting that controls the
// maximum size of the contention event store.
var StoreCapacity = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"sql.contention.event_store.capacity",
	"the in-memory storage capacity per-node of contention event store",
	64*1024*1024, // 64 MB per node.
	settings.WithPublic)

// DurationThreshold is the cluster setting for the threshold of LOCK_WAIT
// contention event durations. Only the contention events whose duration exceeds the
// threshold will be collected into crdb_internal.transaction_contention_events.
var DurationThreshold = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.contention.event_store.duration_threshold",
	"minimum contention duration to cause the contention events to be collected "+
		"into crdb_internal.transaction_contention_events",
	0,
	settings.WithPublic)

// EnableSerializationConflictEvents is the cluster setting to enable recording
// SERIALIZATION_CONFLICT contention events to the event store.
var EnableSerializationConflictEvents = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.contention.record_serialization_conflicts.enabled",
	"enables recording 40001 errors with conflicting txn meta as SERIALIZATION_CONFLICT"+
		"contention events into crdb_internal.transaction_contention_events",
	true,
	settings.WithPublic)
