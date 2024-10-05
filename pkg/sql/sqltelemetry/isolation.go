// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// IsolationLevelCounter is to be incremented whenever a transaction is started
// or if the isolation level of a running transaction is configured. It tracks
// the isolation level that CRDB uses after checking which levels are supported
// and enabled by the cluster, which may differ from the one the user requested.
func IsolationLevelCounter(ctx context.Context, level isolation.Level) telemetry.Counter {
	const prefix = "sql.txn.isolation.executed_at."
	var key string
	switch level {
	case isolation.Serializable:
		key = prefix + "serializable"
	case isolation.Snapshot:
		key = prefix + "snapshot"
	case isolation.ReadCommitted:
		key = prefix + "read_committed"
	default:
		log.Warningf(ctx, "unexpected isolation level: %v", level)
	}
	return telemetry.GetCounter(key)
}

// IsolationLevelUpgradedCounter is to be incremented whenever a transaction is
// started with a higher isolation level than the one that the user requested
// for the transaction. It tracks the isolation level that the user requested.
func IsolationLevelUpgradedCounter(
	ctx context.Context, level tree.IsolationLevel,
) telemetry.Counter {
	const prefix = "sql.txn.isolation.upgraded_from."
	var key string
	switch level {
	case tree.SerializableIsolation:
		key = prefix + "serializable"
	case tree.SnapshotIsolation:
		key = prefix + "snapshot"
	case tree.RepeatableReadIsolation:
		key = prefix + "repeatable_read"
	case tree.ReadCommittedIsolation:
		key = prefix + "read_committed"
	case tree.ReadUncommittedIsolation:
		key = prefix + "read_uncommitted"
	default:
		log.Warningf(ctx, "unexpected isolation level: %v", level)
	}
	return telemetry.GetCounter(key)
}
