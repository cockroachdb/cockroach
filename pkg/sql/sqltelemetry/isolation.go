// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// IsolationLevelCounter is to be incremented whenever a transaction is started
// or if the isolation level of a running transaction is configured. It tracks
// the isolation level that CRDB uses after checking which levels are supported
// and enabled by the cluster, which may differ from the one the user requested.
func IsolationLevelCounter(level isolation.Level) telemetry.Counter {
	telemetryKey := strings.ToLower(level.String())
	if level == isolation.ReadCommitted {
		telemetryKey = "read_committed"
	}
	return telemetry.GetCounter(fmt.Sprintf("sql.txn.isolation.executed_at.%s", telemetryKey))
}

// IsolationLevelUpgradedCounter is to be incremented whenever a transaction is
// started with a higher isolation level than the one that the user requested
// for the transaction. It tracks the isolation level that the user requested.
func IsolationLevelUpgradedCounter(level tree.IsolationLevel) telemetry.Counter {
	telemetryKey := strings.ReplaceAll(strings.ToLower(level.String()), " ", "_")
	return telemetry.GetCounter(fmt.Sprintf("sql.txn.isolation.upgraded_from.%s", telemetryKey))
}
