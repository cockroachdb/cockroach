// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package concurrency provides a concurrency manager structure that
// encapsulates the details of concurrency control and contention handling for
// serializable key-value transactions.

package spanlatch

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

var longLatchHoldDuration = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.concurrency.long_latch_hold_duration",
	"duration threshold for logging long latch holding",
	3*time.Second,
	settings.NonNegativeDuration,
)
