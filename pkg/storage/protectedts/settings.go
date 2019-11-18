// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protectedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// PollInterval defines how frequently the protectedts state is polled by the
// Tracker.
var PollInterval = settings.RegisterNonNegativeDurationSetting(
	"kv.protectedts.poll_interval",
	"the interval at which the protectedts subsystem state is polled",
	2*time.Minute)

// Records and their spans are stored in memory on every host so it's best
// not to let this data size be unbounded.

// MaxRecords controls the maximum number of timestamp records which can
// be created.
var MaxRecords = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_records",
	"the limit on the number of timestamps which can be protected",
	4096)

// MaxBytes controls the maximum number of bytes worth of spans and metadata
// which can be protected by all protected timestamp records.
var MaxBytes = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_bytes",
	"the limit on the number of bytes of spans and metadata which can be protected",
	1<<20, // 8 MiB
)

// MaxSpans controls the maximum number of spans which can be protected
// by all protected timestamp records.
var MaxSpans = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_spans",
	"the limit on the number of spans which can be protected",
	4096)

func init() {
	MaxBytes.Hidden()
	MaxRecords.Hidden()
	MaxSpans.Hidden()
}
