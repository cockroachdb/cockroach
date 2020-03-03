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

// Records and their spans are stored in memory on every host so it's best
// not to let this data size be unbounded.

// MaxBytes controls the maximum number of bytes worth of spans and metadata
// which can be protected by all protected timestamp records.
var MaxBytes = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_bytes",
	"if non-zero the limit of the number of bytes of spans and metadata which can be protected",
	1<<20, // 1 MiB
)

// MaxSpans controls the maximum number of spans which can be protected
// by all protected timestamp records.
var MaxSpans = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_spans",
	"if non-zero the limit of the number of spans which can be protected",
	4096)

// PollInterval defines how frequently the protectedts state is polled by the
// Tracker.
var PollInterval = settings.RegisterNonNegativeDurationSetting(
	"kv.protectedts.poll_interval",
	// TODO(ajwerner): better description.
	"the interval at which the protectedts subsystem state is polled",
	2*time.Minute)

func init() {
	MaxBytes.SetVisibility(settings.Reserved)
	MaxSpans.SetVisibility(settings.Reserved)
}
