// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package protectedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Records and their spans are stored in memory on every host so it's best
// not to let this data size be unbounded.

// MaxBytes controls the maximum number of bytes worth of spans and metadata
// which can be protected by all protected timestamp records.
var MaxBytes = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.protectedts.max_bytes",
	"if non-zero, this limits the number of bytes used by protected timestamp records in the protected timestamps"+
		" system table. this will be a noop in 24.1 onwards and deprecated in the future",
	1<<20, // 1 MiB
	settings.NonNegativeInt,
	settings.WithVisibility(settings.Reserved),
)

// MaxSpans controls the maximum number of spans which can be protected
// by all protected timestamp records.
var MaxSpans = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"kv.protectedts.max_spans",
	"if non-zero the limit of the number of spans which can be protected",
	32768,
	settings.NonNegativeInt,
	settings.WithVisibility(settings.Reserved),
)

// PollInterval defines how frequently the protectedts state is polled by the
// Tracker.
var PollInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"kv.protectedts.poll_interval",
	// TODO(ajwerner): better description.
	"the interval at which the protectedts subsystem state is polled",
	2*time.Minute, settings.NonNegativeDuration)
