package protectedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// MaxSpans controls the maximum number of spans which can be protected
// by all protected timestamp records.
//
// Records and their spans are stored in memory on every host so it's best
// not to let this data size be unbounded.
var MaxSpans = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_spans",
	"if non-zero the limit of the number of spans which can be protected",
	4096)

// MaxRecords controls the maximum number of timestamp records which can
// be created.
//
// Records and their spans are stored in memory on every host so it's best
// not to let this data size be unbounded.
var MaxRecords = settings.RegisterNonNegativeIntSetting(
	"kv.protectedts.max_records",
	"if non-zero the limit of the number of timestamps which can be protected",
	4096)

// PollInterval defines how frequently the protectedts state is polled by the
// Tracker.
var PollInterval = settings.RegisterNonNegativeDurationSetting(
	"kv.protectedts.poll_interval",
	// TODO(ajwerner): better description.
	"the interval at which the protectedts subsystem state is polled",
	2*time.Minute)
