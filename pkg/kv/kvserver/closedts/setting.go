// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// NB: These settings are SystemVisible because they need to be read by e.g.
// rangefeed clients and follower_read_timestamp(). However, they really need
// to see the host's setting, not the tenant's setting. See:
// https://github.com/cockroachdb/cockroach/issues/108677

// TargetDuration is the follower reads closed timestamp update target duration.
var TargetDuration = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.target_duration",
	"if nonzero, attempt to provide closed timestamp notifications for timestamps trailing cluster time by approximately this duration",
	3*time.Second,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

// SideTransportCloseInterval determines the ClosedTimestampSender's frequency.
var SideTransportCloseInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.side_transport_interval",
	"the interval at which the closed timestamp side-transport attempts to "+
		"advance each range's closed timestamp; set to 0 to disable the side-transport",
	200*time.Millisecond,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

// LeadForGlobalReadsOverride overrides the lead time that ranges with the
// LEAD_FOR_GLOBAL_READS closed timestamp policy use to publish close timestamps
// (see TargetForPolicy), if it is set to a non-zero value. Meant as an escape
// hatch.
var LeadForGlobalReadsOverride = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.lead_for_global_reads_override",
	"if nonzero, overrides the lead time that global_read ranges use to publish closed timestamps",
	0,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

// LeadForGlobalReadsAutoTuneInterval auto-tunes the lead time that ranges with
// the LEAD_FOR_GLOBAL_READS closed timestamp policy use to publish close
// timestamps based on observed network latency. Falls back to the hardcoded
// lead time calculation if no data is observed. LeadForGlobalReadsOverride
// takes precedence over this setting.
var LeadForGlobalReadsAutoTuneInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.lead_for_global_reads_auto_tune_interval",
	"interval at which the closed timestamp lead time is automatically adjusted based on observed network latency "+
		"between nodes. This setting controls how frequently the system measures latency and updates the lead time "+
		"for ranges using the LEAD_FOR_GLOBAL_READS policy. The auto-tuning only takes effect when side_transport_interval "+
		"is non-zero. kv.closed_timestamp.lead_for_global_reads_override takes precedence over this setting."+
		"Set to 0 to disable auto-tuning.",
	5*time.Minute,
	settings.NonNegativeDuration,
	settings.WithPublic,
)
