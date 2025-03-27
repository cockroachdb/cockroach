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

// RangeClosedTimestampPolicyRefreshInterval controls how frequently the system
// refreshes policies on leaseholders of nodes. These policies are refreshed
// regardless of whether auto-tuning is enabled.
var RangeClosedTimestampPolicyRefreshInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.policy_refresh_interval",
	"interval at which the system refreshes closed timestamp policies for leaseholders",
	5*time.Minute,
	settings.NonNegativeDuration,
)

// LeadForGlobalReadsAutoTuneEnabled determines whether ranges configured to
// serve global reads would be assigned policies based on observed latency
// between their leaseholder and furthest follower. This dynamic adjustment
// helps optimize closed timestamp lead times. If no latency data is available,
// falls back to default lead times. Note that the LeadForGlobalReadsOverride
// setting takes precedence if set.
var LeadForGlobalReadsAutoTuneEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.lead_for_global_reads_auto_tune.enabled",
	"if enabled, observed network latency between leaseholders and their "+
		"furthest follower will be used to adjust closed timestamp policies for ranges"+
		"ranges configured to serve global reads. "+
		"kv.closed_timestamp.lead_for_global_reads_override takes precedence if set.",
	false,
	settings.WithPublic,
)
