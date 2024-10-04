// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// TargetDuration is the follower reads closed timestamp update target duration.
var TargetDuration = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.closed_timestamp.target_duration",
	"if nonzero, attempt to provide closed timestamp notifications for timestamps trailing cluster time by approximately this duration",
	3*time.Second,
	settings.NonNegativeDuration,
)

// SideTransportCloseInterval determines the ClosedTimestampSender's frequency.
var SideTransportCloseInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.closed_timestamp.side_transport_interval",
	"the interval at which the closed-timestamp side-transport attempts to "+
		"advance each range's closed timestamp; set to 0 to disable the side-transport",
	200*time.Millisecond,
	settings.NonNegativeDuration,
)

// LeadForGlobalReadsOverride overrides the lead time that ranges with the
// LEAD_FOR_GLOBAL_READS closed timestamp policy use to publish close timestamps
// (see TargetForPolicy), if it is set to a non-zero value. Meant as an escape
// hatch.
var LeadForGlobalReadsOverride = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.closed_timestamp.lead_for_global_reads_override",
	"if nonzero, overrides the lead time that global_read ranges use to publish closed timestamps",
	0,
	settings.NonNegativeDuration,
)
