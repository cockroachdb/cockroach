// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package closedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// NB: These settings are TenantReadOnly because they need to be read by e.g.
// rangefeed clients and follower_read_timestamp(). However, they really need
// to see the host's setting, not the tenant's setting. See:
// https://github.com/cockroachdb/cockroach/issues/108677

// TargetDuration is the follower reads closed timestamp update target duration.
var TargetDuration = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"kv.closed_timestamp.target_duration",
	"if nonzero, attempt to provide closed timestamp notifications for timestamps trailing cluster time by approximately this duration",
	3*time.Second,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

// SideTransportCloseInterval determines the ClosedTimestampSender's frequency.
var SideTransportCloseInterval = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
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
	settings.TenantReadOnly,
	"kv.closed_timestamp.lead_for_global_reads_override",
	"if nonzero, overrides the lead time that global_read ranges use to publish closed timestamps",
	0,
	settings.NonNegativeDuration,
	settings.WithPublic,
)
