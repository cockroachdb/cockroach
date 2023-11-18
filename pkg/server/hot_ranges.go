// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// HotRangesRequestNodeTimeout controls the timeout of a serverpb.HotRangesRequest.
// A default value of 5 minutes is meant to be an escape hatch for a node that is taking
// too long to fulfill the request. The setting should be lowered for a faster response,
// at the expense of possibly incomplete data, or raised for complete data, at the cost
// of a possibly slower response.
var HotRangesRequestNodeTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.hot_ranges_request.node.timeout",
	"the duration allowed for a single node to return hot range data before the request is cancelled; if set to 0, there is no timeout",
	time.Minute*5,
	settings.NonNegativeDuration,
	settings.WithPublic)
