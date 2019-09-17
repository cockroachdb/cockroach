// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

var showTelemetryCounters map[string]telemetry.Counter

func init() {
	showTelemetryCounters = make(map[string]telemetry.Counter)
}

func IncrementShowCounter(showType string) {
	if counter, ok := showTelemetryCounters[showType]; ok {
		telemetry.Inc(counter)
	} else {
		showTelemetryCounters[showType] = telemetry.GetCounterOnce(fmt.Sprintf("sql.show.%s", showType))
		telemetry.Inc(showTelemetryCounters[showType])
	}
}
