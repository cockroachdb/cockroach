// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

// OpTelemetryCounters stores telemetry counters for operators marked with the
// "Telemetry" tag. All other operators have nil values.
var OpTelemetryCounters [NumOperators]telemetry.Counter

func init() {
	for _, op := range TelemetryOperators {
		OpTelemetryCounters[op] = sqltelemetry.OptNodeCounter(op.String())
	}
}
