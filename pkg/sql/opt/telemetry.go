// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// OpTelemetryCounters stores telemetry counters for operators marked with the
// "Telemetry" tag. All other operators have nil values.
var OpTelemetryCounters [NumOperators]telemetry.Counter

func init() {
	for _, op := range TelemetryOperators {
		OpTelemetryCounters[op] = sqltelemetry.OptNodeCounter(op.String())
	}
}

// JoinTypeToUseCounter returns the JoinTypeXyzUseCounter for the given join
// operator.
func JoinTypeToUseCounter(op Operator) telemetry.Counter {
	switch op {
	case InnerJoinOp:
		return sqltelemetry.JoinTypeInnerUseCounter
	case LeftJoinOp, RightJoinOp:
		return sqltelemetry.JoinTypeLeftUseCounter
	case FullJoinOp:
		return sqltelemetry.JoinTypeFullUseCounter
	case SemiJoinOp:
		return sqltelemetry.JoinTypeSemiUseCounter
	case AntiJoinOp:
		return sqltelemetry.JoinTypeAntiUseCounter
	default:
		panic(errors.AssertionFailedf("unhandled join op %s", op))
	}
}
