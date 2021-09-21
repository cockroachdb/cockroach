// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

func newBreaker(
	ctx context.Context, name string, asyncProbe func(report func(error), done func()),
) *circuit.Breaker {
	return circuit.NewBreaker(circuit.Options{
		Name: name,
		ShouldTrip: func(err error) error {
			return err
		},
		AsyncProbe: asyncProbe,
		EventHandler: circuit.EventLogBridge{
			Logf: func(format redact.SafeString, args ...interface{}) {
				// To see the circuitbreaker debug messages set --vmodule=breaker=2.
				if log.V(2) {
					log.Ops.InfofDepth(ctx, 1, string(format), args...)
				}
			},
		},
	})
}
