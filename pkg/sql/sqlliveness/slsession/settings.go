// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slsession

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/pkg/errors"
)

var (
	// DefaultTTL specifies the time to expiration when a session is created.
	DefaultTTL = settings.RegisterDurationSetting(
		"server.sqlliveness.ttl",
		"default sqlliveness session ttl",
		120*time.Second,
		settings.NonNegativeDuration,
	)
	// DefaultHeartBeat specifies the period between attempts to extend a session.
	DefaultHeartBeat = settings.RegisterDurationSetting(
		"server.sqlliveness.heartbeat",
		"duration heart beats to push session expiration further out in time",
		5*time.Second,
		settings.NonNegativeDuration,
	)

	// GCInterval specifies duration between attempts to delete extant
	// sessions that have expired.
	GCInterval = settings.RegisterDurationSetting(
		"server.sqlliveness.gc_interval",
		"duration between attempts to delete extant sessions that have expired",
		20*time.Second,
		settings.NonNegativeDuration,
	)

	// GCJitter specifies the jitter fraction on the interval between attempts to
	// delete extant sessions that have expired.
	//
	// [(1-GCJitter) * GCInterval, (1+GCJitter) * GCInterval]
	GCJitter = settings.RegisterFloatSetting(
		"server.sqlliveness.gc_jitter",
		"jitter fraction on the duration between attempts to delete extant sessions that have expired",
		.15,
		func(f float64) error {
			if f < 0 || f > 1 {
				return errors.Errorf("%f is not in [0, 1]", f)
			}
			return nil
		},
	)
)
