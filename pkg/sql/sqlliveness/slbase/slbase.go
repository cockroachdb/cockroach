// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slbase

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

var (
	// DefaultTTL specifies the time to expiration when a session is created.
	DefaultTTL = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.sqlliveness.ttl",
		"default sqlliveness session ttl",
		40*time.Second,
	)
	// DefaultHeartBeat specifies the period between attempts to extend a session.
	DefaultHeartBeat = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.sqlliveness.heartbeat",
		"duration heart beats to push session expiration further out in time",
		5*time.Second,
	)
	// HeartbeatJitter specifies the jitter applied to heartbeat intervals.
	// This prevents thundering herd effects on the system.sqlliveness table.
	HeartbeatJitter = settings.RegisterFloatSetting(
		settings.ApplicationLevel,
		"server.sqlliveness.heartbeat_jitter",
		"jitter fraction for sqlliveness heartbeat interval (0.0 to 0.95)",
		0.1, //defaultValue
		settings.FloatInRange(0.0, 0.95),
	)
)
