// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
)

// StalledOpTimeoutSetting specifies how long a split/merge operation can remain
// in its current state before another fixup worker may attempt to assist. If
// this is set too high, then a fixup can get stuck for too long. If it is set
// too low, then multiple workers can assist at the same time, resulting in
// duplicate work.
// TODO(andyk): Consider making this more dynamic, e.g. with
// livenesspb.NodeVitalityInterface.
var StalledOpTimeoutSetting = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.vecindex.stalled_op.timeout",
	"amount of time before other vector index workers will assist with a stalled background fixup",
	cspann.DefaultStalledOpTimeout,
	settings.WithPublic,
)
