// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package spanconfiglimiter is used to limit how many span configs are
// installed by tenants.
package spanconfiglimiter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

var _ spanconfig.Limiter = &Limiter{}

// tenantLimitSetting controls how many span configs a secondary tenant is
// allowed to install. It's settable only by the system tenant.
var tenantLimitSetting = settings.RegisterIntSetting(
	settings.SystemVisible,
	"spanconfig.tenant_limit",
	"limit on the number of span configs that can be set up by a virtual cluster",
	5000,
	settings.WithName("spanconfig.virtual_cluster.max_spans"),
)

// Limiter is used to limit the number of span configs installed by secondary
// tenants. It's a concrete implementation of the spanconfig.Limiter interface.
type Limiter struct {
	ie       isql.Executor
	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs
}

// New constructs and returns a Limiter.
func New(ie isql.Executor, settings *cluster.Settings, knobs *spanconfig.TestingKnobs) *Limiter {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Limiter{
		ie:       ie,
		settings: settings,
		knobs:    knobs,
	}
}

// ShouldLimit is part of the spanconfig.Limiter interface.
func (l *Limiter) ShouldLimit(ctx context.Context, txn *kv.Txn, delta int) (bool, error) {
	if delta == 0 {
		return false, nil
	}

	limit := tenantLimitSetting.Get(&l.settings.SV)
	if overrideFn := l.knobs.LimiterLimitOverride; overrideFn != nil {
		limit = overrideFn()
	}

	const updateSpanCountStmt = `
INSERT INTO system.span_count (span_count) VALUES ($1)
ON CONFLICT (singleton)
DO UPDATE SET span_count = system.span_count.span_count + $1
RETURNING span_count
`
	datums, err := l.ie.QueryRowEx(ctx, "update-span-count", txn,
		sessiondata.NodeUserSessionDataOverride,
		updateSpanCountStmt, delta)
	if err != nil {
		return false, err
	}
	if len(datums) != 1 {
		return false, errors.AssertionFailedf("expected to return 1 row, return %d", len(datums))
	}

	if delta < 0 {
		return false, nil // always allowed to lower span count
	}
	spanCountWithDelta := int64(tree.MustBeDInt(datums[0]))
	return spanCountWithDelta > limit, nil
}
