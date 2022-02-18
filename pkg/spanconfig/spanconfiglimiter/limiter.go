// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package spanconfiglimiter is used to limit how many span configs are
// installed by tenants.
package spanconfiglimiter

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var _ spanconfig.Limiter = &Limiter{}

// tenantLimitSetting controls how many span configs a secondary tenant is
// allowed to install. It's settable only by the system tenant.
var tenantLimitSetting = settings.RegisterIntSetting(
	settings.TenantReadOnly,
	"spanconfig.tenant_limit",
	"limit on the number of span configs a tenant is allowed to install",
	5000,
)

// Limiter is used to limit the number of span configs installed by secondary
// tenants. It's a concrete implementation of the spanconfig.Limiter interface.
type Limiter struct {
	ie       sqlutil.InternalExecutor
	settings *cluster.Settings
	splitter spanconfig.Splitter
	knobs    *spanconfig.TestingKnobs
}

// New constructs and returns a Limiter.
func New(
	ie sqlutil.InternalExecutor,
	settings *cluster.Settings,
	splitter spanconfig.Splitter,
	knobs *spanconfig.TestingKnobs,
) *Limiter {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Limiter{
		ie:       ie,
		settings: settings,
		splitter: splitter,
		knobs:    knobs,
	}
}

// ShouldLimit is part of the spanconfig.Limiter interface.
func (l *Limiter) ShouldLimit(
	ctx context.Context, txn *kv.Txn, committed, uncommitted catalog.TableDescriptor,
) (bool, error) {
	if !l.settings.Version.IsActive(ctx, clusterversion.PreSeedSpanCountTable) {
		return false, nil // nothing to do
	}

	if isNil(committed) && isNil(uncommitted) {
		log.Fatalf(ctx, "unexpected: got two empty table descriptors")
	}

	var nonNilDesc catalog.TableDescriptor
	if !isNil(committed) {
		nonNilDesc = committed
	} else {
		nonNilDesc = uncommitted
	}
	if nonNilDesc.GetParentID() == systemschema.SystemDB.GetID() {
		return false, nil // we don't count tables in system database
	}

	limit := tenantLimitSetting.Get(&l.settings.SV)
	if overrideFn := l.knobs.LimiterLimitOverride; overrideFn != nil {
		limit = overrideFn()
	}

	committedSplits, err := l.splitter.Splits(ctx, committed)
	if err != nil {
		return false, err
	}
	uncommittedSplits, err := l.splitter.Splits(ctx, uncommitted)
	if err != nil {
		return false, err
	}

	delta := uncommittedSplits - committedSplits
	if delta == 0 {
		return false, nil
	}

	log.VInfof(ctx, 1, "adding %d to span count for %s", delta, nonNilDesc.GetName())

	const updateSpanCountStmt = `
INSERT INTO system.span_count (span_count) VALUES ($1)
ON CONFLICT (singleton)
DO UPDATE SET span_count = system.span_count.span_count + $1
RETURNING span_count
`
	datums, err := l.ie.QueryRowEx(ctx, "update-span-count", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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

func isNil(table catalog.TableDescriptor) bool {
	vTable := reflect.ValueOf(table)
	return vTable.Kind() == reflect.Ptr && vTable.IsNil() || table == nil
}
