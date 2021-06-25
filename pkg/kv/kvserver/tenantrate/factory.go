// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TestingKnobs configures a LimiterFactory for testing.
type TestingKnobs struct {
	TimeSource timeutil.TimeSource
}

// LimiterFactory constructs and manages per-tenant Limiters.
type LimiterFactory struct {
	knobs         TestingKnobs
	metrics       Metrics
	systemLimiter systemLimiter
	mu            struct {
		syncutil.RWMutex
		config  Config
		tenants map[roachpb.TenantID]*refCountedLimiter
	}
}

// refCountedLimiter maintains a refCount for a limiter.
type refCountedLimiter struct {
	refCount int
	lim      limiter
}

// NewLimiterFactory constructs a new LimiterFactory.
func NewLimiterFactory(sv *settings.Values, knobs *TestingKnobs) *LimiterFactory {
	rl := &LimiterFactory{
		metrics: makeMetrics(),
	}
	if knobs != nil {
		rl.knobs = *knobs
	}
	rl.mu.tenants = make(map[roachpb.TenantID]*refCountedLimiter)
	rl.mu.config = ConfigFromSettings(sv)
	rl.systemLimiter = systemLimiter{
		tenantMetrics: rl.metrics.tenantMetrics(roachpb.SystemTenantID),
	}
	updateFn := func(_ context.Context) {
		config := ConfigFromSettings(sv)
		rl.UpdateConfig(config)
	}
	for _, setting := range configSettings {
		setting.SetOnChange(sv, updateFn)
	}
	tenantcostmodel.SetOnChange(sv, updateFn)
	return rl
}

// GetTenant gets or creates a limiter for the given tenant. The limiters are
// reference counted; call Destroy on the returned limiter when it is no longer
// in use. If the closer channel is non-nil, closing it will lead to any blocked
// requests becoming unblocked.
func (rl *LimiterFactory) GetTenant(tenantID roachpb.TenantID, closer <-chan struct{}) Limiter {

	if tenantID == roachpb.SystemTenantID {
		return &rl.systemLimiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	rcLim, ok := rl.mu.tenants[tenantID]
	if !ok {
		var options []quotapool.Option
		if rl.knobs.TimeSource != nil {
			options = append(options, quotapool.WithTimeSource(rl.knobs.TimeSource))
		}
		if closer != nil {
			options = append(options, quotapool.WithCloser(closer))
		}
		rcLim = new(refCountedLimiter)
		rcLim.lim.init(rl, tenantID, rl.mu.config, rl.metrics.tenantMetrics(tenantID), options...)
		rl.mu.tenants[tenantID] = rcLim
	}
	rcLim.refCount++
	return &rcLim.lim
}

// Release releases a limiter associated with a tenant.
func (rl *LimiterFactory) Release(lim Limiter) {
	if _, isSystem := lim.(*systemLimiter); isSystem {
		return
	}
	l := lim.(*limiter)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rcLim, ok := rl.mu.tenants[l.tenantID]
	if !ok {
		panic(errors.AssertionFailedf("expected to find entry for tenant %v", l.tenantID))
	}
	if &rcLim.lim != lim {
		panic(errors.AssertionFailedf("two limiters exist for tenant %v", l.tenantID))
	}
	if rcLim.refCount--; rcLim.refCount == 0 {
		l.metrics.destroy()
		delete(rl.mu.tenants, l.tenantID)
	}
}

// UpdateConfig changes the config of all limiters (existing and future).
// It is called automatically when a cluster setting is changed. It is also
// called by tests.
func (rl *LimiterFactory) UpdateConfig(config Config) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.mu.config = config
	for _, rcLim := range rl.mu.tenants {
		rcLim.lim.updateConfig(rl.mu.config)
	}
}

// Metrics returns the LimiterFactory's metric.Struct.
func (rl *LimiterFactory) Metrics() *Metrics {
	return &rl.metrics
}
