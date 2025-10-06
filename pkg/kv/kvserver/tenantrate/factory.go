// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantrate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TestingKnobs configures a LimiterFactory for testing.
type TestingKnobs struct {
	QuotaPoolOptions []quotapool.Option

	// Authorizer, if set, replaces the authorizer in the RPCContext.
	Authorizer tenantcapabilities.Authorizer
}

// LimiterFactory constructs and manages per-tenant Limiters.
type LimiterFactory struct {
	knobs         TestingKnobs
	metrics       Metrics
	systemLimiter systemLimiter
	authorizer    tenantcapabilities.Authorizer

	mu struct {
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
func NewLimiterFactory(
	sv *settings.Values, knobs *TestingKnobs, authorizer tenantcapabilities.Authorizer,
) *LimiterFactory {
	rl := &LimiterFactory{
		metrics:    makeMetrics(),
		authorizer: authorizer,
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
	return rl
}

// GetTenant gets or creates a limiter for the given tenant. The limiters are
// reference counted; call Unlink on the returned limiter when it is no longer
// in use. If the closer channel is non-nil, closing it will lead to any blocked
// requests becoming unblocked.
func (rl *LimiterFactory) GetTenant(
	ctx context.Context, tenantID roachpb.TenantID, closer <-chan struct{},
) Limiter {

	if tenantID == roachpb.SystemTenantID {
		return &rl.systemLimiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	rcLim, ok := rl.mu.tenants[tenantID]
	if !ok {
		var options []quotapool.Option
		options = append(options, rl.knobs.QuotaPoolOptions...)
		if closer != nil {
			options = append(options, quotapool.WithCloser(closer))
		}

		rcLim = new(refCountedLimiter)
		rcLim.lim.init(rl, tenantID, rl.mu.config, rl.metrics.tenantMetrics(tenantID), rl.authorizer, options...)
		rl.mu.tenants[tenantID] = rcLim
		log.Infof(
			ctx, "tenant %s rate limiter initialized (rate: %g RU/s; burst: %g RU)",
			tenantID, rl.mu.config.Rate, rl.mu.config.Burst,
		)
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
	tenID := l.TenantID()

	rl.mu.Lock()
	defer rl.mu.Unlock()
	rcLim, ok := rl.mu.tenants[tenID]
	if !ok {
		panic(errors.AssertionFailedf("expected to find entry for tenant %v", tenID))
	}
	if &rcLim.lim != lim {
		panic(errors.AssertionFailedf("two limiters exist for tenant %v", tenID))
	}
	if rcLim.refCount--; rcLim.refCount == 0 {
		l.Release()
		delete(rl.mu.tenants, tenID)
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
