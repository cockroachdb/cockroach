// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package multitenant

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TenantSideCostController is an interface through which tenant code reports
// and throttles resource usage. Its implementation lives in the
// tenantcostclient CCL package.
type TenantSideCostController interface {
	Start(
		ctx context.Context,
		stopper *stop.Stopper,
		instanceID base.SQLInstanceID,
		sessionID sqlliveness.SessionID,
		externalUsageFn ExternalUsageFn,
		nextLiveInstanceIDFn NextLiveInstanceIDFn,
	) error

	// GetCPUMovingAvg returns an exponential moving average used for estimating
	// the CPU usage (in CPU secs) per wall-clock second.
	GetCPUMovingAvg() float64

	// GetCostConfig returns the cost model config this TenantSideCostController
	// is using.
	GetCostConfig() *tenantcostmodel.Config

	TenantSideKVInterceptor

	TenantSideExternalIORecorder
}

// ExternalUsage contains information about usage that is not tracked through
// TenantSideKVInterceptor or TenantSideExternalIORecorder.
type ExternalUsage struct {
	// CPUSecs is the cumulative CPU usage in seconds for the SQL instance.
	CPUSecs float64

	// PGWireEgressBytes is the total bytes transferred from the SQL instance to
	// the client.
	PGWireEgressBytes uint64
}

// ExternalUsageFn is a function used to retrieve usage that is not tracked
// through TenantSideKVInterceptor.
type ExternalUsageFn func(ctx context.Context) ExternalUsage

// NextLiveInstanceIDFn is a function used to get the next live instance ID
// for this tenant. The information is used as a cleanup trigger on the server
// side and can be stale without causing correctness issues.
//
// Can return 0 if the value is not available right now.
//
// The function must not block.
type NextLiveInstanceIDFn func(ctx context.Context) base.SQLInstanceID

// TenantSideKVInterceptor intercepts KV requests and responses, accounting
// for resource usage and potentially throttling requests.
//
// The TenantSideInterceptor is installed in the DistSender.
type TenantSideKVInterceptor interface {
	// OnRequestWait blocks for as long as the rate limiter is in debt. Note that
	// actual costs are only accounted for by the OnResponseWait method.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	OnRequestWait(ctx context.Context) error

	// OnResponseWait blocks until the rate limiter has enough capacity to allow
	// the given request and response to be accounted for.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	OnResponseWait(
		ctx context.Context, req tenantcostmodel.RequestInfo, resp tenantcostmodel.ResponseInfo,
	) error
}

// WithTenantCostControlExemption generates a child context which will cause the
// TenantSideKVInterceptor to ignore the respective operations. This is used for
// important internal traffic that we don't want to stall (or be accounted for).
func WithTenantCostControlExemption(ctx context.Context) context.Context {
	return context.WithValue(ctx, exemptCtxValue, exemptCtxValue)
}

// HasTenantCostControlExemption returns true if this context or one of its
// parent contexts was created using WithTenantCostControlExemption.
func HasTenantCostControlExemption(ctx context.Context) bool {
	return ctx.Value(exemptCtxValue) != nil
}

// ExternalIOUsage specifies the amount of external I/O that has been consumed.
type ExternalIOUsage struct {
	IngressBytes int64
	EgressBytes  int64
}

// TenantSideExternalIORecorder accounts for resources consumed when writing or
// reading to/from external services such as an external storage provider.
type TenantSideExternalIORecorder interface {
	// OnExternalIOWait blocks until the rate limiter has enough capacity to allow
	// the external I/O operation. It returns an error if the wait is canceled.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	OnExternalIOWait(ctx context.Context, usage ExternalIOUsage) error

	// OnExternalIO reports ingress/egress that has occurred, without any
	// blocking.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	OnExternalIO(ctx context.Context, usage ExternalIOUsage)
}

type exemptCtxValueType struct{}

var exemptCtxValue interface{} = exemptCtxValueType{}

// TenantRUEstimateEnabled determines whether EXPLAIN ANALYZE should return an
// estimate for the number of RUs consumed by tenants.
var TenantRUEstimateEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.tenant_ru_estimation.enabled",
	"determines whether explain analyze should return an estimate for the query's RU consumption",
	true,
)
