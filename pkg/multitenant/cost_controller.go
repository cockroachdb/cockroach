// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenant

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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

	// GetRequestUnitModel returns the request unit cost model that this
	// TenantSideCostController is using.
	GetRequestUnitModel() *tenantcostmodel.RequestUnitModel

	// GetEstimatedCPUModel returns the estimated CPU cost model that this
	// TenantSideCostController is using.
	GetEstimatedCPUModel() *tenantcostmodel.EstimatedCPUModel

	// Metrics returns a metric.Struct which holds metrics for the controller.
	Metrics() metric.Struct

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
		ctx context.Context,
		request *kvpb.BatchRequest,
		response *kvpb.BatchResponse,
		targetRange *roachpb.RangeDescriptor,
		targetReplica *roachpb.ReplicaDescriptor,
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
