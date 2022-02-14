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

	TenantSideKVInterceptor

	TenantSideExternalIOInterceptor
}

// ExternalUsage contains information about usage that is not tracked through
// TenantSideKVInterceptor.
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
	// OnRequestWait accounts for portion of the cost that can be determined
	// upfront. It can block to delay the request as needed, depending on the
	// current allowed rate of resource usage.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	OnRequestWait(ctx context.Context, info tenantcostmodel.RequestInfo) error

	// OnResponse accounts for the portion of the cost that can only be determined
	// after-the-fact. It does not block, but it can push the rate limiting into
	// "debt", causing future requests to be blocked.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	OnResponse(
		ctx context.Context, req tenantcostmodel.RequestInfo, resp tenantcostmodel.ResponseInfo,
	)
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

// TenantSideExternalIOInterceptor intercepts external IO Write
// requests accounting for resource usage.
//
// The TenantSideExternalIOInterceptor is installed in the external
// storage factory.
type TenantSideExternalIOInterceptor interface {
	// OnExeternalWriteResponse records the number of bytes
	// written in an external IO request. We measure this after
	// the request in case no bytes were sent because of a network
	// error.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	//
	// TODO(ssd): Perhaps we should extend RequestInfo rather than
	// passing a direct int64 here.
	//
	// TODO(ssd): We may want two methods here like we have for
	// the KVInterceptor, not just because we may want to block,
	// but also so that we can track the "request count" at a
	// higher level than individual write calls.
	OnExternalWriteResponse(ctx context.Context, bytes int64)
}

type exemptCtxValueType struct{}

var exemptCtxValue interface{} = exemptCtxValueType{}
