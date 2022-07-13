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

	TenantSideExternalIORecorder
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

// TenantSideExternalIORecorder accounts for resources consumed when
// writing or reading to external services such as an external storage
// provider.
type TenantSideExternalIORecorder interface {
	// ExternalIOWriteWait waits for the RUs necessary to write the given number
	// of bytes to an external service. Callers should call ExternaIOWriteSuccess
	// or ExternalIOWriteFailure in the future to record the actual number of
	// bytes written.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	ExternalIOWriteWait(ctx context.Context, bytes int64) error

	// ExternalIOWriteFailure returns RUs to the pool in the case of a Write that
	// failed and wrote fewer bytes than expected. The RUs for the used number of
	// bytes is recorded, the RUs for the unused bytes are returned to the pool.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	ExternalIOWriteFailure(ctx context.Context, used int64, returned int64)

	// ExternalIOWriteSuccess records the usage of RUs associated with a write of
	// the given size. This function does not wait.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	ExternalIOWriteSuccess(ctx context.Context, bytes int64)

	// ExternalIOReadWait records the number of bytes read from an external
	// service, waiting for RUs if necessary.
	//
	// If the context (or a parent context) was created using
	// WithTenantCostControlExemption, the method is a no-op.
	ExternalIOReadWait(ctx context.Context, bytes int64) error
}

type exemptCtxValueType struct{}

var exemptCtxValue interface{} = exemptCtxValueType{}
