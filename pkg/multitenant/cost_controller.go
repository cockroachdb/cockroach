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

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TenantSideCostController is an interface through which tenant code reports
// and throttles resource usage. Its implementation lives in the
// tenantcostclient CCL package.
type TenantSideCostController interface {
	Start(ctx context.Context, stopper *stop.Stopper, cpuSecsFn CPUSecsFn) error

	TenantSideKVInterceptor
}

// CPUSecsFn is a function used to get the cumulative CPU usage in seconds for
// the SQL instance.
type CPUSecsFn func(ctx context.Context) float64

// TenantSideKVInterceptor intercepts KV requests and responses, accounting
// for resource usage and potentially throttling requests.
//
// The TenantSideInterceptor is installed in the DistSender.
type TenantSideKVInterceptor interface {
	// OnRequestWait accounts for portion of the cost that can be determined
	// upfront. It can block to delay the request as needed, depending on the
	// current allowed rate of resource usage.
	OnRequestWait(ctx context.Context, info tenantcostmodel.RequestInfo) error

	// OnResponse accounts for the portion of the cost that can only be determined
	// after-the-fact. It does not block, but it can push the rate limiting into
	// "debt", causing future requests to be blocked.
	OnResponse(ctx context.Context, info tenantcostmodel.ResponseInfo)
}
