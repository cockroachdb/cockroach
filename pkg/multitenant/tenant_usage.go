// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenant

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TenantUsageServer is an interface through which tenant usage is reported and
// controlled, used on the host server side. Its implementation lives in the
// tenantcostserver CCL package.
type TenantUsageServer interface {
	// TokenBucketRequest implements the TokenBucket API of the roachpb.Internal
	// service. Used to service requests coming from tenants (through the
	// kvtenant.Connector)
	TokenBucketRequest(
		ctx context.Context, tenantID roachpb.TenantID, in *kvpb.TokenBucketRequest,
	) *kvpb.TokenBucketResponse

	// ReconfigureTokenBucket updates a tenant's token bucket settings.
	//
	// Arguments:
	//
	//  - availableTokens is the number of tokens that the tenant can consume at
	//    will. Also known as "burst tokens". If this is -1 (or any negative
	//    number), the bucket's available tokens are not updated.
	//
	//  - refillRate is the amount of tokens per second that the tenant
	//    receives. If this is 0, the bucket does not refill on its own.
	//
	//  - maxBurstTokens is the maximum number of tokens that can be accumulated
	//    from the refill rate, or 0 if there is no limit.
	//
	ReconfigureTokenBucket(
		ctx context.Context,
		ie isql.Txn,
		tenantID roachpb.TenantID,
		availableTokens float64,
		refillRate float64,
		maxBurstTokens float64,
	) error

	// Metrics returns the top-level metrics.
	Metrics() metric.Struct
}
