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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TenantUsageServer is an interface through which tenant usage is reported and
// controlled. Its implementation lives in the tenantcostserver CCL package.
type TenantUsageServer interface {
	// TokenBucketRequest implements the TokenBucket API of the roachpb.Internal
	// service. Used to to service requests coming from tenants (through the
	// kvtenant.Connector)
	TokenBucketRequest(
		ctx context.Context, tenantID roachpb.TenantID, in *roachpb.TokenBucketRequest,
	) (*roachpb.TokenBucketResponse, error)

	// TODO(radu): add Reconfigure API.
}
