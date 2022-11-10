// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantutils

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

// CreateTenantOption is an option passed during the creation of a tenant.
type CreateTenantOption func(opts *CreateTenantOptions)

// WithTenantInfo sets the TenantInfo at the time of creation.
func WithTenantInfo(info descpb.TenantInfoWithUsage) CreateTenantOption {
	return func(opts *CreateTenantOptions) {
		opts.TenantInfo = &info
	}
}

// WithSkipTenantKeyspaceInit skip the initialization of the tenant keyspace.
func WithSkipTenantKeyspaceInit() CreateTenantOption {
	return func(opts *CreateTenantOptions) {
		opts.SkipTenantKeyspaceInit = true
	}
}

// CreateTenantOptions holds dependencies and values that can be overridden by
// callers of CreateTenant or CreateTenantWithID via a passed CreateTenantOption.
type CreateTenantOptions struct {
	TenantInfo             *descpb.TenantInfoWithUsage
	SkipTenantKeyspaceInit bool
}
