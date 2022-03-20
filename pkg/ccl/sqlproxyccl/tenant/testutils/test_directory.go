// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package testutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var _ tenant.Directory = &TestDirectory{}

// TestDirectory is a test implementation of the tenant directory, and should
// only be used for testing. This allows callers to define custom logic for
// directory methods through hooks. Note that if a hook was not defined, and
// the method was called, a panic will occur. All methods on this test directory
// are thread-safe.
type TestDirectory struct {
	mu     syncutil.Mutex
	counts struct {
		ensureTenantAddr int
		lookupTenantAddr int
		reportFailure    int
	}
	EnsureTenantAddrFn  func(ctx context.Context, tenantID roachpb.TenantID, clusterName string) (string, error)
	LookupTenantAddrsFn func(ctx context.Context, tenantID roachpb.TenantID) ([]string, error)
	ReportFailureFn     func(ctx context.Context, tenantID roachpb.TenantID, addr string) error
}

// EnsureTenantAddr implements the Resolver interface.
func (d *TestDirectory) EnsureTenantAddr(
	ctx context.Context, tenantID roachpb.TenantID, clusterName string,
) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts.ensureTenantAddr++
	return d.EnsureTenantAddrFn(ctx, tenantID, clusterName)
}

// LookupTenantAddrs implements the Resolver interface.
func (d *TestDirectory) LookupTenantAddrs(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts.lookupTenantAddr++
	return d.LookupTenantAddrsFn(ctx, tenantID)
}

// ReportFailure implements the Resolver interface.
func (d *TestDirectory) ReportFailure(
	ctx context.Context, tenantID roachpb.TenantID, addr string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts.reportFailure++
	return d.ReportFailureFn(ctx, tenantID, addr)
}

// Counts returns the number of times each method is called.
func (d *TestDirectory) Counts() (int, int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.counts.ensureTenantAddr, d.counts.lookupTenantAddr, d.counts.reportFailure
}

// ResetCounts resets the counts associated with each method.
func (d *TestDirectory) ResetCounts() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts.ensureTenantAddr = 0
	d.counts.lookupTenantAddr = 0
	d.counts.reportFailure = 0
}
