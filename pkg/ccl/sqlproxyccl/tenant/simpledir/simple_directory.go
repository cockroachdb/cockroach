// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package simpledir

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// simpleDirectory is a directory that returns a single pre-defined address
// for all tenants. It is expected that a SQL pod is listening on that address,
// or else there will be connection failures.
type simpleDirectory struct {
	// podAddr refers to the address of the SQL pod, which consists of both the
	// host and port (e.g. "127.0.0.1:26257").
	podAddr string
}

var _ tenant.Resolver = &simpleDirectory{}

// NewSimpleDirectory constructs a new simple directory instance that abides to
// the Resolver interface.
func NewSimpleDirectory(podAddr string) tenant.Resolver {
	return &simpleDirectory{podAddr: podAddr}
}

// EnsureTenantAddr returns the SQL pod address associated with this directory.
// If the address cannot be resolved, a GRPC NotFound error will be returned.
//
// EnsureTenantAddr implements the Resolver interface.
func (d *simpleDirectory) EnsureTenantAddr(
	ctx context.Context, tenantID roachpb.TenantID, clusterName string,
) (string, error) {
	if err := d.resolvePodAddr(); err != nil {
		return "", err
	}
	return d.podAddr, nil
}

// LookupTenantAddrs returns a slice with a single SQL pod address, regardless
// of tenantID. If that address cannot be resolved, a GRPC NotFound error will
// be returned.
//
// LookupTenantAddrs implements the Resolver interface.
func (d *simpleDirectory) LookupTenantAddrs(
	ctx context.Context, tenantID roachpb.TenantID,
) ([]string, error) {
	if err := d.resolvePodAddr(); err != nil {
		return nil, err
	}
	return []string{d.podAddr}, nil
}

// ReportFailure is a no-op for the simple directory.
//
// ReportFailure implements the Resolver interface.
func (d *simpleDirectory) ReportFailure(
	ctx context.Context, tenantID roachpb.TenantID, addr string,
) error {
	// Nothing to do here.
	return nil
}

// ResolveTCPAddr is exported for testing.
var ResolveTCPAddr = net.ResolveTCPAddr

// resolvePodAddr resolves the SQL pod address associated with the simple
// directory. If the address cannot be resolved, a GRPC NotFound error is
// returned. This is used to prevent the sqlproxy from continuously retrying
// when there's a connection failure. If the address cannot be resolved, the
// proxy will return an error to the client immediately.
//
// TODO(jaylim-crl): Replace the NotFound error with a directory/resolver
// specific error.
func (d *simpleDirectory) resolvePodAddr() error {
	if _, err := ResolveTCPAddr("tcp", d.podAddr); err != nil {
		return status.Errorf(codes.NotFound, "SQL pod address cannot be resolved: %v", err.Error())
	}
	return nil
}
