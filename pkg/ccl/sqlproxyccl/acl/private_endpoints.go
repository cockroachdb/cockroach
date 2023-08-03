// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"context"
	"net"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/pires/go-proxyproto"
	"github.com/pires/go-proxyproto/tlvparse"
)

type lookupTenantFunc func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error)

// PrivateEndpoints represents the controller used to manage ACL rules for
// private connections. A connection is assumed to be private if it includes
// the EndpointID field, which gets populated through the PROXY headers. The
// controller rejects connections if:
//  1. the cluster does not allow private connections, or
//  2. none of the allowed_private_endpoints entries match the incoming
//     connection's endpoint identifier.
type PrivateEndpoints struct {
	LookupTenantFn lookupTenantFunc
}

var _ AccessController = &PrivateEndpoints{}

// CheckConnection implements the AccessController interface.
func (p *PrivateEndpoints) CheckConnection(ctx context.Context, conn ConnectionTags) error {
	// Public connections. This ACL is only responsible for private endpoints.
	if conn.EndpointID == "" {
		return nil
	}

	tenantObj, err := p.LookupTenantFn(ctx, conn.TenantID)
	if err != nil {
		return err
	}

	// Cluster allows private connections, so we'll check allowed endpoints.
	if tenantObj.AllowPrivateConn() {
		for _, endpoints := range tenantObj.AllowedPrivateEndpoints {
			// A matching endpointID was found.
			if endpoints == conn.EndpointID {
				return nil
			}
		}
	}

	// By default, connections are rejected if the cluster does not allow
	// private connections, or if no endpoints match the connection's endpoint
	// ID.
	return errors.Newf(
		"connection to '%s' denied: cluster does not allow private connections from endpoint '%s'",
		conn.TenantID.String(),
		conn.EndpointID,
	)
}

// FindPrivateEndpointID looks for the endpoint identifier within the connection
// object (which must be a *proxyproto.Conn) and returns that. If no endpoint
// IDs are found, an empty string will be returned.
func FindPrivateEndpointID(conn net.Conn) (string, error) {
	proxyConn, ok := conn.(*proxyproto.Conn)
	if !ok {
		// This should not happen.
		return "", errors.New("connection isn't a proxyproto.Conn")
	}
	header := proxyConn.ProxyHeader()
	// Not a private connection.
	if header == nil {
		return "", nil
	}
	tlvs, err := header.TLVs()
	if err != nil {
		return "", err
	}
	// AWS.
	if eid := tlvparse.FindAWSVPCEndpointID(tlvs); eid != "" {
		return eid, nil
	}
	// Azure.
	if eid, found := tlvparse.FindAzurePrivateEndpointLinkID(tlvs); found {
		return strconv.FormatUint(uint64(eid), 10), nil
	}
	// GCP.
	if eid, found := tlvparse.ExtractPSCConnectionID(tlvs); found {
		return strconv.FormatUint(eid, 10), nil
	}
	return "", nil
}
