// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acl

import (
	"context"
	"net"

	"github.com/cockroachdb/errors"
)

// CIDRRanges represents the controller used to manage ACL rules for public
// connections. It rejects connections if:
//  1. the cluster does not allow public connections, or
//  2. none of the allowed_cidr_ranges entries match the incoming connection's
//     IP.
type CIDRRanges struct {
	LookupTenantFn lookupTenantFunc
}

var _ AccessController = &CIDRRanges{}

// CheckConnection implements the AccessController interface.
func (p *CIDRRanges) CheckConnection(ctx context.Context, conn ConnectionTags) error {
	// Private connections. This ACL is only responsible for public CIDR ranges.
	if conn.EndpointID != "" {
		return nil
	}

	tenantObj, err := p.LookupTenantFn(ctx, conn.TenantID)
	if err != nil {
		return err
	}

	// Cluster allows public connections, so we'll check allowed CIDR ranges.
	if tenantObj.AllowPublicConn() {
		ip := net.ParseIP(conn.IP)
		if ip == nil {
			return errors.Newf("could not parse IP address: '%s'", conn.IP)
		}
		for _, cidrRange := range tenantObj.AllowedCIDRRanges {
			// It is assumed that all public CIDR ranges are valid, so the
			// tenant directory server will have to enforce that.
			_, ipNetwork, err := net.ParseCIDR(cidrRange)
			if err != nil {
				return err
			}
			// A matching CIDR range was found.
			if ipNetwork.Contains(ip) {
				return nil
			}
		}
	}

	// By default, connections are rejected if the cluster does not allow public
	// connections, or if no ranges match the connection's IP.
	return errors.Newf(
		"connection to '%s' denied: cluster does not allow public connections from IP %s",
		conn.TenantID.String(),
		conn.IP,
	)
}
