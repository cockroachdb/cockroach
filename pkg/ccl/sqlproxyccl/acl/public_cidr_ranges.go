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

	"github.com/cockroachdb/errors"
)

// PublicCIDRRanges represents the controller used to manage ACL rules for
// public connections. A connection is assumed to be public if it does not
// include the EndpointID field.
type PublicCIDRRanges struct {
	LookupTenantFn lookupTenantFunc
}

var _ AccessController = &PublicCIDRRanges{}

// CheckConnection implements the AccessController interface.
func (p *PublicCIDRRanges) CheckConnection(ctx context.Context, conn ConnectionTags) error {
	tenantObj, err := p.LookupTenantFn(ctx, conn.TenantID)
	if err != nil {
		return err
	}

	// Private connections. This ACL is only responsible for public CIDR ranges.
	if conn.EndpointID != "" {
		return nil
	}

	// Cluster allows public connections, so we'll check allowed CIDR ranges.
	if tenantObj.AllowPublicConn() {
		ip := net.ParseIP(conn.IP)
		if ip == nil {
			return errors.Newf("could not parse IP address: '%s'", conn.IP)
		}
		for _, cidrRange := range tenantObj.PublicCIDRRanges {
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
	// connections, or there are no ranges defined.
	return errors.Newf(
		"connection to '%s' denied: cluster does not allow public connections from IP %s",
		conn.TenantID.String(),
		conn.IP,
	)
}
