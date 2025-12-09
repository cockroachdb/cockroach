// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
)

const (
	// dnsProblemLabel is the label used when we see transient DNS
	// errors while making API calls to Cloud DNS.
	dnsProblemLabel = "dns_problem"
)

var (
	dnsDefaultZone, dnsDefaultDomain, dnsDefaultManagedZone, dnsDefaultManagedDomain string
)

func initDNSDefault() {
	dnsDefaultZone = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DNS_ZONE",
		"roachprod",
	)
	dnsDefaultDomain = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DNS_DOMAIN",
		// Preserve the legacy environment variable name for backwards
		// compatibility.
		config.EnvOrDefaultString(
			"ROACHPROD_DNS",
			"roachprod.crdb.io",
		),
	)
	dnsDefaultManagedZone = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DNS_MANAGED_ZONE",
		"roachprod-managed",
	)
	dnsDefaultManagedDomain = config.EnvOrDefaultString(
		"ROACHPROD_GCE_DNS_MANAGED_DOMAIN",
		"roachprod-managed.crdb.io",
	)
}

func googleDNSResolvers() []*net.Resolver {
	resolvers := make([]*net.Resolver, 0)
	for i := 1; i <= 4; i++ {
		dnsServer := fmt.Sprintf("ns-cloud-a%d.googledomains.com:53", i)
		resolver := new(net.Resolver)
		resolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, network, dnsServer)
		}
		resolvers = append(resolvers, resolver)
	}
	return resolvers
}
