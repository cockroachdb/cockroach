// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package netutil

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/errors"
)

// SRV returns a slice of addresses from SRV record lookup
func SRV(ctx context.Context, name string) ([]string, error) {
	// Ignore port
	name, _, err := addr.SplitHostPort(name, base.DefaultPort)
	if err != nil {
		return nil, err
	}

	if name == "" {
		return nil, nil
	}

	// "" as the addr and proto forces the direct look up of the name
	_, recs, err := lookupSRV("", "", name)
	if err != nil {
		if dnsErr := (*net.DNSError)(nil); errors.As(err, &dnsErr) && dnsErr.Err == "no such host" {
			return nil, nil
		}

		if log.V(1) {
			log.Infof(context.TODO(), "failed to lookup SRV record for %q: %v", name, err)
		}

		return nil, nil
	}

	addrs := []string{}
	for _, r := range recs {
		if r.Port != 0 {
			addrs = append(addrs, net.JoinHostPort(r.Target, fmt.Sprintf("%d", r.Port)))
		}
	}

	return addrs, nil
}

var (
	lookupSRV = net.LookupSRV
)

// TestingOverrideSRVLookupFn enables a test to temporarily override
// the SRV lookup function.
func TestingOverrideSRVLookupFn(
	fn func(service, proto, name string) (cname string, addrs []*net.SRV, err error),
) func() {
	prevFn := lookupSRV
	lookupSRV = fn
	return func() { lookupSRV = prevFn }
}
