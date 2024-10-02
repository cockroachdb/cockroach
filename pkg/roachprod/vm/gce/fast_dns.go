// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

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

// waitForRecordsAvailable waits for the DNS records to become available on all
// the DNS servers through a standard net tools lookup.
func (n *dnsProvider) waitForRecordsAvailable(ctx context.Context, records ...vm.DNSRecord) error {
	checkResolver := func(resolver *net.Resolver) error {
		type recordKey struct {
			name string
			data string
		}
		trimName := func(name string) string {
			return strings.TrimSuffix(name, ".")
		}
		notAvailable := make(map[recordKey]struct{})
		for _, record := range records {
			notAvailable[recordKey{
				name: trimName(record.Name),
				data: record.Data,
			}] = struct{}{}
		}

		for attempts := 0; attempts < 30; attempts++ {
			for key := range notAvailable {
				// Don't cache the results of this lookup as we may only have partial
				// results.
				foundRecords, err := n.fastLookupSRVRecords(ctx, resolver, key.name, false)
				if err != nil {
					return err
				}
				for _, foundRecord := range foundRecords {
					delete(notAvailable, recordKey{
						name: trimName(foundRecord.Name),
						data: foundRecord.Data,
					})
				}
			}
			if len(notAvailable) == 0 {
				return nil
			}
			time.Sleep(2 * time.Second)
		}
		return rperrors.TransientFailure(
			errors.Newf("waiting for DNS records to become available: %d out of %d records not available",
				len(notAvailable), len(records)), dnsProblemLabel,
		)
	}

	var g errgroup.Group
	for _, resolver := range n.resolvers {
		resolver := resolver
		g.Go(func() error {
			return checkResolver(resolver)
		})
	}
	return g.Wait()
}

// fastLookupSRVRecords uses standard net tools to perform a DNS lookup. This
// function will retry the lookup several times if there are any intermittent
// network problems.
func (n *dnsProvider) fastLookupSRVRecords(
	ctx context.Context, resolver *net.Resolver, name string, cache bool,
) ([]vm.DNSRecord, error) {
	// Check the cache first.
	if cache {
		if cachedRecords, ok := n.getCache(name); ok {
			return cachedRecords, nil
		}
	}
	var err error
	var cName string
	var srvRecords []*net.SRV
	err = retry.WithMaxAttempts(ctx, retry.Options{}, 10, func() error {
		cName, srvRecords, err = resolver.LookupSRV(ctx, "", "", name)
		if dnsError := (*net.DNSError)(nil); errors.As(err, &dnsError) {
			// We ignore some errors here as they are likely due to the record name not
			// existing. The net.LookupSRV function tends to return "server misbehaving"
			// and "no such host" errors when no record entries are found. Hence, making
			// the errors ambiguous and not useful. The errors are not exported, so we
			// have to check the error message.
			if dnsError.Err != "server misbehaving" && dnsError.Err != "no such host" && !dnsError.IsNotFound {
				return rperrors.TransientFailure(dnsError, dnsProblemLabel)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	records := make([]vm.DNSRecord, len(srvRecords))
	for i, srvRecord := range srvRecords {
		records[i] = vm.CreateSRVRecord(cName, *srvRecord)
	}
	if cache {
		n.updateCache(name, records)
	}
	return records, nil
}
