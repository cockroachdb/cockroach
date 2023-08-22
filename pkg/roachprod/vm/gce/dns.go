// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gce

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const (
	dnsManagedZone = "roachprod-managed"
	dnsDomain      = "roachprod-managed.crdb.io"
	dnsServer      = "ns-cloud-a1.googledomains.com"
)

var _ vm.DNSProvider = &dnsProvider{}

// dnsProvider implements the vm.DNSProvider interface.
type dnsProvider struct {
	resolver *net.Resolver
}

func NewDNSProvider() vm.DNSProvider {
	resolver := new(net.Resolver)
	resolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialer := net.Dialer{}
		return dialer.DialContext(ctx, "udp", dnsServer+":53")
	}
	return &dnsProvider{resolver: resolver}
}

// CreateRecords implements the vm.DNSProvider interface.
func (n dnsProvider) CreateRecords(records ...vm.DNSRecord) error {
	recordsByName := make(map[string][]vm.DNSRecord)
	for _, record := range records {
		recordsByName[record.Name] = append(recordsByName[record.Name], record)
	}

	for name, recordGroup := range recordsByName {
		// No need to break the name down into components as the lookup command
		// accepts a fully qualified name as the last parameter if the service and
		// proto parameters are empty strings.
		existingRecords, err := n.lookupSRVRecords("", "", name)
		if err != nil {
			return err
		}
		dataSet := make(map[string]struct{})
		for _, record := range existingRecords {
			dataSet[record.Data] = struct{}{}
		}

		command := "create"
		if len(existingRecords) > 0 {
			command = "update"
		}

		// Add the new record data.
		for _, record := range recordGroup {
			dataSet[record.Data] = struct{}{}
		}
		// We assume that all records in a group have the same name, type, and ttl.
		// TODO(herko): Add error checking to ensure that the above is the case.
		firstRecord := recordGroup[0]
		data := maps.Keys(dataSet)
		sort.Strings(data)
		args := []string{"--project", dnsProject, "dns", "record-sets", command, name,
			"--type", string(firstRecord.Type),
			"--ttl", strconv.Itoa(firstRecord.TTL),
			"--zone", dnsManagedZone,
			"--rrdatas", strings.Join(data, ","),
		}
		cmd := exec.Command("gcloud", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "output: %s", out)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return n.waitForRecordsAvailable(ctx, records...)
}

// LookupSRVRecords implements the vm.DNSProvider interface.
func (n dnsProvider) LookupSRVRecords(service, proto, subdomain string) ([]vm.DNSRecord, error) {
	name := fmt.Sprintf(`%s.%s`, subdomain, n.Domain())
	return n.lookupSRVRecords(service, proto, name)
}

// DeleteRecordsBySubdomain implements the vm.DNSProvider interface.
func (n dnsProvider) DeleteRecordsBySubdomain(subdomain string) error {
	suffix := fmt.Sprintf("%s.%s.", subdomain, n.Domain())
	records, err := n.listSRVRecords(suffix)
	if err != nil {
		return err
	}
	for _, record := range records {
		// Only delete records that match the subdomain. The initial filter by
		// gcloud does not specifically match suffixes, hence we check here to
		// make sure it's only the suffix and not a partial match.
		if !strings.HasSuffix(record.Name, suffix) {
			continue
		}
		args := []string{"--project", dnsProject, "dns", "record-sets", "delete", record.Name,
			"--type", string(record.Type),
			"--zone", dnsManagedZone,
		}
		cmd := exec.Command("gcloud", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "output: %s", out)
		}
	}
	return nil
}

// Domain implements the vm.DNSProvider interface.
func (n dnsProvider) Domain() string {
	return dnsDomain
}

// lookupSRVRecords uses standard net tools to perform a DNS lookup. For
// lookups, we prefer this to using the gcloud command as it is faster, and
// preferable when service information is being queried regularly.
func (n dnsProvider) lookupSRVRecords(service, proto, name string) ([]vm.DNSRecord, error) {
	cName, srvRecords, err := n.resolver.LookupSRV(context.Background(), service, proto, name)
	// If the record is not found we tend to get a variety of errors including
	// "server misbehaving" and "no such host". We do not wish to fail when
	// nothing is found.
	if err != nil {
		//nolint:returnerrcheck
		return nil, nil
	}
	records := make([]vm.DNSRecord, len(srvRecords))
	for i, srvRecord := range srvRecords {
		records[i] = vm.CreateSRVRecord(cName, *srvRecord)
	}
	return records, nil
}

// listSRVRecords returns all SRV records that match the given filter from Google Cloud DNS.
// The data field of the records could be a comma-separated list of values if multiple
// records are returned for the same name.
func (n dnsProvider) listSRVRecords(filter string) ([]vm.DNSRecord, error) {
	args := []string{"--project", dnsProject, "dns", "record-sets", "list",
		"--filter", filter,
		"--zone", dnsManagedZone,
	}
	cmd := exec.Command("gcloud", args...)
	res, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrapf(err, "output: %s", res)
	}
	lines := strings.Split(string(res), "\n")
	if len(lines) < 2 {
		return nil, nil
	}
	records := make([]vm.DNSRecord, 0)
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		if fields[1] != string(vm.SRV) {
			continue
		}
		ttl, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, err
		}
		data := strings.Join(fields[3:], " ")
		records = append(records, vm.CreateDNSRecord(fields[0], vm.SRV, data, ttl))
	}
	return records, nil
}

// waitForRecordsAvailable waits for the DNS records to become available on the
// DNS server through a standard net tools lookup. A context with a timeout
// should be passed in.
func (n dnsProvider) waitForRecordsAvailable(ctx context.Context, records ...vm.DNSRecord) error {
	available := make(map[string]bool)
	splitChar := "|"
	keyFn := func(record vm.DNSRecord) string {
		return strings.TrimSuffix(record.Name, ".") + splitChar + record.Data
	}
	for _, record := range records {
		available[keyFn(record)] = false
	}
	for ctx.Err() == nil {
		keys := maps.Keys(available)
		names := make(map[string]struct{})
		for _, key := range keys {
			names[strings.Split(key, splitChar)[0]] = struct{}{}
		}
		for name := range names {
			foundRecords, err := n.lookupSRVRecords("", "", name)
			if err != nil {
				return err
			}
			for _, foundRecord := range foundRecords {
				delete(available, keyFn(foundRecord))
			}
		}
		if len(available) == 0 {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return errors.Wrapf(ctx.Err(), "timed out waiting for DNS records to become available")
}
