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
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const (
	dnsManagedZone = "roachprod-managed"
	dnsDomain      = "roachprod-managed.crdb.io"
	dnsServer      = "ns-cloud-a1.googledomains.com"
	dnsMaxResults  = 1000
)

var _ vm.DNSProvider = &dnsProvider{}

// dnsProvider implements the vm.DNSProvider interface.
type dnsProvider struct {
	resolver *net.Resolver
}

func NewDNSProvider() vm.DNSProvider {
	resolver := new(net.Resolver)
	resolver.StrictErrors = true
	resolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		dialer := net.Dialer{}
		// Prefer TCP over UDP. This is necessary because the DNS server
		// will return a truncated response if the response is too large
		// for a UDP packet, resulting in a "server misbehaving" error.
		return dialer.DialContext(ctx, "tcp", dnsServer+":53")
	}
	return &dnsProvider{resolver: resolver}
}

// CreateRecords implements the vm.DNSProvider interface.
func (n dnsProvider) CreateRecords(ctx context.Context, records ...vm.DNSRecord) error {
	recordsByName := make(map[string][]vm.DNSRecord)
	for _, record := range records {
		recordsByName[record.Name] = append(recordsByName[record.Name], record)
	}

	for name, recordGroup := range recordsByName {
		// No need to break the name down into components as the lookup command
		// accepts a fully qualified name as the last parameter if the service and
		// proto parameters are empty strings.
		existingRecords, err := n.lookupSRVRecords(ctx, "", "", name)
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
	// The DNS records are not immediately available after creation. We wait until
	// they are available before returning. This is necessary because the records
	// are required for starting servers. The waiting period should usually be
	// short (less than 30 seconds).
	return n.waitForRecordsAvailable(ctx, records...)
}

// LookupSRVRecords implements the vm.DNSProvider interface.
func (n dnsProvider) LookupSRVRecords(
	ctx context.Context, service, proto, subdomain string,
) ([]vm.DNSRecord, error) {
	name := fmt.Sprintf(`%s.%s`, subdomain, n.Domain())
	return n.lookupSRVRecords(ctx, service, proto, name)
}

// DeleteRecordsBySubdomain implements the vm.DNSProvider interface.
func (n dnsProvider) DeleteRecordsBySubdomain(subdomain string) error {
	suffix := fmt.Sprintf("%s.%s.", subdomain, n.Domain())
	records, err := n.listSRVRecords(suffix, dnsMaxResults)
	names := make(map[string]struct{})
	for _, record := range records {
		names[record.Name] = struct{}{}
	}
	if err != nil {
		return err
	}
	for name := range names {
		// Only delete records that match the subdomain. The initial filter by
		// gcloud does not specifically match suffixes, hence we check here to
		// make sure it's only the suffix and not a partial match.
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		args := []string{"--project", dnsProject, "dns", "record-sets", "delete", name,
			"--type", string(vm.SRV),
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
func (n dnsProvider) lookupSRVRecords(
	ctx context.Context, service, proto, name string,
) ([]vm.DNSRecord, error) {
	cName, srvRecords, err := n.resolver.LookupSRV(ctx, service, proto, name)
	if dnsError := (*net.DNSError)(nil); errors.As(err, &dnsError) {
		// We ignore some errors here as they are likely due to the record name not
		// existing. The net.LookupSRV function tends to return "server misbehaving"
		// and "no such host" errors when no record entries are found. Hence, making
		// the errors ambiguous and not useful. The errors are not exported, so we
		// have to check the error message.
		if dnsError.Err != "server misbehaving" && dnsError.Err != "no such host" && !dnsError.IsNotFound {
			return nil, err
		}
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
func (n dnsProvider) listSRVRecords(filter string, limit int) ([]vm.DNSRecord, error) {
	args := []string{"--project", dnsProject, "dns", "record-sets", "list",
		"--filter", filter,
		"--limit", strconv.Itoa(limit),
		"--page-size", strconv.Itoa(limit),
		"--zone", dnsManagedZone,
		"--format", "json",
	}
	cmd := exec.Command("gcloud", args...)
	res, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrapf(err, "output: %s", res)
	}
	var jsonList []struct {
		Name       string   `json:"name"`
		Kind       string   `json:"kind"`
		RecordType string   `json:"type"`
		TTL        int      `json:"ttl"`
		RRDatas    []string `json:"rrdatas"`
	}

	err = json.Unmarshal(res, &jsonList)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling output: %s", res)
	}

	records := make([]vm.DNSRecord, 0)
	for _, record := range jsonList {
		if record.Kind != "dns#resourceRecordSet" {
			continue
		}
		if record.RecordType != string(vm.SRV) {
			continue
		}
		for _, data := range record.RRDatas {
			records = append(records, vm.CreateDNSRecord(record.Name, vm.SRV, data, record.TTL))
		}
	}
	return records, nil
}

// waitForRecordsAvailable waits for the DNS records to become available on the
// DNS server through a standard net tools lookup.
func (n dnsProvider) waitForRecordsAvailable(ctx context.Context, records ...vm.DNSRecord) error {
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

	return retry.WithMaxAttempts(ctx, retry.Options{
		InitialBackoff: 5 * time.Second,
		Multiplier:     1,
	}, 20, func() error {
		for key := range notAvailable {
			foundRecords, err := n.lookupSRVRecords(ctx, "", "", key.name)
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
		return errors.Newf("waiting for DNS records to become available: %d out of %d records not available",
			len(notAvailable), len(records))
	})
}
