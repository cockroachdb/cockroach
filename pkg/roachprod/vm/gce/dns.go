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
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

const (
	dnsManagedZone           = "roachprod-managed"
	dnsDomain                = "roachprod-managed.crdb.io"
	dnsMaxResults            = 1000
	dnsMaxConcurrentRequests = 4
)

var ErrDNSOperation = fmt.Errorf("error during Google Cloud DNS operation")

var _ vm.DNSProvider = &dnsProvider{}

// dnsProvider implements the vm.DNSProvider interface.
type dnsProvider struct {
	recordsCache struct {
		mu      syncutil.Mutex
		records map[string][]vm.DNSRecord
	}
}

func NewDNSProvider() vm.DNSProvider {
	return &dnsProvider{
		recordsCache: struct {
			mu      syncutil.Mutex
			records map[string][]vm.DNSRecord
		}{records: make(map[string][]vm.DNSRecord)},
	}
}

// CreateRecords implements the vm.DNSProvider interface.
func (n *dnsProvider) CreateRecords(ctx context.Context, records ...vm.DNSRecord) error {
	recordsByName := make(map[string][]vm.DNSRecord)
	for _, record := range records {
		// Ensure we use the normalised name for grouping records.
		record.Name = n.normaliseName(record.Name)
		recordsByName[record.Name] = append(recordsByName[record.Name], record)
	}

	for name, recordGroup := range recordsByName {
		existingRecords, err := n.lookupSRVRecords(ctx, name)
		if err != nil {
			return err
		}
		command := "create"
		if len(existingRecords) > 0 {
			command = "update"
		}

		// Combine old and new records using a map to deduplicate with the record
		// data as the key.
		combinedRecords := make(map[string]vm.DNSRecord)
		for _, record := range existingRecords {
			combinedRecords[record.Data] = record
		}
		for _, record := range recordGroup {
			combinedRecords[record.Data] = record
		}

		// We assume that all records in a group have the same name, type, and ttl.
		// TODO(herko): Add error checking to ensure that the above is the case.
		firstRecord := recordGroup[0]
		data := maps.Keys(combinedRecords)
		sort.Strings(data)
		args := []string{"--project", dnsProject, "dns", "record-sets", command, name,
			"--type", string(firstRecord.Type),
			"--ttl", strconv.Itoa(firstRecord.TTL),
			"--zone", dnsManagedZone,
			"--rrdatas", strings.Join(data, ","),
		}
		cmd := exec.CommandContext(ctx, "gcloud", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return markDNSOperationError(errors.Wrapf(err, "output: %s", out))
		}
		n.updateCache(name, maps.Values(combinedRecords))
	}
	return nil
}

// LookupSRVRecords implements the vm.DNSProvider interface.
func (n *dnsProvider) LookupSRVRecords(ctx context.Context, name string) ([]vm.DNSRecord, error) {
	return n.lookupSRVRecords(ctx, name)
}

// ListRecords implements the vm.DNSProvider interface.
func (n *dnsProvider) ListRecords(ctx context.Context) ([]vm.DNSRecord, error) {
	return n.listSRVRecords(ctx, "", dnsMaxResults)
}

// DeleteRecordsByName implements the vm.DNSProvider interface.
func (n *dnsProvider) DeleteRecordsByName(ctx context.Context, names ...string) error {
	var g errgroup.Group
	g.SetLimit(dnsMaxConcurrentRequests)
	for _, name := range names {
		// capture loop variable
		name := name
		g.Go(func() error {
			args := []string{"--project", dnsProject, "dns", "record-sets", "delete", name,
				"--type", string(vm.SRV),
				"--zone", dnsManagedZone,
			}
			cmd := exec.CommandContext(ctx, "gcloud", args...)
			out, err := cmd.CombinedOutput()
			if err != nil {
				return markDNSOperationError(errors.Wrapf(err, "output: %s", out))
			}
			n.clearCacheEntry(name)
			return nil
		})
	}
	return g.Wait()
}

// DeleteRecordsBySubdomain implements the vm.DNSProvider interface.
func (n *dnsProvider) DeleteRecordsBySubdomain(ctx context.Context, subdomain string) error {
	suffix := fmt.Sprintf("%s.%s.", subdomain, n.Domain())
	records, err := n.listSRVRecords(ctx, suffix, dnsMaxResults)
	if err != nil {
		return err
	}

	names := make(map[string]struct{})
	for _, record := range records {
		names[record.Name] = struct{}{}
	}
	for name := range names {
		// Only delete records that match the subdomain. The initial filter by
		// gcloud does not specifically match suffixes, hence we check here to
		// make sure it's only the suffix and not a partial match. If not, we
		// delete the record from the map of names to delete.
		if !strings.HasSuffix(name, suffix) {
			delete(names, name)
		}
	}
	return n.DeleteRecordsByName(ctx, maps.Keys(names)...)
}

// Domain implements the vm.DNSProvider interface.
func (n *dnsProvider) Domain() string {
	return dnsDomain
}

// lookupSRVRecords uses standard net tools to perform a DNS lookup. This
// function will retry the lookup several times if there are any intermittent
// network problems. For lookups, we prefer this to using the gcloud command as
// it is faster, and preferable when service information is being queried
// regularly.
func (n *dnsProvider) lookupSRVRecords(ctx context.Context, name string) ([]vm.DNSRecord, error) {
	// Check the cache first.
	if cachedRecords, ok := n.getCache(name); ok {
		return cachedRecords, nil
	}
	// Lookup the records, if no records are found in the cache.
	records, err := n.listSRVRecords(ctx, name, dnsMaxResults)
	filteredRecords := make([]vm.DNSRecord, 0, len(records))
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		// Filter out records that do not match the full normalised target name.
		// This is necessary because the gcloud command does partial matching.
		if n.normaliseName(record.Name) != n.normaliseName(name) {
			continue
		}
		filteredRecords = append(filteredRecords, record)
	}
	n.updateCache(name, filteredRecords)
	return filteredRecords, nil
}

// listSRVRecords returns all SRV records that match the given filter from Google Cloud DNS.
// The data field of the records could be a comma-separated list of values if multiple
// records are returned for the same name.
func (n *dnsProvider) listSRVRecords(
	ctx context.Context, filter string, limit int,
) ([]vm.DNSRecord, error) {
	args := []string{"--project", dnsProject, "dns", "record-sets", "list",
		"--limit", strconv.Itoa(limit),
		"--page-size", strconv.Itoa(limit),
		"--zone", dnsManagedZone,
		"--format", "json",
	}
	if filter != "" {
		args = append(args, "--filter", filter)
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	res, err := cmd.CombinedOutput()
	if err != nil {
		return nil, markDNSOperationError(errors.Wrapf(err, "output: %s", res))
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
		return nil, markDNSOperationError(errors.Wrapf(err, "error unmarshalling output: %s", res))
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

func (n *dnsProvider) updateCache(name string, records []vm.DNSRecord) {
	n.recordsCache.mu.Lock()
	defer n.recordsCache.mu.Unlock()
	n.recordsCache.records[n.normaliseName(name)] = records
}

func (n *dnsProvider) getCache(name string) ([]vm.DNSRecord, bool) {
	n.recordsCache.mu.Lock()
	defer n.recordsCache.mu.Unlock()
	records, ok := n.recordsCache.records[n.normaliseName(name)]
	return records, ok
}

func (n *dnsProvider) clearCacheEntry(name string) {
	n.recordsCache.mu.Lock()
	defer n.recordsCache.mu.Unlock()
	delete(n.recordsCache.records, n.normaliseName(name))
}

// normaliseName removes the trailing dot from a DNS name if it exists.
// This is necessary because depending on where the name originates from, it
// may or may not have a trailing dot.
func (n *dnsProvider) normaliseName(name string) string {
	return strings.TrimSuffix(name, ".")
}

// markDNSOperationError should be used to mark any external DNS API or Google
// Cloud DNS CLI errors as DNS operation errors.
func markDNSOperationError(err error) error {
	return errors.Mark(err, ErrDNSOperation)
}
