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
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const (
	dnsManagedZone = "roachprod-managed"
	dnsDomain      = "roachprod-managed.crdb.io"
	dnsMaxResults  = 10000

	// dnsProblemLabel is the label used when we see transient DNS
	// errors while making API calls to Cloud DNS.
	dnsProblemLabel = "dns_problem"
)

var (
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
)

var ErrDNSOperation = fmt.Errorf("error during Google Cloud DNS operation")

var _ vm.DNSProvider = &dnsProvider{}

type ExecFn func(cmd *exec.Cmd) ([]byte, error)

// dnsProvider implements the vm.DNSProvider interface.
type dnsProvider struct {
	// dnsProject is the project used for all DNS operations.
	dnsProject string

	// zone is the gce zone used to manage A records for all clusters (e.g. roachprod).
	zone string
	// domain is the DNS domain used to manage A records for all clusters (e.g. roachprod.crdb.io).
	domain string

	// managedZone is the managed zone for SRV records (e.g. roachprod-managed).
	managedZone string
	// managedDomain is the domain for SRV records (e.g. roachprod-managed.crdb.io).
	managedDomain string

	recordsCache struct {
		mu      syncutil.Mutex
		records map[string][]vm.DNSRecord
	}
	execFn ExecFn
}

func NewDNSProvider() *dnsProvider {
	var gcloudMu syncutil.Mutex
	return NewDNSProviderWithExec(func(cmd *exec.Cmd) ([]byte, error) {
		// Limit to one gcloud command at a time. At this time we are unsure if it's
		// safe to make concurrent calls to the `gcloud` CLI to mutate DNS records
		// in the same zone. We don't mutate the same record in parallel, but we do
		// mutate different records in the same zone. See: #122180 for more details.
		gcloudMu.Lock()
		defer gcloudMu.Unlock()
		return cmd.CombinedOutput()
	})
}

func NewDNSProviderWithExec(execFn ExecFn) *dnsProvider {
	return &dnsProvider{
		dnsProject:    defaultDNSProject,
		zone:          dnsDefaultZone,
		domain:        dnsDefaultDomain,
		managedZone:   dnsDefaultManagedZone,
		managedDomain: dnsDefaultManagedDomain,
		recordsCache: struct {
			mu      syncutil.Mutex
			records map[string][]vm.DNSRecord
		}{records: make(map[string][]vm.DNSRecord)},
		execFn: execFn,
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
		args := []string{"--project", n.dnsProject, "dns", "record-sets", command, name,
			"--type", string(firstRecord.Type),
			"--ttl", strconv.Itoa(firstRecord.TTL),
			"--zone", n.managedZone,
			"--rrdatas", strings.Join(data, ","),
		}
		cmd := exec.CommandContext(ctx, "gcloud", args...)
		out, err := n.execFn(cmd)
		if err != nil {
			// Clear the cache entry if the operation failed, as the records may
			// have been partially updated.
			n.clearCacheEntry(name)
			return rperrors.TransientFailure(errors.Wrapf(err, "output: %s", out), dnsProblemLabel)
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
	for _, name := range names {
		args := []string{"--project", n.dnsProject, "dns", "record-sets", "delete", name,
			"--type", string(vm.SRV),
			"--zone", n.zone,
		}
		cmd := exec.CommandContext(ctx, "gcloud", args...)
		out, err := n.execFn(cmd)
		// Clear the cache entry regardless of the outcome. As the records may
		// have been partially deleted.
		n.clearCacheEntry(name)
		if err != nil {
			return rperrors.TransientFailure(errors.Wrapf(err, "output: %s", out), dnsProblemLabel)
		}
	}
	return nil
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
//
// Note that this is the domain used for the managed zone with SRV records, not
// the public zone.
func (n *dnsProvider) Domain() string {
	return n.managedDomain
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
	if err != nil {
		return nil, err
	}
	filteredRecords := make([]vm.DNSRecord, 0, len(records))
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
	args := []string{"--project", n.dnsProject, "dns", "record-sets", "list",
		"--limit", strconv.Itoa(limit),
		"--page-size", strconv.Itoa(limit),
		"--zone", n.managedZone,
		"--format", "json",
	}
	if filter != "" {
		args = append(args, "--filter", filter)
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	res, err := n.execFn(cmd)
	if err != nil {
		return nil, rperrors.TransientFailure(errors.Wrapf(err, "output: %s", res), dnsProblemLabel)
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
		return nil, rperrors.TransientFailure(errors.Wrapf(err, "error unmarshaling output: %s", res), dnsProblemLabel)
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

// syncDNS syncs the public DNS zone with the given list of VMs.
//
// Note that this operates on the public DNS zone, not the managed zone.
func (p *dnsProvider) syncDNS(l *logger.Logger, vms vm.List) (err error) {
	if p.domain == "" {
		return nil
	}

	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "syncing DNS for %s", p.domain)
		}
	}()

	f, err := os.CreateTemp(os.ExpandEnv("$HOME/.roachprod/"), "dns.bind")
	if err != nil {
		return err
	}
	defer f.Close()

	// Keep imported zone file in dry run mode.
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			l.Errorf("removing %s failed: %v", f.Name(), err)
		}
	}()

	var zoneBuilder strings.Builder
	for _, vm := range vms {
		entry, err := vm.ZoneEntry()
		if err != nil {
			l.Printf("WARN: skipping: %s\n", err)
			continue
		}
		zoneBuilder.WriteString(entry)
	}
	fmt.Fprint(f, zoneBuilder.String())
	f.Close()

	args := []string{"--project", p.dnsProject, "dns", "record-sets", "import",
		f.Name(), "-z", p.zone, "--delete-all-existing", "--zone-file-format"}
	cmd := exec.Command("gcloud", args...)
	output, err := cmd.CombinedOutput()

	return errors.Wrapf(err,
		"Command: %s\nOutput: %s\nZone file contents:\n%s",
		cmd, output, zoneBuilder.String(),
	)
}
