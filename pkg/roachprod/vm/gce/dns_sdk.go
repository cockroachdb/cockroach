// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/roachprodutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/option"
)

const (
	maxRecordsAPIPageSize = int64(200)
)

// sdkDNSProvider is a DNS provider that uses the Google Cloud DNS SDK.
type sdkDNSProvider struct {
	// dnsProject is the project used for all DNS operations.
	dnsProject string

	// publicZone is the gce zone used to manage A records for all clusters (e.g. roachprod).
	publicZone string
	// publicDomain is the DNS domain used to manage A records for all clusters (e.g. roachprod.crdb.io).
	publicDomain string

	// managedZone is the managed zone for SRV records (e.g. roachprod-managed).
	managedZone string
	// managedDomain is the domain for SRV records (e.g. roachprod-managed.crdb.io).
	managedDomain string

	recordsCache struct {
		mu      syncutil.Mutex
		records map[string][]vm.DNSRecord
	}
	recordLock struct {
		mu    syncutil.Mutex
		locks map[string]*syncutil.Mutex
	}

	resolvers []*net.Resolver

	dnsClient *dns.Service
}

// NewSDKDNSProvider creates a new GCE DNS provider using the Cloud DNS SDK.
func NewSDKDNSProvider(opts SDKDNSProviderOpts) (*sdkDNSProvider, error) {

	p := &sdkDNSProvider{
		dnsProject:    opts.DNSProject,
		publicZone:    opts.PublicZone,
		publicDomain:  opts.PublicDomain,
		managedZone:   opts.ManagedZone,
		managedDomain: opts.ManagedDomain,
		recordsCache: struct {
			mu      syncutil.Mutex
			records map[string][]vm.DNSRecord
		}{records: make(map[string][]vm.DNSRecord)},
		recordLock: struct {
			mu    syncutil.Mutex
			locks map[string]*syncutil.Mutex
		}{locks: make(map[string]*syncutil.Mutex)},
		resolvers: googleDNSResolvers(),
	}

	creds, _, err := roachprodutil.GetGCECredentials(
		context.Background(),
		roachprodutil.IAPTokenSourceOptions{},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get credentials")
	}

	client, err := dns.NewService(context.Background(), option.WithCredentials(creds))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DNS client")
	}

	p.dnsClient = client

	return p, nil
}

// SDKDNSProviderOpts defines the options for the SDK DNS provider.
type SDKDNSProviderOpts struct {
	DNSProject    string
	PublicZone    string
	PublicDomain  string
	ManagedZone   string
	ManagedDomain string
}

// NewFromGCEDNSProviderOpts creates a new SDKDNSProviderOpts from a gce.dnsOpts.
func (o *SDKDNSProviderOpts) NewFromGCEDNSProviderOpts(opts dnsOpts) SDKDNSProviderOpts {
	return SDKDNSProviderOpts(opts)
}

// NewSDKDNSProviderDefaultOptions returns the default options for the SDK DNS provider
// which are used if no options are provided.
// These defaults are the same as the legacy gcloud DNS provider.
func NewSDKDNSProviderDefaultOptions() SDKDNSProviderOpts {
	return SDKDNSProviderOpts{
		DNSProject:    defaultDNSProject,
		PublicZone:    dnsDefaultZone,
		PublicDomain:  dnsDefaultDomain,
		ManagedZone:   dnsDefaultManagedZone,
		ManagedDomain: dnsDefaultManagedDomain,
	}
}

// CreateRecords creates DNS records using the Cloud DNS SDK.
func (n *sdkDNSProvider) CreateRecords(ctx context.Context, records ...vm.DNSRecord) error {
	recordsByName := make(map[string][]vm.DNSRecord)
	for _, record := range records {
		// Ensure we use the normalized name for grouping records.
		record.Name = n.normalizeName(record.Name)
		recordsByName[record.Name] = append(recordsByName[record.Name], record)
	}

	for name, recordGroup := range recordsByName {

		// We assume that all records in a group have the same name, type, and ttl.
		firstRecord := recordGroup[0]
		for _, record := range recordGroup[1:] {
			if record.Type != firstRecord.Type || record.TTL != firstRecord.TTL {
				return errors.New("all records in a group must have the same type and ttl")
			}
		}

		// Determine the zone to use based on whether the record is public or not.
		zone := n.managedZone
		if firstRecord.Public {
			zone = n.publicZone
		}

		err := n.withRecordLock(name, func() error {
			existingRecords, err := n.lookupRecords(ctx, firstRecord.Type, name)
			if err != nil {
				return err
			}

			// For SRV records, we combine existing and new records because
			// we support multiple SRV records for the same name.
			// For other record types, we override existing records as we assume
			// one public IP address per roachprod node.
			combinedRecords := make(map[string]vm.DNSRecord)
			for _, record := range recordGroup {
				combinedRecords[record.Data] = record
			}

			// Combine old and new records using a map to deduplicate with the record
			// data as the key.
			if firstRecord.Type == vm.SRV && len(existingRecords) > 0 {
				for _, record := range existingRecords {
					combinedRecords[record.Data] = record
				}
			}

			// Build the record data
			data := maps.Keys(combinedRecords)
			sort.Strings(data)

			// Build the ResourceRecordSet
			rrset := &dns.ResourceRecordSet{
				Name:    n.ensureTrailingDot(name),
				Type:    string(firstRecord.Type),
				Ttl:     int64(firstRecord.TTL),
				Rrdatas: data,
			}

			// Execute create or update
			err = n.upsertRecordSet(ctx, zone, rrset, len(existingRecords) > 0)
			if err != nil {
				n.clearCacheEntry(name)
				return rperrors.TransientFailure(err, dnsProblemLabel)
			}

			// If fastDNS is enabled, we need to wait for the SRV records to become available
			// on the Google DNS servers.
			if config.FastDNS && !firstRecord.Public {
				err = n.waitForSRVRecordsAvailable(ctx, maps.Values(combinedRecords)...)
				if err != nil {
					return err
				}
			}

			n.updateCache(name, maps.Values(combinedRecords))
			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "failed to update records for %s", name)
		}
	}
	return nil
}

// upsertRecordSet creates or updates a DNS record set.
// If updateMode is true, it updates existing records; otherwise, it creates new records.
// It automatically retries with the opposite operation on 409 (conflict) or 404 (not found) errors.
func (n *sdkDNSProvider) upsertRecordSet(
	ctx context.Context, zone string, rrset *dns.ResourceRecordSet, updateMode bool,
) error {
	change := &dns.Change{}

	if updateMode {
		// For update, we need to specify both deletions and additions
		// First, get the existing record
		existing, err := n.dnsClient.ResourceRecordSets.List(n.dnsProject, zone).
			Name(rrset.Name).
			Type(rrset.Type).
			Context(ctx).
			Do()
		if err != nil {
			return errors.Wrap(err, "failed to list existing records for update")
		}

		if len(existing.Rrsets) == 0 {
			// Record doesn't exist, retry with create
			return n.upsertRecordSet(ctx, zone, rrset, false)
		}

		change.Deletions = existing.Rrsets
		change.Additions = []*dns.ResourceRecordSet{rrset}
	} else {
		// For create, only specify additions
		change.Additions = []*dns.ResourceRecordSet{rrset}
	}

	_, err := n.dnsClient.Changes.Create(n.dnsProject, zone, change).Context(ctx).Do()
	if err != nil {
		// Handle race conditions
		if strings.Contains(err.Error(), "409") || strings.Contains(err.Error(), "alreadyExists") {
			if !updateMode {
				// Retry with update
				return n.upsertRecordSet(ctx, zone, rrset, true)
			}
		}
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "notFound") {
			if updateMode {
				// Retry with create
				return n.upsertRecordSet(ctx, zone, rrset, false)
			}
		}
		return errors.Wrap(err, "failed to create/update DNS record")
	}

	return nil
}

// LookupRecords implements the vm.DNSProvider interface.
func (n *sdkDNSProvider) LookupRecords(
	ctx context.Context, recordType vm.DNSType, name string,
) ([]vm.DNSRecord, error) {
	var records []vm.DNSRecord
	var err error
	err = n.withRecordLock(name, func() error {
		if config.FastDNS && recordType == vm.SRV {
			rIdx := randutil.FastUint32() % uint32(len(n.resolvers))
			records, err = n.fastLookupSRVRecords(ctx, n.resolvers[rIdx], name, true)
			return err
		}
		records, err = n.lookupRecords(ctx, recordType, name)
		return err
	})
	return records, err
}

// ListRecords implements the vm.DNSProvider interface.
func (n *sdkDNSProvider) ListRecords(ctx context.Context) ([]vm.DNSRecord, error) {
	return n.listRecords(ctx, vm.SRV, "")
}

// deleteRecords deletes DNS records using the Cloud DNS SDK.
func (n *sdkDNSProvider) deleteRecords(
	ctx context.Context, zone string, recordType vm.DNSType, names ...string,
) error {
	for _, name := range names {
		err := n.withRecordLock(name, func() error {
			// First, list the records to delete
			existing, err := n.dnsClient.ResourceRecordSets.List(n.dnsProject, zone).
				Name(n.ensureTrailingDot(name)).
				Type(string(recordType)).
				Context(ctx).
				Do()
			if err != nil {
				return errors.Wrap(err, "failed to list records for deletion")
			}

			// Clear cache regardless of outcome
			defer n.clearCacheEntry(name)

			if len(existing.Rrsets) == 0 {
				// Record doesn't exist - treat as success (idempotent)
				return nil
			}

			// Create a change to delete the records
			change := &dns.Change{
				Deletions: existing.Rrsets,
			}

			_, err = n.dnsClient.Changes.Create(n.dnsProject, zone, change).Context(ctx).Do()
			if err != nil {
				// Treat 404 as success - record is already gone
				if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "notFound") {
					return nil
				}
				return errors.Wrap(err, "failed to delete DNS records")
			}

			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteSRVRecordsByName implements the vm.DNSProvider interface.
func (n *sdkDNSProvider) DeleteSRVRecordsByName(ctx context.Context, names ...string) error {
	return n.deleteRecords(ctx, n.managedZone, vm.SRV, names...)
}

// DeletePublicRecordsByName implements the vm.DNSProvider interface
func (n *sdkDNSProvider) DeletePublicRecordsByName(ctx context.Context, names ...string) error {
	return n.deleteRecords(ctx, n.publicZone, vm.A, names...)
}

// DeleteRecordsBySubdomain implements the vm.DNSProvider interface.
func (n *sdkDNSProvider) DeleteSRVRecordsBySubdomain(ctx context.Context, subdomain string) error {
	suffix := fmt.Sprintf("%s.%s.", subdomain, n.Domain())
	records, err := n.listRecords(ctx, vm.SRV, suffix)
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
	return n.DeleteSRVRecordsByName(ctx, maps.Keys(names)...)
}

// Domain implements the vm.DNSProvider interface.
// Note that this is the domain used for the managed zone with SRV records, not
// the public zone.
func (n *sdkDNSProvider) Domain() string {
	return n.managedDomain
}

// PublicDomain implements the vm.DNSProvider interface.
// This is the domain used for the public zone with A records.
func (n *sdkDNSProvider) PublicDomain() string {
	return n.publicDomain
}

// SyncDNS implements the vm.DNSProvider interface.
func (n *sdkDNSProvider) SyncDNS(l *logger.Logger, vms vm.List) error {
	return n.SyncDNSWithContext(context.Background(), l, vms)
}

// SyncDNSWithContext implements the vm.DNSProvider interface.
func (n *sdkDNSProvider) SyncDNSWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List,
) error {
	return n.syncPublicDNS(ctx, l, vms)
}

// ProviderName implements the vm.DNSProvider interface.
// The SDK DNS provider uses the same provider name as the gcloud DNS provider
// because they can be used interchangeably. The SDK is the preferred method
// going forward, but gcloud is still supported for backward compatibility.
func (n *sdkDNSProvider) ProviderName() string {
	return ProviderName
}

// LookupRecords looks up DNS records using the Cloud DNS SDK.
func (n *sdkDNSProvider) lookupRecords(
	ctx context.Context, recordType vm.DNSType, name string,
) ([]vm.DNSRecord, error) {
	// Check the cache first
	if cachedRecords, ok := n.getCache(name); ok {
		return cachedRecords, nil
	}

	// List records with filter
	records, err := n.listRecords(ctx, recordType, name)
	if err != nil {
		return nil, err
	}

	// Filter out records that do not match the full normalized target name
	filteredRecords := make([]vm.DNSRecord, 0, len(records))
	for _, record := range records {
		if n.normalizeName(record.Name) != n.normalizeName(name) {
			continue
		}
		filteredRecords = append(filteredRecords, record)
	}

	n.updateCache(name, filteredRecords)
	return filteredRecords, nil
}

// ListRecords lists DNS records using the Cloud DNS SDK.
// Handles pagination automatically to fetch all results.
func (n *sdkDNSProvider) listRecords(
	ctx context.Context, recordType vm.DNSType, filter string,
) ([]vm.DNSRecord, error) {
	zone := n.managedZone
	if recordType == vm.A {
		zone = n.publicZone
	}

	// DNS API max page size is much smaller than 10000
	// Use 200 as a safe page size (API limit is typically 1000 or less)

	var records []vm.DNSRecord
	pageToken := ""

	for {
		// Build the list request
		req := n.dnsClient.ResourceRecordSets.List(n.dnsProject, zone).
			MaxResults(maxRecordsAPIPageSize).
			Context(ctx)

		if filter != "" {
			req = req.Name(n.ensureTrailingDot(filter))
		}

		if pageToken != "" {
			req = req.PageToken(pageToken)
		}

		// Execute the request
		resp, err := req.Do()
		if err != nil {
			return nil, errors.Wrap(err, "failed to list DNS records")
		}

		// Convert to vm.DNSRecord
		for _, rrset := range resp.Rrsets {
			if rrset.Type != string(recordType) {
				continue
			}
			for _, data := range rrset.Rrdatas {
				records = append(records, vm.CreateDNSRecord(
					rrset.Name,
					recordType,
					data,
					int(rrset.Ttl),
				))
			}
		}

		// Check if there are more pages
		pageToken = resp.NextPageToken
		if pageToken == "" {
			break
		}
	}

	return records, nil
}

// lockRecordByName locks the record with the given name and returns a function
// that can be used to unlock it. The lock is used to prevent concurrent
// operations on the same record.
func (n *sdkDNSProvider) withRecordLock(name string, f func() error) error {
	recordMutex := func() *syncutil.Mutex {
		n.recordLock.mu.Lock()
		defer n.recordLock.mu.Unlock()
		normalizedName := n.normalizeName(name)
		mutex, ok := n.recordLock.locks[normalizedName]
		if !ok {
			mutex = new(syncutil.Mutex)
			n.recordLock.locks[normalizedName] = mutex
		}
		return mutex
	}()
	recordMutex.Lock()
	defer recordMutex.Unlock()
	return f()
}

func (n *sdkDNSProvider) updateCache(name string, records []vm.DNSRecord) {
	n.recordsCache.mu.Lock()
	defer n.recordsCache.mu.Unlock()
	n.recordsCache.records[n.normalizeName(name)] = records
}

func (n *sdkDNSProvider) getCache(name string) ([]vm.DNSRecord, bool) {
	n.recordsCache.mu.Lock()
	defer n.recordsCache.mu.Unlock()
	records, ok := n.recordsCache.records[n.normalizeName(name)]
	return records, ok
}

func (n *sdkDNSProvider) clearCacheEntry(name string) {
	n.recordsCache.mu.Lock()
	defer n.recordsCache.mu.Unlock()
	delete(n.recordsCache.records, n.normalizeName(name))
}

// syncPublicDNSWithSDK syncs the public DNS zone using the Cloud DNS SDK.
// This replaces all A records in the zone with records from the given VMs.
func (n *sdkDNSProvider) syncPublicDNS(ctx context.Context, l *logger.Logger, vms vm.List) error {
	if n.publicDomain == "" {
		return nil
	}

	// Build new A records from VMs
	var additions []*dns.ResourceRecordSet
	recordsByName := make(map[string]*dns.ResourceRecordSet)

	for _, v := range vms {
		// Local is hardcoded to avoid import cycle with vm.Provider's local package.
		if v.Provider == "local" {
			continue
		}
		if len(v.Name) >= 60 {
			l.Printf("debug: skipping VM %s with too long name for DNS sync", v.Name)
			continue
		}
		if v.PublicIP == "" {
			l.Printf("debug: skipping VM %s with no public IP for DNS sync", v.Name)
			continue
		}

		recordName := fmt.Sprintf("%s.%s.", v.Name, n.publicDomain)

		// Group IPs by name (in case multiple VMs have the same DNS name)
		if existing, ok := recordsByName[recordName]; ok {
			existing.Rrdatas = append(existing.Rrdatas, v.PublicIP)
		} else {
			recordsByName[recordName] = &dns.ResourceRecordSet{
				Name:    recordName,
				Type:    "A",
				Ttl:     60,
				Rrdatas: []string{v.PublicIP},
			}
		}
	}

	// Convert map to slice
	for _, rr := range recordsByName {
		additions = append(additions, rr)
	}

	// List existing A records with pagination
	const maxPageSize = 500
	var allExistingRecords []*dns.ResourceRecordSet
	pageToken := ""

	for {
		req := n.dnsClient.ResourceRecordSets.List(n.dnsProject, n.publicZone).
			MaxResults(maxPageSize).Context(ctx)

		if pageToken != "" {
			req = req.PageToken(pageToken)
		}

		existingResp, err := req.Do()
		if err != nil {
			return errors.Wrap(err, "failed to list existing DNS records")
		}

		allExistingRecords = append(allExistingRecords, existingResp.Rrsets...)

		pageToken = existingResp.NextPageToken
		if pageToken == "" {
			break
		}
	}

	// Filter deletions: delete all A records except NS and SOA
	var deletions []*dns.ResourceRecordSet
	for _, rrset := range allExistingRecords {
		// Skip NS and SOA records whatever happens next:
		// they are required for the zone to function.
		if rrset.Type == "NS" || rrset.Type == "SOA" {
			continue
		}

		// Only delete A records
		if rrset.Type == "A" {
			deletions = append(deletions, rrset)
		}
	}

	// If no changes, return early
	if len(deletions) == 0 && len(additions) == 0 {
		return nil
	}

	// Create the atomic change: delete all old A records, add new ones
	_, err := n.dnsClient.Changes.Create(
		n.dnsProject,
		n.publicZone,
		&dns.Change{
			Deletions: deletions,
			Additions: additions,
		},
	).Context(ctx).Do()
	if err != nil {
		return errors.Wrap(err, "failed to sync DNS records")
	}

	return nil
}

// waitForRecordsAvailable waits for the DNS records to become available on all
// the DNS servers through a standard net tools lookup.
func (n *sdkDNSProvider) waitForSRVRecordsAvailable(
	ctx context.Context, records ...vm.DNSRecord,
) error {
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
func (n *sdkDNSProvider) fastLookupSRVRecords(
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

// normalizeName removes the trailing dot from a DNS name if it exists.
// This is necessary because depending on where the name originates from, it
// may or may not have a trailing dot.
func (n *sdkDNSProvider) normalizeName(name string) string {
	return strings.TrimSuffix(name, ".")
}

// ensureTrailingDot adds a trailing dot to a DNS name if it does not exist.
func (n *sdkDNSProvider) ensureTrailingDot(name string) string {
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	return name
}
