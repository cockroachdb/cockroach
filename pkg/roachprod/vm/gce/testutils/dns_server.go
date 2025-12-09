// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/exp/maps"
)

type Metrics struct {
	ListCalls, CreateCalls, UpdateCalls, DeleteCalls int
}

// TestDNSServer is a DNS "server" that can be used for testing purposes.
// It stores DNS records in memory and implements the vm.DNSProvider interface.
type TestDNSServer interface {
	vm.DNSProvider
	Metrics() Metrics
	Count() int
}

type testDNSServer struct {
	mu           syncutil.Mutex
	records      map[string]vm.DNSRecord
	metrics      Metrics
	domain       string
	publicDomain string
	providerName string
}

func (t *testDNSServer) storedName(name string) string {
	// Google DNS always returns the name with a trailing ".",
	// so we emulate that here.
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	return name
}

func (t *testDNSServer) normalizeName(name string) string {
	return strings.TrimSuffix(name, ".")
}

func (t *testDNSServer) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.records)
}

func (t *testDNSServer) Metrics() Metrics {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.metrics
}

// vm.DNSProvider interface implementation

func (t *testDNSServer) CreateRecords(ctx context.Context, records ...vm.DNSRecord) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Group records by name
	recordsByName := make(map[string][]vm.DNSRecord)
	for _, record := range records {
		normalizedName := t.normalizeName(record.Name)
		recordsByName[normalizedName] = append(recordsByName[normalizedName], record)
	}

	for name, recordGroup := range recordsByName {
		storedName := t.storedName(name)

		// Combine existing and new records (for SRV records)
		combinedRecords := make(map[string]vm.DNSRecord)
		for _, record := range recordGroup {
			combinedRecords[record.Data] = record
		}

		// Check if record already exists
		existingRecord, recordExists := t.records[storedName]
		if recordExists {
			// For SRV records, merge with existing
			if existingRecord.Type == vm.SRV {
				for _, data := range strings.Split(existingRecord.Data, ",") {
					if data != "" {
						combinedRecords[data] = vm.DNSRecord{
							Type: existingRecord.Type,
							Name: storedName,
							Data: data,
							TTL:  existingRecord.TTL,
						}
					}
				}
			}
		}

		// Build combined data
		dataSlice := maps.Keys(combinedRecords)
		combinedData := strings.Join(dataSlice, ",")

		// Update metrics based on whether this is a create or update
		firstRecord := recordGroup[0]
		if !recordExists {
			// This is a new record, increment CreateCalls
			t.metrics.CreateCalls++
			t.records[storedName] = vm.DNSRecord{
				Type: firstRecord.Type,
				Name: storedName,
				Data: combinedData,
				TTL:  firstRecord.TTL,
			}
		} else if existingRecord.Data != combinedData {
			// This is an update with different data, increment UpdateCalls
			t.metrics.UpdateCalls++
			t.records[storedName] = vm.DNSRecord{
				Type: firstRecord.Type,
				Name: storedName,
				Data: combinedData,
				TTL:  firstRecord.TTL,
			}
		}
		// If the record exists and the data is identical, don't increment anything
	}

	return nil
}

func (t *testDNSServer) LookupRecords(
	ctx context.Context, recordType vm.DNSType, name string,
) ([]vm.DNSRecord, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.ListCalls++

	storedName := t.storedName(name)
	if record, ok := t.records[storedName]; ok && record.Type == recordType {
		// Split the data back into individual records
		var records []vm.DNSRecord
		for _, data := range strings.Split(record.Data, ",") {
			if data != "" {
				records = append(records, vm.DNSRecord{
					Type: record.Type,
					Name: record.Name,
					Data: data,
					TTL:  record.TTL,
				})
			}
		}
		return records, nil
	}

	return []vm.DNSRecord{}, nil
}

func (t *testDNSServer) ListRecords(ctx context.Context) ([]vm.DNSRecord, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.ListCalls++

	var records []vm.DNSRecord
	for _, record := range t.records {
		// Split the data back into individual records
		for _, data := range strings.Split(record.Data, ",") {
			if data != "" {
				records = append(records, vm.DNSRecord{
					Type: record.Type,
					Name: record.Name,
					Data: data,
					TTL:  record.TTL,
				})
			}
		}
	}

	return records, nil
}

func (t *testDNSServer) DeleteSRVRecordsByName(ctx context.Context, names ...string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.DeleteCalls++

	for _, name := range names {
		storedName := t.storedName(name)
		delete(t.records, storedName)
	}

	return nil
}

func (t *testDNSServer) DeletePublicRecordsByName(ctx context.Context, names ...string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.DeleteCalls++

	for _, name := range names {
		storedName := t.storedName(name)
		if record, ok := t.records[storedName]; ok && record.Type == vm.A {
			delete(t.records, storedName)
		}
	}

	return nil
}

func (t *testDNSServer) DeleteSRVRecordsBySubdomain(ctx context.Context, subdomain string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics.DeleteCalls++

	suffix := fmt.Sprintf("%s.%s.", subdomain, t.domain)
	var toDelete []string
	for name, record := range t.records {
		if record.Type == vm.SRV && strings.HasSuffix(name, suffix) {
			toDelete = append(toDelete, name)
		}
	}

	for _, name := range toDelete {
		delete(t.records, name)
	}

	return nil
}

func (t *testDNSServer) Domain() string {
	return t.domain
}

func (t *testDNSServer) PublicDomain() string {
	return t.publicDomain
}

func (t *testDNSServer) SyncDNS(l *logger.Logger, vms vm.List) error {
	return t.SyncDNSWithContext(context.Background(), l, vms)
}

func (t *testDNSServer) SyncDNSWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List,
) error {
	// For testing purposes, this is a no-op
	return nil
}

func (t *testDNSServer) ProviderName() string {
	return t.providerName
}

// ProviderWithTestDNSServer initializes a test DNS server and a DNS capable
// provider pointing to the test server. It returns the test DNS server, the
// test DNS provider, and the provider name.
func ProviderWithTestDNSServer(rng *rand.Rand) (TestDNSServer, vm.DNSProvider, string) {
	// Since this is a global variable, we need to make sure the provider name is
	// unique, in order to avoid conflicts with other tests.
	providerName := fmt.Sprintf("testProvider-%d", rng.Uint32())

	testServer := &testDNSServer{
		records:      make(map[string]vm.DNSRecord),
		domain:       "test.crdb.io",
		publicDomain: "test-public.crdb.io",
		providerName: providerName,
	}

	// Register the test DNS provider globally
	vm.DNSProviders[providerName] = testServer

	return testServer, testServer, providerName
}
