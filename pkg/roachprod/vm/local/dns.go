// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package local

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/roachprod/lock"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"golang.org/x/exp/maps"
)

var _ vm.DNSProvider = &dnsProvider{}

// dnsProvider implements the vm.DNSProvider interface.
type dnsProvider struct {
	configDir    string
	lockFilePath string
	zone         string
}

func NewDNSProvider(configDir, zone string) vm.DNSProvider {
	return &dnsProvider{configDir: configDir, lockFilePath: path.Join(configDir, "DNS_LOCK"), zone: zone}
}

// Domain is part of the vm.DNSProvider interface.
func (n *dnsProvider) Domain() string {
	return n.zone
}

// CreateRecords is part of the vm.DNSProvider interface.
func (n *dnsProvider) CreateRecords(_ context.Context, records ...vm.DNSRecord) error {
	unlock, err := lock.AcquireFilesystemLock(n.lockFilePath)
	if err != nil {
		return err
	}
	defer unlock()

	entries, err := n.loadRecords()
	if err != nil {
		return err
	}
	for _, record := range records {
		key := dnsKey(record)
		entries[key] = record
	}
	return n.saveRecords(entries)
}

// LookupSRVRecords is part of the vm.DNSProvider interface.
func (n *dnsProvider) LookupSRVRecords(_ context.Context, name string) ([]vm.DNSRecord, error) {
	records, err := n.loadRecords()
	if err != nil {
		return nil, err
	}
	var matchingRecords []vm.DNSRecord
	for _, record := range records {
		if record.Name == name && record.Type == vm.SRV {
			matchingRecords = append(matchingRecords, record)
		}
	}
	return matchingRecords, nil
}

// ListRecords is part of the vm.DNSProvider interface.
func (n *dnsProvider) ListRecords(_ context.Context) ([]vm.DNSRecord, error) {
	records, err := n.loadRecords()
	if err != nil {
		return nil, err
	}
	return maps.Values(records), nil
}

// DeleteRecordsByName is part of the vm.DNSProvider interface.
func (n *dnsProvider) DeleteRecordsByName(_ context.Context, names ...string) error {
	unlock, err := lock.AcquireFilesystemLock(n.lockFilePath)
	if err != nil {
		return err
	}
	defer unlock()

	entries, err := n.loadRecords()
	if err != nil {
		return err
	}
	for _, name := range names {
		delete(entries, name)
	}
	return n.saveRecords(entries)
}

// DeleteRecordsBySubdomain is part of the vm.DNSProvider interface.
func (n *dnsProvider) DeleteRecordsBySubdomain(_ context.Context, subdomain string) error {
	unlock, err := lock.AcquireFilesystemLock(n.lockFilePath)
	if err != nil {
		return err
	}
	defer unlock()

	re := regexp.MustCompile(fmt.Sprintf(`.*\.%s\.%s$`, subdomain, n.Domain()))
	entries, err := n.loadRecords()
	if err != nil {
		return err
	}
	for key, record := range entries {
		if re.MatchString(record.Name) {
			delete(entries, key)
		}
	}
	return n.saveRecords(entries)
}

// saveRecords saves the given records to a local DNS cache file.
func (n *dnsProvider) saveRecords(recordEntries map[string]vm.DNSRecord) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetIndent("", "  ")
	records := maps.Values(recordEntries)
	if err := enc.Encode(&records); err != nil {
		return err
	}

	// Other roachprod processes might be accessing the cache files at the same
	// time, so we need to write the file atomically by writing to a temporary
	// file and renaming. We store the temporary file in the same directory so
	// that it can always be renamed.
	tmpFile, err := os.CreateTemp(os.ExpandEnv(n.configDir), n.zone+".tmp")
	if err != nil {
		return err
	}

	_, err = tmpFile.Write(b.Bytes())
	err = errors.CombineErrors(err, tmpFile.Sync())
	err = errors.CombineErrors(err, tmpFile.Close())
	if err == nil {
		err = os.Rename(tmpFile.Name(), n.dnsFileName())
	}
	if err != nil {
		_ = os.Remove(tmpFile.Name())
		return err
	}
	return nil
}

// loadRecords loads the DNS records from the local DNS cache file.
func (n *dnsProvider) loadRecords() (map[string]vm.DNSRecord, error) {
	data, err := os.ReadFile(n.dnsFileName())
	recordEntries := make(map[string]vm.DNSRecord, 0)
	if err != nil {
		// It is expected that the file might not exist yet if no records have been
		// created before. In this case, return an empty map.
		if oserror.IsNotExist(err) {
			return recordEntries, nil
		}
		return nil, err
	}
	records := make([]vm.DNSRecord, 0)
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	for _, record := range records {
		recordEntries[dnsKey(record)] = record
	}
	return recordEntries, nil
}

// dnsFileName returns the name of the local file storing DNS records.
func (n *dnsProvider) dnsFileName() string {
	return path.Join(os.ExpandEnv(n.configDir), n.zone+".json")
}

// dnsKey returns a unique key for the given DNS record.
func dnsKey(record vm.DNSRecord) string {
	return fmt.Sprintf("%s:%s:%s", record.Name, record.Type, record.Data)
}
