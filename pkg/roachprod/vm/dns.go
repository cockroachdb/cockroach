// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vm

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const DNSRecordTTL = 60

var srvRe = regexp.MustCompile(`(\d+)\s+(\d+)\s+(\d+)\s+(\S+)$`)

// DNSType represents a DNS record type.
type DNSType string

const (
	A   DNSType = "A"
	SRV DNSType = "SRV"
)

// DNSRecord represents a DNS record.
type DNSRecord struct {
	// Name is the name of the DNS record.
	Name string `json:"name"`
	// Type is the type of the DNS record.
	Type DNSType `json:"type"`
	// Data is the data of the DNS record.
	Data string `json:"data"`
	// TTL is the time to live of the DNS record.
	TTL int `json:"TTL"`
}

// DNSProvider is an optional capability for a Provider that provides DNS
// management services.
type DNSProvider interface {
	CreateRecords(ctx context.Context, records ...DNSRecord) error
	LookupSRVRecords(ctx context.Context, service, proto, subdomain string) ([]DNSRecord, error)
	DeleteRecordsBySubdomain(subdomain string) error
	Domain() string
}

// FanOutDNS collates a collection of VMs by their DNS providers and invoke the
// callbacks in parallel. This function is lenient and skips VMs that do not
// have a DNS provider or if the provider is not a DNSProvider.
func FanOutDNS(list List, action func(DNSProvider, List) error) error {
	var m = map[string]List{}
	for _, vm := range list {
		// We allow DNSProvider to be empty, in which case we don't do anything.
		if vm.DNSProvider == "" {
			continue
		}
		m[vm.DNSProvider] = append(m[vm.DNSProvider], vm)
	}

	var g errgroup.Group
	for name, vms := range m {
		// capture loop variables
		n := name
		v := vms
		g.Go(func() error {
			p, ok := Providers[n]
			if !ok {
				return errors.Errorf("unknown provider name: %s", n)
			}
			dnsProvider, ok := p.(DNSProvider)
			if !ok {
				return errors.Errorf("provider %s is not a DNS provider", n)
			}
			return action(dnsProvider, v)
		})
	}

	return g.Wait()
}

// ForDNSProvider resolves the DNSProvider with the given name and executes the
// action.
func ForDNSProvider(named string, action func(DNSProvider) error) error {
	if named == "" {
		return errors.New("no DNS provider specified")
	}
	p, ok := Providers[named]
	if !ok {
		return errors.Errorf("unknown vm provider: %s", named)
	}
	dnsProvider, ok := p.(DNSProvider)
	if !ok {
		return errors.Errorf("provider %s is not a DNS provider", named)
	}
	if err := action(dnsProvider); err != nil {
		return errors.Wrapf(err, "in provider: %s", named)
	}
	return nil
}

// CreateDNSRecord creates a new DNS record.
func CreateDNSRecord(name string, dnsType DNSType, data string, ttl int) DNSRecord {
	return DNSRecord{
		Name: name,
		Type: dnsType,
		Data: data,
		TTL:  ttl,
	}
}

// CreateSRVRecord creates a new SRV DNS record.
func CreateSRVRecord(name string, data net.SRV) DNSRecord {
	dataStr := fmt.Sprintf("%d %d %d %s", data.Priority, data.Weight, data.Port, data.Target)
	return CreateDNSRecord(name, SRV, dataStr, DNSRecordTTL)
}

// ParseSRVRecord parses the data field in a DNS record. An SRV data struct is
// returned if the DNS record is an SRV record, otherwise an error is returned.
func (record DNSRecord) ParseSRVRecord() (*net.SRV, error) {
	if record.Type != SRV {
		return nil, fmt.Errorf("record is not an SRV record")
	}
	matches := srvRe.FindStringSubmatch(record.Data)
	if len(matches) != 5 {
		return nil, fmt.Errorf("invalid SRV record: %s", record.Data)
	}
	data := &net.SRV{}
	data.Target = matches[4]
	for i, field := range []*uint16{&data.Priority, &data.Weight, &data.Port} {
		v, err := strconv.Atoi(matches[i+1])
		*field = uint16(v)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
