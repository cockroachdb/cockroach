// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package local

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

type dnsTestRec [2]string

func createTestDNSRecords(testRecords ...dnsTestRec) []vm.DNSRecord {
	var dnsRecords []vm.DNSRecord
	for _, v := range testRecords {
		dnsRecords = append(dnsRecords, vm.DNSRecord{
			Name: v[0],
			Type: vm.SRV,
			Data: v[1],
			TTL:  60,
		})
	}
	return dnsRecords
}

func createTestDNSProvider(t *testing.T, testRecords ...dnsTestRec) vm.DNSProvider {
	p := NewDNSProvider(t.TempDir(), "local-zone")
	err := p.CreateRecords(context.Background(), createTestDNSRecords(testRecords...)...)
	require.NoError(t, err)
	return p
}

func TestLookupRecords(t *testing.T) {
	ctx := context.Background()
	p := createTestDNSProvider(t, []dnsTestRec{
		{"_system-sql._tcp.local.local-zone", "0 1000 29001 local-0001.local-zone"},
		{"_system-sql._tcp.local.local-zone", "0 1000 29002 local-0002.local-zone"},
		{"_system-sql._tcp.local.local-zone", "0 1000 29003 local-0003.local-zone"},
		{"_tenant-1-sql._tcp.local.local-zone", "5 50 29004 local-0001.local-zone"},
		{"_tenant-2-sql._tcp.local.local-zone", "5 50 29005 local-0002.local-zone"},
		{"_tenant-3-sql._tcp.local.local-zone", "5 50 29006 local-0003.local-zone"},
	}...)

	t.Run("lookup system", func(t *testing.T) {
		records, err := p.LookupSRVRecords(ctx, "_system-sql._tcp.local.local-zone")
		require.NoError(t, err)
		require.Equal(t, 3, len(records))
		for _, r := range records {
			require.True(t, strings.HasPrefix(r.Name, "_system-sql"))
			require.Equal(t, vm.SRV, r.Type)
		}
	})

	t.Run("parse SRV data", func(t *testing.T) {
		records, err := p.LookupSRVRecords(ctx, "_tenant-1-sql._tcp.local.local-zone")
		require.NoError(t, err)
		require.Equal(t, 1, len(records))
		data, err := records[0].ParseSRVRecord()
		require.NoError(t, err)
		require.Equal(t, uint16(5), data.Priority)
		require.Equal(t, uint16(50), data.Weight)
		require.Equal(t, uint16(29004), data.Port)
	})

}
