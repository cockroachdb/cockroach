// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cidr

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestCIDRLookup(t *testing.T) {
	c := CIDRLookup{}

	destinations := []CIDR{
		// 192.168.0.0 - 192.168.0.255
		{
			Name:  "CIDR1",
			Ipnet: "192.168.0.0/24",
		},
		// 192.168.0.0 - 192.168.0.127
		{
			Name:  "CIDR2",
			Ipnet: "192.168.0.0/25",
		},
		// 10.0.0.0 - 10.0.255.255
		{
			Name:  "CIDR3",
			Ipnet: "10.0.0.0/16",
		},
		// 10.0.0.1
		{
			Name:  "CIDR4",
			Ipnet: "10.0.0.1/32",
		},
	}
	require.NoError(t, c.setDestinations(context.Background(), destinations))

	tests := []struct {
		ip       string
		expected string
	}{
		{"192.168.0.200", "CIDR1"},
		{"192.168.0.2", "CIDR2"},
		{"10.0.0.2", "CIDR3"},
		{"10.0.0.1", "CIDR4"},
		{"172.16.0.1", ""},
	}

	for _, test := range tests {
		t.Run(test.ip, func(t *testing.T) {
			actual := c.Lookup(net.ParseIP(test.ip))
			require.Equal(t, test.expected, actual)
		})
	}
}

// TestCIDRSetURL reads destinations from a JSON file and tests the CIDR lookup.
func TestCIDRSetURL(t *testing.T) {
	skip.Unimplemented(t, 0, "test's on CI don't have internet access")
	// Read the JSON file a the URL
	url := "https://download.microsoft.com/download/7/1/D/71D86715-5596-4529-9B13-DA13A5DE5B63/ServiceTags_Public_20240701.json"
	c := CIDRLookup{}
	c.transform = `
[ .values |  .[] | select(.properties.region != "") | .properties.region as $Name | .properties.addressPrefixes[] as $Ipnet | {$Name, $Ipnet} | select(.Ipnet | contains("::") | not) ]
`
	err := c.setURL(context.Background(), url)
	require.NoError(t, err)

	// Test Lookup
	require.Equal(t, "australiacentral", c.Lookup(net.ParseIP("20.213.228.121")))
	require.Equal(t, "", c.Lookup(net.ParseIP("10.213.228.121")))
}

func TestEmpty(t *testing.T) {
	settings := cluster.MakeClusterSettings()
	c := NewCIDRLookup(context.Background(), &settings.SV, stop.NewStopper())
	c.Lookup(net.ParseIP("127.0.0.1"))
}

// TestCIDRFile reads a file from the local filesystem.
func TestCIDRFile(t *testing.T) {
	skip.UnderStress(t)
	filename := filepath.Join(t.TempDir(), "file.json")

	file, err := os.Create(filename)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString(`[ {"Name": "loopback", "Ipnet": "127.0.0.0/24"} ]`)
	require.NoError(t, err)

	c := CIDRLookup{}
	err = c.setURL(context.Background(), "file://"+filename)
	require.NoError(t, err)

	require.Equal(t, "loopback", c.Lookup(net.ParseIP("127.0.0.1")))
}
