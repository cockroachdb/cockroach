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
	c := Lookup{}

	destinations := []cidr{
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
			actual := c.LookupIP(net.ParseIP(test.ip))
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestEmpty(t *testing.T) {
	settings := cluster.MakeClusterSettings()
	c := NewLookup(context.Background(), &settings.SV, stop.NewStopper())
	c.LookupIP(net.ParseIP("127.0.0.1"))
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

	c := Lookup{}
	err = c.setURL(context.Background(), "file://"+filename)
	require.NoError(t, err)

	require.Equal(t, "loopback", c.LookupIP(net.ParseIP("127.0.0.1")))
}
