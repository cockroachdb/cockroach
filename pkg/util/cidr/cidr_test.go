// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cidr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestCIDRLookup tests that the CIDR lookup works as expected when there are
// overlapping matching entries.
func TestCIDRLookup(t *testing.T) {
	c := Lookup{}

	destinations := `[
		{ "Name":  "CIDR1", "Ipnet": "192.168.0.0/24" },
		{ "Name":  "CIDR2", "Ipnet": "192.168.0.0/25" },
		{ "Name":  "CIDR3", "Ipnet": "10.0.0.0/16" },
		{ "Name":  "CIDR4", "Ipnet": "10.0.0.1/32" }
		]`
	require.NoError(t, c.setDestinations(context.Background(), []byte(destinations)))

	testCases := []struct {
		ip       string
		expected string
	}{
		{"192.168.0.200", "CIDR1"},
		{"192.168.0.2", "CIDR2"},
		{"10.0.0.2", "CIDR3"},
		{"10.0.0.1", "CIDR4"},
		{"172.16.0.1", ""},
		{"2001:0db8:0a0b:12f0:0000:0000:0000:0001", ""},
	}
	for _, tc := range testCases {
		t.Run(tc.ip, func(t *testing.T) {
			actual := c.LookupIP(net.ParseIP(tc.ip))
			require.Equal(t, tc.expected, actual)
		})
	}
}

// TestValidCIDR tests that valid CIDR entries are accepted.
func TestValidCIDR(t *testing.T) {
	testCases := []struct {
		name  string
		value string
	}{
		{"basic", `[ { "Name": "Name", "Ipnet": "192.168.0.0/24" } ]`},
		{"unicode", `[ { "Name": "ABC€", "Ipnet": "192.168.0.0/24" } ]`},
		{"extra", `[ { "Name": "Name", "Ipnet": "192.168.0.0/24", "Other": "Foo" } ]`},
	}
	c := Lookup{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, c.setDestinations(context.Background(), []byte(tc.value)))
		})
	}
}

// TestInvalidCIDR tests that invalid CIDR entries are rejected.
func TestInvalidCIDR(t *testing.T) {
	testCases := []struct {
		name  string
		value string
	}{
		{"garbage", "garbage"},
		{"missing quotes", `[ { Name:  "CIDR1", Ipnet: "192.168.0.0/24" } ]`},
		{"int name ", `[ { "Name":  1, "Ipnet": "192.168.0.0/24" } ]`},
		{"missing cidr", `[ { Name:  "CIDR1" } ]`},
		{"malformed cidr", `[ { "Name":  "CIDR1", "Ipnet": "192.168.0.0.1/24" } ]`},
		{"ipv6", `[ { "Name":  "CIDR1", "Ipnet": "2001:db8::/40" } ]`},
	}
	c := Lookup{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, c.setDestinations(context.Background(), []byte(tc.value)))
		})
	}
}

// TestEmptyLookup tests that the lookup returns an empty string when no lookups
// are defined.
func TestEmptyLookup(t *testing.T) {
	settings := cluster.MakeClusterSettings()
	c := NewLookup(&settings.SV)
	require.NoError(t, c.Start(context.Background(), stop.NewStopper()))
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

// TestRefresh tests that the CIDR lookup is refreshed when the URL is changed.
func TestRefresh(t *testing.T) {
	skip.UnderStress(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	filename := filepath.Join(t.TempDir(), "file.json")

	file, err := os.Create(filename)
	require.NoError(t, err)
	_, err = file.WriteString(`[ {"Name": "loopback", "Ipnet": "127.0.0.0/24"} ]`)
	require.NoError(t, err)
	file.Close()

	st := cluster.MakeClusterSettings()
	c := NewLookup(&st.SV)
	require.NoError(t, c.Start(context.Background(), stopper))
	// We haven't set the URL yet, so it should return an empty string.
	require.Equal(t, "", c.LookupIP(net.ParseIP("127.0.0.1")))

	// Set the URL to the file we created. Verify it takes effect quickly.
	cidrMappingUrl.Override(context.Background(), &st.SV, "file://"+filename)
	testutils.SucceedsSoon(t, func() error {
		if c.LookupIP(net.ParseIP("127.0.0.1")) != "loopback" {
			return errors.New("not refreshed")
		}
		return nil
	})

	cidrRefreshInterval.Override(context.Background(), &st.SV, time.Second)

	file2, err := os.Create(filename)
	require.NoError(t, err)
	_, err = file2.WriteString(`[ {"Name": "other", "Ipnet": "127.0.0.0/24"} ]`)
	require.NoError(t, err)
	file.Close()

	testutils.SucceedsSoon(t, func() error {
		if c.LookupIP(net.ParseIP("127.0.0.1")) != "other" {
			// Touch the file to ensure the file modification time changes.
			require.NoError(t, os.Chtimes(filename, timeutil.Now(), timeutil.Now()))
			return errors.New("not refreshed")
		}
		return nil
	})
}

var writeBytes = metric.Metadata{
	Name:        "write_bytes",
	Help:        "Number of bytes written",
	Measurement: "Bytes",
	Unit:        metric.Unit_BYTES,
}
var readBytes = metric.Metadata{
	Name:        "read_bytes",
	Help:        "Number of bytes read",
	Measurement: "Bytes",
	Unit:        metric.Unit_BYTES,
}

// TestWrapHTTP validates the metrics for a HTTP connections.
func TestWrapHTTP(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer s.Close()
	// Create a mapping for this server's IP.
	mapping := fmt.Sprintf(`[ { "Name": "test", "Ipnet": "%s/32" } ]`, s.Listener.Addr().(*net.TCPAddr).IP.String())
	c := Lookup{}
	require.NoError(t, c.setDestinations(context.Background(), []byte(mapping)))

	// This is the standard way to wrap the transport.
	m := c.MakeNetMetrics(writeBytes, readBytes, "label")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = m.Wrap(transport.DialContext, "foo")

	// Execute a simple get request.
	client := &http.Client{Transport: transport}
	_, err := client.Get(s.URL)
	require.NoError(t, err)

	// Ideally we could check the actual value, but the header includes the date
	// and could be flaky.
	require.Greater(t, m.WriteBytes.Count(), int64(1))
	require.Greater(t, m.ReadBytes.Count(), int64(1))
	// Also check the child metrics by looking up in the map directly.
	m.mu.Lock()
	defer m.mu.Unlock()
	require.Greater(t, m.mu.childMetrics["foo/test"].WriteBytes.Value(), int64(1))
	require.Greater(t, m.mu.childMetrics["foo/test"].ReadBytes.Value(), int64(1))
}

func TestWrapDialer(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer s.Close()
	// Create a mapping for this server's IP.
	mapping := fmt.Sprintf(`[ { "Name": "test", "Ipnet": "%s/32" } ]`, s.Listener.Addr().(*net.TCPAddr).IP.String())
	c := Lookup{}
	require.NoError(t, c.setDestinations(context.Background(), []byte(mapping)))

	m := c.MakeNetMetrics(writeBytes, readBytes, "label")
	dialer := m.WrapDialer(&net.Dialer{}, "foo")
	conn, err := dialer.Dial(s.Listener.Addr().Network(), s.Listener.Addr().String())
	require.NoError(t, err)
	_, err = conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	require.NoError(t, err)
	var b [1024]byte
	_, err = conn.Read(b[:])
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// Ideally we could check the actual value, but the header includes the date
	// and could be flaky.
	require.Greater(t, m.WriteBytes.Count(), int64(1))
	require.Greater(t, m.ReadBytes.Count(), int64(1))
	// Also check the child metrics by looking up in the map directly.
	m.mu.Lock()
	defer m.mu.Unlock()
	require.Greater(t, m.mu.childMetrics["foo/test"].WriteBytes.Value(), int64(1))
	require.Greater(t, m.mu.childMetrics["foo/test"].ReadBytes.Value(), int64(1))
}
