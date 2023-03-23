// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDenyListFileParsing(t *testing.T) {
	t.Run("test custom marshal code", func(t *testing.T) {
		cases := []struct {
			t        DenyType
			expected string
		}{
			{IPAddrType, "ip"},
			{ClusterType, "cluster"},
		}
		for _, tc := range cases {
			s, err := tc.t.MarshalYAML()
			require.NoError(t, err)
			require.Equal(t, tc.expected, s)
		}
	})

	t.Run("test DenyType custom unmarshal code", func(t *testing.T) {
		cases := []struct {
			raw      string
			expected DenyType
		}{
			{"ip", IPAddrType},
			{"IP", IPAddrType},
			{"Ip", IPAddrType},
			{"Cluster", ClusterType},
			{"cluster", ClusterType},
			{"CLUSTER", ClusterType},
			{"random text", UnknownType},
		}
		for _, tc := range cases {
			var parsed DenyType
			err := yaml.UnmarshalStrict([]byte(tc.raw), &parsed)
			require.NoError(t, err)
			require.Equal(t, tc.expected, parsed)
		}
	})

	t.Run("end to end testing of DenylistFile parsing", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		expirationTimeString := "2021-01-01T15:20:39Z"
		expirationTime := time.Date(2021, 1, 1, 15, 20, 39, 0, time.UTC)

		emptyMap := make(map[DenyEntity]*DenyEntry)

		testCases := []struct {
			input    string
			expected map[DenyEntity]*DenyEntry
		}{
			{"text: ", emptyMap},
			{"SequenceNumber: 0", emptyMap},
			{"SequenceNumber: 7", emptyMap},
			{
				// Old denylist format; making sure it won't break new denylist code.
				`
SequenceNumber: 8
1.1.1.1: some reason
61: another reason`,
				emptyMap,
			},
			{
				fmt.Sprintf(`
SequenceNumber: 9
denylist:
- entity: {"item":"1.2.3.4", "type": "ip"}
  expiration: %s
  reason: over quota`,
					expirationTimeString,
				),
				map[DenyEntity]*DenyEntry{
					{"1.2.3.4", IPAddrType}: {
						DenyEntity{"1.2.3.4", IPAddrType},
						expirationTime,
						"over quota",
					},
				},
			},
		}

		// use cancel to prevent leaked goroutines from file watches
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tempDir := t.TempDir()
		for i, tc := range testCases {
			filename := filepath.Join(tempDir, fmt.Sprintf("denylist%d.yaml", i))
			require.NoError(t, os.WriteFile(filename, []byte(tc.input), 0777))
			controller, _, err := newAccessControllerFromFile[*Denylist](ctx, filename, defaultPollingInterval, nil)
			require.NoError(t, err)
			entries := emptyMap
			if controller != nil {
				dl := controller.(*Denylist)
				entries = dl.entries
			}
			require.Equal(t, tc.expected, entries, "should return expected parsed file for %s",
				tc.input)
		}
	})

	t.Run("test Ser/De of File", func(t *testing.T) {
		file := DenylistFile{
			Seq: 72,
			Denylist: []*DenyEntry{
				{
					DenyEntity{"63", ClusterType},
					timeutil.NowNoMono(),
					"over usage",
				},
				{
					DenyEntity{"8.8.8.8", IPAddrType},
					timeutil.NowNoMono().Add(1 * time.Hour),
					"malicious IP",
				},
			},
		}

		raw, err := yaml.Marshal(file)
		require.NoError(t, err)
		deserialized, err := Deserialize[DenylistFile](bytes.NewBuffer(raw))
		require.NoError(t, err)
		require.EqualValues(t, file, deserialized)
	})
}

func TestDenylistLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startTime := time.Date(2021, 1, 1, 15, 20, 39, 0, time.UTC)
	expirationTimeString := "2021-01-01T15:30:39Z"
	futureTime := startTime.Add(time.Minute * 20)

	type denyIOSpec struct {
		connection ConnectionTags
		outcome    string
	}

	// This is a time evolution of a denylist.
	testCases := []struct {
		input string
		time  time.Time
		specs []denyIOSpec
	}{
		// Blocks IP address only.
		{
			fmt.Sprintf(`
SequenceNumber: 9
denylist:
- entity: {"item": "1.2.3.4", "type": "IP"}
  expiration: %s
  reason: over quota`,
				expirationTimeString,
			),
			startTime.Add(10 * time.Second),
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", "foo"}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.1.1.1", "61"}, ""},
				{ConnectionTags{"1.2.3.5", "foo"}, ""},
			},
		},
		// Blocks both IP address and tenant cluster.
		{
			fmt.Sprintf(`
SequenceNumber: 10
denylist:
- entity: {"item": "1.2.3.4", "type": "IP"}
  expiration: %s
  reason: over quota
- entity: {"item": 61, "type": "Cluster"}
  expiration: %s
  reason: splunk pipeline`,
				expirationTimeString,
				expirationTimeString,
			),
			startTime.Add(20 * time.Second),
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", "foo"}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.2.3.4", "61"}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.1.1.1", "61"}, "connection cluster '61' denied: splunk pipeline"},
				{ConnectionTags{"1.2.3.5", "foo"}, ""},
			},
		},
		// Entry that has expired.
		{
			fmt.Sprintf(`
SequenceNumber: 11
denylist:
- entity: {"item": "1.2.3.4", "type": "ip"}
  expiration: %s
  reason: over quota`,
				expirationTimeString,
			),
			futureTime,
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", "foo"}, ""},
				{ConnectionTags{"1.1.1.1", "61"}, ""},
				{ConnectionTags{"1.2.3.5", "foo"}, ""},
			},
		},
		// Entry without any expiration.
		{
			`
SequenceNumber: 11
denylist:
- entity: {"item": "1.2.3.4", "type": "ip"}
  reason: over quota`,
			futureTime,
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", "foo"}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.1.1.1", "61"}, ""},
				{ConnectionTags{"1.2.3.5", "foo"}, ""},
			},
		},
	}
	// Use cancel to prevent leaked goroutines from file watches.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempDir := t.TempDir()

	filename := filepath.Join(tempDir, "denylist.yaml")
	require.NoError(t, os.WriteFile(filename, []byte("{}"), 0777))
	manualTime := timeutil.NewManualTime(startTime)
	_, channel, err := newAccessControllerFromFile[*Denylist](
		ctx, filename, 100*time.Millisecond, nil)
	require.NoError(t, err)
	for _, tc := range testCases {
		require.NoError(t, os.WriteFile(filename, []byte(tc.input), 0777))
		manualTime.AdvanceTo(tc.time)
		controller := <-channel
		for _, ioPairs := range tc.specs {
			err := controller.CheckConnection(ioPairs.connection, manualTime)
			if ioPairs.outcome == "" {
				require.Nil(t, err)
			} else {
				require.EqualError(t, err, ioPairs.outcome)
			}
		}
	}
}

func parseIPNet(cidr string) *net.IPNet {
	_, ipNet, _ := net.ParseCIDR(cidr)
	return ipNet
}

func TestAllowListFileParsing(t *testing.T) {

	t.Run("test AllowEntry custom unmarshal code", func(t *testing.T) {
		cases := []struct {
			raw      string
			expected AllowEntry
		}{
			// invalid ip entry
			{`{"ips": ["1.1.1.1"]}`, AllowEntry{
				ips: []*net.IPNet{},
			}},
			// one valid one invalid
			{`{"ips": ["1.1.1.1", "1.1.1.1/0"]}`, AllowEntry{
				ips: []*net.IPNet{
					{
						IP:   net.IP{0, 0, 0, 0},
						Mask: net.IPMask{0, 0, 0, 0},
					},
				},
			}},
			// different kinds of CIDR ranges
			{`{"ips": ["1.1.1.1/0"]}`, AllowEntry{
				ips: []*net.IPNet{
					{
						IP:   net.IP{0, 0, 0, 0},
						Mask: net.IPMask{0, 0, 0, 0},
					},
				},
			}},
			{`{"ips": ["1.1.1.1/16"]}`, AllowEntry{
				ips: []*net.IPNet{
					{
						IP:   net.IP{1, 1, 0, 0},
						Mask: net.IPMask{255, 255, 0, 0},
					},
				},
			}},
			{`{"ips": ["1.1.1.1/32"]}`, AllowEntry{
				ips: []*net.IPNet{
					{
						IP:   net.IP{1, 1, 1, 1},
						Mask: net.IPMask{255, 255, 255, 255},
					},
				},
			}},
		}
		for _, tc := range cases {
			var parsed AllowEntry
			err := yaml.UnmarshalStrict([]byte(tc.raw), &parsed)
			require.NoError(t, err)
			require.Equal(t, tc.expected, parsed)
		}
	})

	t.Run("end to end testing of AllowlistFile parsing", func(t *testing.T) {
		defer leaktest.AfterTest(t)()

		testCases := []struct {
			input    string
			expected map[string]AllowEntry
		}{
			{"text: ", nil},
			{"SequenceNumber: 0", nil},
			{"SequenceNumber: 7", nil},
			{
				`
SequenceNumber: 9
allowlist:
  "61":
    ips: ["1.2.3.4/16"]				

`,
				map[string]AllowEntry{
					"61": {
						ips: []*net.IPNet{
							parseIPNet("1.2.3.4/16"),
						},
					},
				},
			},
			{
				`
SequenceNumber: 9
allowlist:
  "61":
    ips: ["4.3.2.1/16"]				
  "1357":
    ips: ["44.22.33.11/19", "not-an-ip-address", "12.34.56.78/5"]

`,
				map[string]AllowEntry{
					"61": {
						ips: []*net.IPNet{
							parseIPNet("4.3.2.1/16"),
						},
					},
					"1357": {
						ips: []*net.IPNet{
							parseIPNet("44.22.33.11/19"),
							parseIPNet("12.34.56.78/5"),
						},
					},
				},
			},
		}

		// use cancel to prevent leaked goroutines from file watches
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tempDir := t.TempDir()
		for i, tc := range testCases {
			filename := filepath.Join(tempDir, fmt.Sprintf("allowlist%d.yaml", i))
			require.NoError(t, os.WriteFile(filename, []byte(tc.input), 0777))
			controller, _, err := newAccessControllerFromFile[*Allowlist](ctx, filename, defaultPollingInterval, nil)
			require.NoError(t, err)
			dl := controller.(*Allowlist)
			var entries map[string]AllowEntry
			if dl != nil {
				entries = dl.entries
			}
			require.Equal(t, tc.expected, entries, "should return expected parsed file for %s",
				tc.input)
		}
	})

}

func TestAllowlistLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type allowIOSpec struct {
		connection ConnectionTags
		outcome    string
	}

	testCases := []struct {
		input string
		specs []allowIOSpec
	}{
		{
			`
SequenceNumber: 9
allowlist:
  "61":
    ips: ["1.2.3.4/16"]
`,
			[]allowIOSpec{
				{ConnectionTags{"1.2.3.4", "foo"}, ""},
				{ConnectionTags{"1.1.1.1", "61"}, "connection ip '1.1.1.1' denied: ip address not allowed"},
				{ConnectionTags{"1.2.1.1", "61"}, ""},
			},
		},
	}
	// Use cancel to prevent leaked goroutines from file watches.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempDir := t.TempDir()

	filename := filepath.Join(tempDir, "allowlist.yaml")
	require.NoError(t, os.WriteFile(filename, []byte("{}"), 0777))
	_, channel, err := newAccessControllerFromFile[*Allowlist](
		ctx, filename, 100*time.Millisecond, nil)
	require.NoError(t, err)
	for _, tc := range testCases {
		require.NoError(t, os.WriteFile(filename, []byte(tc.input), 0777))

		controller := <-channel
		for _, ioPairs := range tc.specs {
			err := controller.CheckConnection(ioPairs.connection, nil)
			if ioPairs.outcome == "" {
				require.Nil(t, err)
			} else {
				require.EqualError(t, err, ioPairs.outcome)
			}
		}
	}
}

func TestParsingErrorHandling(t *testing.T) {
	// Use cancel to prevent leaked goroutines from file watches.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir := t.TempDir()

	t.Run("error on initial file parse", func(t *testing.T) {
		errorCountMetric := metric.NewGauge(metric.Metadata{})

		filename := filepath.Join(tempDir, "error_file.idk")
		require.NoError(t, os.WriteFile(filename, []byte("not yaml"), 0777))

		_, _, err := newAccessControllerFromFile[*Allowlist](
			ctx, filename, 100*time.Millisecond, errorCountMetric)

		require.Error(t, err)
		require.ErrorContains(t, err, "error when creating access controller from file")
		errorCount := errorCountMetric.Snapshot().Value()
		require.Equal(t, int64(0), errorCount)
	})

	t.Run("error after update", func(t *testing.T) {
		errorCountMetric := metric.NewGauge(metric.Metadata{})
		// This is the maximum amount of time we are willing to wait for the error metric to be updated.
		// We check the value every 100ms, but will stop waiting after this duration.
		maxWaitTime := time.Second * 10

		// Create access controller and watcher with a valid file
		filename := filepath.Join(tempDir, "allowlist.yaml")
		require.NoError(t, os.WriteFile(filename, []byte(`{"allowlist":{"tenant":{"ips": ["1.1.1.1/32"]}}}`), 0777))

		controller, next, err := newAccessControllerFromFile[*Allowlist](
			ctx, filename, 100*time.Millisecond, errorCountMetric)

		require.NoError(t, err)
		require.NotNil(t, next)
		require.Equal(t, int64(0), errorCountMetric.Snapshot().Value())
		allowlist := controller.(*Allowlist)
		require.Equal(t, map[string]AllowEntry{
			"tenant": {
				ips: []*net.IPNet{
					parseIPNet("1.1.1.1/32"),
				},
			},
		}, allowlist.entries)

		// Update with invalid data
		require.NoError(t, os.WriteFile(filename, []byte(`no longer valid yaml`), 0777))
		timeout := time.After(maxWaitTime)
	loop:
		for {
			select {
			case <-next:
				t.Error("should not have gotten a new controller")
			case <-time.After(time.Millisecond * 100):
				// error count should go up
				if errorCountMetric.Snapshot().Value() == 1 {
					// If the value went up, we can stop the loop!
					break loop
				}
			case <-timeout:
				t.Fatalf("timed out after %s waiting for error metric to be updated", maxWaitTime)
			}
		}

		// Update with valid data now
		require.NoError(t, os.WriteFile(filename, []byte(`{"allowlist":{"tenant":{"ips": ["2.2.2.2/32"]}}}`), 0777))
		select {
		case controller := <-next:
			// error count should go down
			require.Equal(t, int64(0), errorCountMetric.Snapshot().Value())
			allowlist := controller.(*Allowlist)
			require.Equal(t, map[string]AllowEntry{
				"tenant": {
					ips: []*net.IPNet{
						parseIPNet("2.2.2.2/32"),
					},
				},
			}, allowlist.entries)
		case <-time.After(maxWaitTime):
			t.Error("should have gotten a new controller")
		}
	})
}
