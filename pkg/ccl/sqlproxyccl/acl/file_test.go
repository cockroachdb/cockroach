// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDenyListFileParsing(t *testing.T) {
	t.Run("test custom marshal code", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		testutilsccl.ServerlessOnly(t)

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
		defer leaktest.AfterTest(t)()
		testutilsccl.ServerlessOnly(t)

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
		testutilsccl.ServerlessOnly(t)

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
			timeSource := timeutil.DefaultTimeSource{}
			controller, _, err := newAccessControllerFromFile[*Denylist](
				ctx, filename, timeSource, defaultPollingInterval, nil, func(c *Denylist) {
					c.timeSource = timeSource
				})
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
		defer leaktest.AfterTest(t)()
		testutilsccl.ServerlessOnly(t)

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
	testutilsccl.ServerlessOnly(t)

	startTime := time.Date(2021, 1, 1, 15, 20, 39, 0, time.UTC)
	longExpirationTimeString := "2030-01-01T15:30:39Z"
	shortExpirationTimeString := "2021-01-01T15:30:39Z"

	type denyIOSpec struct {
		connection ConnectionTags
		outcome    string
	}

	// This is a time evolution of a denylist.
	testCases := []struct {
		name   string
		input  string
		preRun func(timeSource *timeutil.ManualTime)
		specs  []denyIOSpec
	}{
		// Blocks IP address only.
		{
			"block_ip_address",
			fmt.Sprintf(`
SequenceNumber: 9
denylist:
- entity: {"item": "1.2.3.4", "type": "IP"}
  expiration: %s
  reason: over quota`,
				longExpirationTimeString,
			),
			nil,
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", roachpb.MustMakeTenantID(61), ""}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.1.1.1", roachpb.MustMakeTenantID(61), ""}, ""},
				{ConnectionTags{"1.2.3.5", roachpb.MustMakeTenantID(61), ""}, ""},
			},
		},
		// Blocks both IP address and tenant cluster.
		{
			"block_both_ip_address_and_tenant",
			fmt.Sprintf(`
SequenceNumber: 10
denylist:
- entity: {"item": "1.2.3.4", "type": "IP"}
  expiration: %s
  reason: over quota
- entity: {"item": 61, "type": "Cluster"}
  expiration: %s
  reason: splunk pipeline`,
				longExpirationTimeString,
				longExpirationTimeString,
			),
			nil,
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", roachpb.MustMakeTenantID(100), ""}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.2.3.4", roachpb.MustMakeTenantID(61), ""}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.1.1.1", roachpb.MustMakeTenantID(61), ""}, "connection cluster '61' denied: splunk pipeline"},
				{ConnectionTags{"1.2.3.5", roachpb.MustMakeTenantID(100), ""}, ""},
			},
		},
		// Entry without any expiration.
		{
			"entry_without_expiration",
			`
SequenceNumber: 11
denylist:
- entity: {"item": "1.2.3.4", "type": "ip"}
  reason: over quota`,
			nil,
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", roachpb.MustMakeTenantID(100), ""}, "connection ip '1.2.3.4' denied: over quota"},
				{ConnectionTags{"1.1.1.1", roachpb.MustMakeTenantID(61), ""}, ""},
				{ConnectionTags{"1.2.3.5", roachpb.MustMakeTenantID(100), ""}, ""},
			},
		},
		// Entry that has expired.
		{
			"expired_entry",
			fmt.Sprintf(`
SequenceNumber: 12
denylist:
- entity: {"item": "1.2.3.4", "type": "ip"}
  expiration: %s
  reason: over quota`,
				shortExpirationTimeString,
			),
			func(timeSource *timeutil.ManualTime) {
				// Move the manual time to a date that has expired.
				timeSource.AdvanceTo(startTime.Add(20 * time.Minute))
			},
			[]denyIOSpec{
				{ConnectionTags{"1.2.3.4", roachpb.MustMakeTenantID(100), ""}, ""},
				{ConnectionTags{"1.1.1.1", roachpb.MustMakeTenantID(61), ""}, ""},
				{ConnectionTags{"1.2.3.5", roachpb.MustMakeTenantID(100), ""}, ""},
			},
		},
	}

	// Use cancel to prevent leaked goroutines from file watches.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir := t.TempDir()
	filename := filepath.Join(tempDir, "denylist.yaml")
	require.NoError(t, os.WriteFile(filename, []byte("{}"), 0777))

	timeSource := timeutil.NewManualTime(startTime)
	loadInterval := 100 * time.Millisecond
	_, channel, err := newAccessControllerFromFile[*Denylist](
		ctx, filename, timeSource, loadInterval, nil, func(c *Denylist) { c.timeSource = timeSource },
	)
	require.NoError(t, err)

	validateSpecs := func(controller AccessController, spec []denyIOSpec) error {
		for _, ioPairs := range spec {
			err := controller.CheckConnection(ctx, ioPairs.connection)
			if ioPairs.outcome == "" {
				if err != nil {
					return errors.Newf("expected no error, but got %v", err) // nolint:errwrap
				}
			} else {
				if err.Error() != ioPairs.outcome {
					return errors.Newf("expected %v, but got %v", ioPairs.outcome, err) // nolint:errwrap
				}
			}
		}
		return nil
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Write the denylist file.
			require.NoError(t, os.WriteFile(filename, []byte(tc.input), 0777))

			if tc.preRun != nil {
				tc.preRun(timeSource)
			}

			// Keep advancing time source until we match all the specs.
			testutils.SucceedsSoon(t, func() error {
				timeSource.Advance(loadInterval)
				select {
				case controller := <-channel:
					return validateSpecs(controller, tc.specs)
				default:
					return errors.New("no data yet")
				}
			})
		})
	}
}

func parseIPNet(cidr string) *net.IPNet {
	_, ipNet, _ := net.ParseCIDR(cidr)
	return ipNet
}

func TestAllowListFileParsing(t *testing.T) {

	t.Run("test AllowEntry custom unmarshal code", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		testutilsccl.ServerlessOnly(t)

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
		testutilsccl.ServerlessOnly(t)

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
			controller, _, err := newAccessControllerFromFile[*Allowlist](
				ctx, filename, timeutil.DefaultTimeSource{}, defaultPollingInterval, nil, nil)
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
	testutilsccl.ServerlessOnly(t)

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
				{ConnectionTags{"1.2.3.4", roachpb.MustMakeTenantID(100), ""}, ""},
				{ConnectionTags{"1.1.1.1", roachpb.MustMakeTenantID(61), ""}, "connection ip '1.1.1.1' denied: ip address not allowed"},
				{ConnectionTags{"1.2.1.1", roachpb.MustMakeTenantID(61), ""}, ""},
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
		ctx, filename, timeutil.DefaultTimeSource{}, 100*time.Millisecond, nil, nil)
	require.NoError(t, err)
	for _, tc := range testCases {
		require.NoError(t, os.WriteFile(filename, []byte(tc.input), 0777))

		controller := <-channel
		for _, ioPairs := range tc.specs {
			err := controller.CheckConnection(ctx, ioPairs.connection)
			if ioPairs.outcome == "" {
				require.Nil(t, err)
			} else {
				require.EqualError(t, err, ioPairs.outcome)
			}
		}
	}
}

func TestParsingErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	// Use cancel to prevent leaked goroutines from file watches.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir := t.TempDir()

	t.Run("error on initial file parse", func(t *testing.T) {
		errorCountMetric := metric.NewGauge(metric.Metadata{})

		filename := filepath.Join(tempDir, "error_file.idk")
		require.NoError(t, os.WriteFile(filename, []byte("not yaml"), 0777))

		_, _, err := newAccessControllerFromFile[*Allowlist](
			ctx, filename, timeutil.DefaultTimeSource{}, 100*time.Millisecond, errorCountMetric, nil)

		require.Error(t, err)
		require.ErrorContains(t, err, "error when creating access controller from file")
		errorCount := errorCountMetric.Value()
		require.Equal(t, int64(0), errorCount)
	})

	t.Run("error after update", func(t *testing.T) {
		errorCountMetric := metric.NewGauge(metric.Metadata{})

		// Create access controller and watcher with a valid file
		filename := filepath.Join(tempDir, "allowlist.yaml")
		require.NoError(t, os.WriteFile(filename, []byte(`{"allowlist":{"tenant":{"ips": ["1.1.1.1/32"]}}}`), 0777))

		controller, next, err := newAccessControllerFromFile[*Allowlist](
			ctx, filename, timeutil.DefaultTimeSource{}, 100*time.Millisecond, errorCountMetric, nil)

		require.NoError(t, err)
		require.NotNil(t, next)
		require.Equal(t, int64(0), errorCountMetric.Value())
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

		testutils.SucceedsSoon(t, func() error {
			select {
			case <-next:
				t.Fatal("should not have gotten a new controller")
				// We need to return something to make the compiler happy, but t.Fatal will end execution.
				return nil
			default:
				errorCount := errorCountMetric.Value()
				// If error count isn't one, then it hasn't happened yet.
				if errorCount != 1 {
					return fmt.Errorf("Expected error count to be 1 but got %d", errorCount)
				}

				return nil
			}
		})

		// Update with valid data now
		require.NoError(t, os.WriteFile(filename, []byte(`{"allowlist":{"tenant":{"ips": ["2.2.2.2/32"]}}}`), 0777))
		testutils.SucceedsSoon(t, func() error {
			select {
			case controller := <-next:
				// error count should go down
				require.Equal(t, int64(0), errorCountMetric.Value())
				allowlist := controller.(*Allowlist)
				require.Equal(t, map[string]AllowEntry{
					"tenant": {
						ips: []*net.IPNet{
							parseIPNet("2.2.2.2/32"),
						},
					},
				}, allowlist.entries)
				return nil
			default:
				return errors.New("new controller not created yet")
			}
		})
	})
}
