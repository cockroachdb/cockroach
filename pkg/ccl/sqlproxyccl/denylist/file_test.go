// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDenyListFileParsing(t *testing.T) {
	t.Run("test custom marshal code", func(t *testing.T) {
		cases := []struct {
			t        Type
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

	t.Run("test custom unmarshal code", func(t *testing.T) {
		cases := []struct {
			raw      string
			expected Type
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
			var parsed Type
			err := yaml.UnmarshalStrict([]byte(tc.raw), &parsed)
			require.NoError(t, err)
			require.Equal(t, tc.expected, parsed)
		}
	})

	t.Run("end to end testing of file parsing", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		expirationTimeString := "2021-01-01T15:20:39Z"
		expirationTime := time.Date(2021, 1, 1, 15, 20, 39, 0, time.UTC)

		emptyMap := make(map[DenyEntity]*DenyEntry)

		testCases := []struct {
			input    string
			expected map[DenyEntity]*DenyEntry
		}{
			{"text: ", emptyMap},
			{"random text\n\n\nmore random text", emptyMap},
			{defaultEmptyDenylistText, emptyMap},
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
			require.NoError(t, ioutil.WriteFile(filename, []byte(tc.input), 0777))
			dl := NewDenylistWithFile(ctx, filename)
			require.Equal(t, tc.expected, dl.mu.entries, "should return expected parsed file for %s",
				tc.input)
		}
	})

	t.Run("test Ser/De of File", func(t *testing.T) {
		file := File{
			Seq: 72,
			Denylist: []*DenyEntry{
				{
					DenyEntity{"63", ClusterType},
					timeutil.Now(),
					"over usage",
				},
				{
					DenyEntity{"8.8.8.8", IPAddrType},
					timeutil.Now().Add(1 * time.Hour),
					"malicious IP",
				},
			},
		}

		raw, err := file.Serialize()
		require.NoError(t, err)
		deserialized, err := Deserialize(bytes.NewBuffer(raw))
		require.NoError(t, err)
		require.EqualValues(t, file, *deserialized)
	})
}

func TestDenylistLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startTime := time.Date(2021, 1, 1, 15, 20, 39, 0, time.UTC)
	expirationTimeString := "2021-01-01T15:30:39Z"
	futureTime := startTime.Add(time.Minute * 20)

	type denyIOSpec struct {
		entity  DenyEntity
		outcome *Entry
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
				{DenyEntity{"1.2.3.4", IPAddrType}, &Entry{"over quota"}},
				{DenyEntity{"61", ClusterType}, nil},
				{DenyEntity{"1.2.3.5", IPAddrType}, nil},
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
				{DenyEntity{"1.2.3.4", IPAddrType}, &Entry{"over quota"}},
				{DenyEntity{"61", ClusterType}, &Entry{"splunk pipeline"}},
				{DenyEntity{"1.2.3.5", IPAddrType}, nil},
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
				{DenyEntity{"1.2.3.4", IPAddrType}, nil},
				{DenyEntity{"61", ClusterType}, nil},
				{DenyEntity{"1.2.3.5", IPAddrType}, nil},
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
				{DenyEntity{"1.2.3.4", IPAddrType}, &Entry{"over quota"}},
				{DenyEntity{"61", ClusterType}, nil},
				{DenyEntity{"1.2.3.5", IPAddrType}, nil},
			},
		},
	}
	// Use cancel to prevent leaked goroutines from file watches.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempDir := t.TempDir()

	filename := filepath.Join(tempDir, "denylist.yaml")
	manualTime := timeutil.NewManualTime(startTime)
	dl := NewDenylistWithFile(ctx, filename, WithPollingInterval(100*time.Millisecond))
	dl.timeSource = manualTime
	for _, tc := range testCases {
		require.NoError(t, ioutil.WriteFile(filename, []byte(tc.input), 0777))
		manualTime.AdvanceTo(tc.time)
		time.Sleep(500 * time.Millisecond)
		for _, ioPairs := range tc.specs {
			actual, err := dl.Denied(ioPairs.entity)
			require.NoError(t, err)
			require.Equal(t, ioPairs.outcome, actual)
		}
	}
}
