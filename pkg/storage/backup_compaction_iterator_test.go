// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBackupCompactionIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pebble, err := Open(context.Background(), InMemory(),
		cluster.MakeTestingClusterSettings(), CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer pebble.Close()

	// The test turns each `input` into a batch for the readAsOfIterator, fully
	// iterates the iterator, and puts the surfaced keys into a string in the same
	// format as `input`. The test then compares the output to 'expectedNextKey'.
	// The 'asOf' field represents the wall time of the hlc.Timestamp for the
	// readAsOfIterator.
	tests := []asOfTest{
		// Ensure vanilla iteration works, surfacing latest values with no AOST.
		{input: "a1b1", expectedNextKey: "a1b1", asOf: ""},
		{input: "a2a1", expectedNextKey: "a2", asOf: ""},
		{input: "a1b2b1", expectedNextKey: "a1b2", asOf: ""},
		{input: "a2Xa1", expectedNextKey: "a2X", asOf: ""},
		{input: "a1b2Xb1", expectedNextKey: "a1b2X", asOf: ""},

		// Ensure vanilla iterations works with provided AOST
		{input: "a1b1", expectedNextKey: "a1b1", asOf: "1"},
		{input: "a1b1", expectedNextKey: "a1b1", asOf: "2"},
		{input: "a1b2b1", expectedNextKey: "a1b2", asOf: "2"},
		{input: "a1b1X", expectedNextKey: "a1b1X", asOf: "2"},
		{input: "a1b2Xb1", expectedNextKey: "a1b2X", asOf: "2"},

		// Skipping keys with AOST.
		{input: "a2a1", expectedNextKey: "a1", asOf: "1"},
		{input: "a1b2b1", expectedNextKey: "a1b1", asOf: "1"},

		// Double skip within keys.
		{input: "b3b2b1", expectedNextKey: "b1", asOf: "1"},

		// Double skip across keys.
		{input: "b2c2c1", expectedNextKey: "c1", asOf: "1"},

		// Skipping tombstones with AOST.
		{input: "a1b2Xb1", expectedNextKey: "a1b1", asOf: "1"},
		{input: "a2Xa1b2Xb1c2Xc1", expectedNextKey: "a1b1c1", asOf: "1"},

		// Skipping under tombstone to land on another tombstone
		{input: "a2Xa1b2b1X", expectedNextKey: "a1b1X", asOf: "1"},

		// Ensure next key captures at most one mvcc key per key after an asOf skip.
		{input: "b3c2c1", expectedNextKey: "c2", asOf: "2"},

		// Ensure clean iteration over double tombstone.
		{input: "a1Xb2Xb1c1", expectedNextKey: "a1Xb2Xc1", asOf: ""},
		{input: "a1Xb2Xb1c1", expectedNextKey: "a1Xb1c1", asOf: "1"},

		// Ensure key before delete tombstone gets read if under AOST.
		{input: "b2b1Xc1", expectedNextKey: "b2c1", asOf: ""},
		{input: "b2b1Xc1", expectedNextKey: "b2c1", asOf: "2"},
	}

	for i, test := range tests {
		name := fmt.Sprintf("Test %d: %s, AOST %s", i, test.input, test.asOf)
		t.Run(name, func(t *testing.T) {
			batch := pebble.NewBatch()
			defer batch.Close()
			populateBatch(t, batch, test.input)
			iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
			require.NoError(t, err)
			defer iter.Close()

			subtests := []iterSubtest{
				{"Next", test.expectedNextKey, SimpleMVCCIterator.Next},
				{"NextKey", test.expectedNextKey, SimpleMVCCIterator.NextKey},
			}
			for _, subtest := range subtests {
				t.Run(subtest.name, func(t *testing.T) {
					asOf := hlc.Timestamp{}
					if test.asOf != "" {
						asOf.WallTime = int64(test.asOf[0])
					} else {
						asOf = hlc.MaxTimestamp
					}
					it, err := NewBackupCompactionIterator(iter, asOf)
					require.NoError(t, err)
					iterateSimpleMVCCIterator(t, it, subtest)
				})
			}
		})
	}
}

func TestBackupCompactionIteratorSeek(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pebble, err := Open(context.Background(), InMemory(),
		cluster.MakeTestingClusterSettings(), CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer pebble.Close()

	tests := []struct {
		input    string
		seekKey  string
		expected string
		asOf     string
	}{
		// Ensure vanilla seek works.
		{"a1b1", "a1", "a1", ""},

		// Ensure seek always returns the latest key AOST of an MVCC key.
		{"a2a1b1", "a1", "b1", ""},
		{"a2a1b1", "a1", "b1", "2"},
		{"a2a1b1", "a1", "a1", "1"},

		// Seeking above all keys will return the latest key AOST of an MVCC key.
		{"a2a1b3", "a8", "a2", ""},
		{"a2a1b3", "a8", "a1", "1"},
		{"a2a1b3X", "b8", "b3X", ""},
		{"a2a1b3Xb2", "b8", "b2", "2"},

		// Ensure out of bounds seek fails gracefully.
		{"a1", "b1", "notOK", ""},

		// Ensure the asOf timestamp moves the iterator during a seek.
		{"a2a1", "a2", "a1", "1"},
		{"a2b1", "a2", "b1", "1"},

		// Ensure seek will return on a tombstone if it is the latest key.
		{"a3Xa1b1", "a3", "a3X", ""},
		{"a3Xa1c2Xc1", "b1", "c2X", ""},

		// Ensure seek will only return the latest key AOST
		{"a3Xa2a1b1", "a2", "b1", ""},
		{"a3Xa2a1b1", "a2", "b1", "3"},
		{"a3a2Xa1b1", "a1", "b1", ""},
		{"a3a2Xa1b2Xb1c1", "a1", "b2X", ""},

		// Ensure we can seek to a key right before a tombstone.
		{"a2Xa1b2b1Xc1", "a1", "b2", ""},

		// Ensure seek on a key above the AOST returns the correct key AOST.
		{"a3a2Xa1b3Xb2b1", "b3", "b2", "2"},
		{"a3a2Xa1b3Xb2b1", "b3", "b1", "1"},

		// Ensure seeking on a key on AOST returns that key
		{"a3a2Xa1b3Xb2b1", "a2", "a2X", "2"},
		{"a3a2Xa1b3Xb2b1", "b2", "b2", "2"},
	}
	for i, test := range tests {
		name := fmt.Sprintf("Test %d: %s, AOST %s", i, test.input, test.asOf)
		t.Run(name, func(t *testing.T) {
			batch := pebble.NewBatch()
			defer batch.Close()
			populateBatch(t, batch, test.input)
			iter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
			require.NoError(t, err)
			defer iter.Close()

			asOf := hlc.Timestamp{}
			if test.asOf != "" {
				asOf.WallTime = int64(test.asOf[0])
			} else {
				asOf = hlc.MaxTimestamp
			}
			it, err := NewBackupCompactionIterator(iter, asOf)
			require.NoError(t, err)
			var output bytes.Buffer

			seekKey := MVCCKey{
				Key:       []byte{test.seekKey[0]},
				Timestamp: hlc.Timestamp{WallTime: int64(test.seekKey[1])},
			}
			it.SeekGE(seekKey)
			ok, err := it.Valid()
			require.NoError(t, err)
			if !ok {
				if test.expected == "notOK" {
					return
				}
				require.NoError(t, err, "seek not ok")
			}
			output.Write(it.UnsafeKey().Key)
			output.WriteByte(byte(it.UnsafeKey().Timestamp.WallTime))
			v, err := DecodeMVCCValueAndErr(it.UnsafeValue())
			require.NoError(t, err)
			if v.IsTombstone() {
				output.WriteRune('X')
			}
			require.Equal(t, test.expected, output.String())
		})
	}
}
