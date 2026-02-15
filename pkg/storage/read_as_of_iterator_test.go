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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type asOfTest struct {
	input           string
	expectedNextKey string
	asOf            string
}

func TestReadAsOfIterator(t *testing.T) {
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
		// Ensure nextkey works as expected.
		{input: "b1c1", expectedNextKey: "b1c1", asOf: ""},
		{input: "b2b1", expectedNextKey: "b2", asOf: ""},

		// Ensure AOST is an inclusive upper bound.
		{input: "b1", expectedNextKey: "b1", asOf: "1"},
		{input: "b2b1", expectedNextKey: "b1", asOf: "1"},

		// Double skip within keys.
		{input: "b3b2b1", expectedNextKey: "b1", asOf: "1"},

		// Double skip across keys.
		{input: "b2c2c1", expectedNextKey: "c1", asOf: "1"},

		// Ensure next key captures at most one mvcc key per key after an asOf skip.
		{input: "b3c2c1", expectedNextKey: "c2", asOf: "2"},

		// Ensure an AOST 'next' takes precedence over a tombstone 'nextkey'.
		{input: "b2Xb1c1", expectedNextKey: "c1", asOf: ""},
		{input: "b2Xb1c1", expectedNextKey: "b1c1", asOf: "1"},

		// Ensure clean iteration over double tombstone.
		{input: "a1Xb2Xb1c1", expectedNextKey: "c1", asOf: ""},
		{input: "a1Xb2Xb1c1", expectedNextKey: "b1c1", asOf: "1"},

		// Ensure tombstone is skipped after an AOST skip.
		{input: "b3c2Xc1d1", expectedNextKey: "d1", asOf: "2"},
		{input: "b3c2Xc1d1", expectedNextKey: "c1d1", asOf: "1"},

		// Ensure key before delete tombstone gets read if under AOST.
		{input: "b2b1Xc1", expectedNextKey: "b2c1", asOf: ""},
		{input: "b2b1Xc1", expectedNextKey: "c1", asOf: "1"},
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
				{"NextKey", test.expectedNextKey, (SimpleMVCCIterator).NextKey},
			}
			for _, subtest := range subtests {
				t.Run(subtest.name, func(t *testing.T) {
					asOf := hlc.Timestamp{}
					if test.asOf != "" {
						asOf.WallTime = int64(test.asOf[0])
					}
					it := NewReadAsOfIterator(iter, asOf)
					iterateSimpleMVCCIterator(t, it, subtest)
				})
			}
		})
	}
}

func TestReadAsOfIteratorSeek(t *testing.T) {
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

		// Ensure seek always returns the latest key of an MVCC key.
		{"a2a1b1", "a1", "b1", ""},
		{"a2a1b1", "a1", "b1", "2"},
		{"a2a1b1", "a1", "a1", "1"},

		// Ensure out of bounds seek fails gracefully.
		{"a1", "b1", "notOK", ""},

		// Ensure the asOf timestamp moves the iterator during a seek.
		{"a2a1", "a2", "a1", "1"},
		{"a2b1", "a2", "b1", "1"},

		// Ensure seek does not return on a tombstone.
		{"a3Xa1b1", "a3", "b1", ""},

		// Ensure seek does not return on a key shadowed by a tombstone.
		{"a3Xa2a1b1", "a2", "b1", ""},
		{"a3Xa2a1b1", "a2", "b1", "3"},
		{"a3a2Xa1b1", "a1", "b1", ""},
		{"a3a2Xa1b2Xb1c1", "a1", "c1", ""},

		// Ensure we can seek to a key right before a tombstone.
		{"a2Xa1b2b1Xc1", "a1", "b2", ""},

		// Ensure AOST 'next' takes precendence over tombstone 'nextkey'.
		{"a4a3Xa1b1", "a3", "a1", "1"},
		{"a4a3Xa2a1b1", "a2", "a1", "1"},
		{"a4a3Xa2a1b1", "a2", "a2", "2"},
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
			}
			it := NewReadAsOfIterator(iter, asOf)
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
			require.Equal(t, test.expected, output.String())
		})
	}
}

// populateBatch populates a pebble batch with a series of MVCC key values.
// input is a string containing key, timestamp, value tuples: first a single
// character key, then a single character timestamp walltime. If the
// character after the timestamp is an M, this entry is a "metadata" key
// (timestamp=0, sorts before any non-0 timestamp, and no value). If the
// character after the timestamp is an X, this entry is a deletion
// tombstone. Otherwise the value is the same as the timestamp.
func populateBatch(t *testing.T, batch Batch, input string) {
	for i := 0; ; {
		if i == len(input) {
			break
		}
		k := []byte{input[i]}
		ts := hlc.Timestamp{WallTime: int64(input[i+1])}
		var v MVCCValue
		if i+1 < len(input) && input[i+1] == 'M' {
			ts = hlc.Timestamp{}
		} else if i+2 < len(input) && input[i+2] == 'X' {
			i++
		} else {
			v.Value.SetString(string(input[i+1]))
		}
		i += 2
		if ts.IsEmpty() {
			vRaw, err := EncodeMVCCValue(v)
			require.NoError(t, err)
			require.NoError(t, batch.PutUnversioned(k, vRaw))
		} else {
			require.NoError(t, batch.PutMVCC(MVCCKey{Key: k, Timestamp: ts}, v))
		}
	}
}

type iterSubtest struct {
	name     string
	expected string
	fn       func(SimpleMVCCIterator)
}

// iterateSimpleMVCCIterator iterates through a simpleMVCCIterator for expected values,
// and assumes that populateBatch populated the keys for the iterator.
func iterateSimpleMVCCIterator(t *testing.T, it SimpleMVCCIterator, subtest iterSubtest) {
	var output bytes.Buffer
	for it.SeekGE(MVCCKey{Key: keys.LocalMax}); ; subtest.fn(it) {
		ok, err := it.Valid()
		require.NoError(t, err)
		if !ok {
			break
		}
		output.Write(it.UnsafeKey().Key)
		if it.UnsafeKey().Timestamp.IsEmpty() {
			output.WriteRune('M')
		} else {
			output.WriteByte(byte(it.UnsafeKey().Timestamp.WallTime))
			v, err := DecodeMVCCValueAndErr(it.UnsafeValue())
			require.NoError(t, err)
			if v.IsTombstone() {
				output.WriteRune('X')
			}
		}
	}
	require.Equal(t, subtest.expected, output.String())
}

// TestReadAsOfIteratorEmitDeletes tests that the ReadAsOfIterator emits
// tombstones when created with NewReadAsOfIteratorWithEmitDeletes.
func TestReadAsOfIteratorEmitDeletes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pebble, err := Open(context.Background(), InMemory(),
		cluster.MakeTestingClusterSettings(), CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer pebble.Close()

	tests := []struct {
		input              string
		expectedNormal     string // expected output with normal iterator (skips tombstones)
		expectedEmitDelete string // expected output with emitDeletes iterator
		asOf               string
	}{
		// A tombstone with a value underneath - normal skips both, emitDeletes emits tombstone.
		{input: "b2Xb1", expectedNormal: "", expectedEmitDelete: "b2X", asOf: ""},

		// Multiple keys, one with a tombstone.
		{input: "a1b2Xb1c1", expectedNormal: "a1c1", expectedEmitDelete: "a1b2Xc1", asOf: ""},

		// Tombstone only (no live value underneath).
		{input: "a2X", expectedNormal: "", expectedEmitDelete: "a2X", asOf: ""},

		// Multiple tombstones on different keys.
		{input: "a2Xa1b2X", expectedNormal: "", expectedEmitDelete: "a2Xb2X", asOf: ""},

		// With AOST - tombstone above AOST should be skipped, value below should be returned.
		{input: "a3Xa1", expectedNormal: "a1", expectedEmitDelete: "a1", asOf: "2"},

		// With AOST - tombstone at AOST should be emitted.
		{input: "a2Xa1", expectedNormal: "", expectedEmitDelete: "a2X", asOf: "2"},
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
			}

			// Test normal iterator (skips tombstones).
			t.Run("normal", func(t *testing.T) {
				it := NewReadAsOfIterator(iter, asOf)
				iterateSimpleMVCCIterator(t, it, iterSubtest{
					name:     "NextKey",
					expected: test.expectedNormal,
					fn:       (SimpleMVCCIterator).NextKey,
				})
			})

			// Test emitDeletes iterator (emits tombstones).
			t.Run("emitDeletes", func(t *testing.T) {
				iter2, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
				require.NoError(t, err)
				defer iter2.Close()

				it := NewReadAsOfIteratorWithEmitDeletes(iter2, asOf)
				iterateSimpleMVCCIterator(t, it, iterSubtest{
					name:     "NextKey",
					expected: test.expectedEmitDelete,
					fn:       (SimpleMVCCIterator).NextKey,
				})
			})
		})
	}
}

// TestReadAsOfIteratorEmitDeletesSeekGE tests SeekGE behavior with the
// emitDeletes iterator, particularly when seeking to or past tombstones.
func TestReadAsOfIteratorEmitDeletesSeekGE(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pebble, err := Open(context.Background(), InMemory(),
		cluster.MakeTestingClusterSettings(), CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer pebble.Close()

	tests := []struct {
		input    string
		seekKey  string
		expected string // expected key after SeekGE with emitDeletes
		asOf     string
	}{
		// SeekGE to a key with a tombstone should return the tombstone.
		{input: "a2Xb1", seekKey: "a", expected: "a2X", asOf: ""},

		// SeekGE to a key past a tombstone should skip it.
		{input: "a2Xb1", seekKey: "b", expected: "b1", asOf: ""},

		// SeekGE to a tombstone-only key.
		{input: "a2X", seekKey: "a", expected: "a2X", asOf: ""},

		// SeekGE with AOST - tombstone above AOST should be skipped.
		{input: "a3Xa1b1", seekKey: "a", expected: "a1", asOf: "2"},

		// SeekGE with AOST - tombstone at AOST should be emitted.
		{input: "a2Xa1b1", seekKey: "a", expected: "a2X", asOf: "2"},

		// SeekGE to a key with tombstone that shadows a value.
		{input: "a2Xa1b1", seekKey: "a", expected: "a2X", asOf: ""},

		// SeekGE to exact timestamp of a tombstone.
		{input: "a2a1Xb1", seekKey: "a", expected: "a2", asOf: ""},

		// SeekGE to a key between keys where first has tombstone.
		{input: "a2Xc1", seekKey: "b", expected: "c1", asOf: ""},
	}

	for i, test := range tests {
		name := fmt.Sprintf("Test %d: seek %s in %s, AOST %s", i, test.seekKey, test.input, test.asOf)
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
			}

			it := NewReadAsOfIteratorWithEmitDeletes(iter, asOf)

			seekKey := MVCCKey{Key: roachpb.Key(test.seekKey)}
			it.SeekGE(seekKey)

			ok, err := it.Valid()
			require.NoError(t, err)

			if test.expected == "" {
				require.False(t, ok, "expected invalid iterator after SeekGE")
				return
			}

			require.True(t, ok, "expected valid iterator after SeekGE")

			var output bytes.Buffer
			output.Write(it.UnsafeKey().Key)
			output.WriteByte(byte(it.UnsafeKey().Timestamp.WallTime))
			v, err := DecodeMVCCValueAndErr(it.UnsafeValue())
			require.NoError(t, err)
			if v.IsTombstone() {
				output.WriteRune('X')
			}

			require.Equal(t, test.expected, output.String(),
				"SeekGE(%s) in %s with AOST %s", test.seekKey, test.input, test.asOf)
		})
	}
}
