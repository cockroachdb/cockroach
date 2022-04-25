// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type asOfTest struct {
	input           string
	expectedNextKey string
	expectedNext    string
	asOf            string
}

func TestReadAsOfIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pebble, err := Open(context.Background(), InMemory(), CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatal(err)
	}
	defer pebble.Close()

	// Each `input` is turned into an iterator and these are passed to a new
	// readAsOfIterator, which is fully iterated (using either NextKey or Next) and
	// turned back into a string in the same format as `input`. This is compared
	// to 'expectedNextKey' or 'expectedNext'. The 'asOf' field represents the
	// wall time of the hlc.Timestamp for the readAsOfIterator. This field is a
	// string to play nice with the 'input' parser in populateBatch.
	tests := []asOfTest{
		// ensure next and nextkey work as expected
		{input: "b1c1", expectedNextKey: "b1c1", expectedNext: "b1c1", asOf: ""},
		{input: "b2b1", expectedNextKey: "b2", expectedNext: "b2b1", asOf: ""},

		// ensure AOST is an inclusive upper bound
		{input: "b1", expectedNextKey: "b1", expectedNext: "b1", asOf: "1"},
		{input: "b2b1", expectedNextKey: "b1", expectedNext: "b1", asOf: "1"},

		//double skip within keys
		{input: "b3b2b1", expectedNextKey: "b1", expectedNext: "b1", asOf: "1"},

		// double skip across keys
		{input: "b2c2c1", expectedNextKey: "c1", expectedNext: "c1", asOf: "1"},

		// ensure next key captures at most one mvcc key per key after an asOf skip
		{input: "b3c2c1", expectedNextKey: "c2", expectedNext: "c2c1", asOf: "2"},

		// ensure an AOST 'next' takes precedence over a tombstone 'nextkey'
		{input: "b2Xb1c1", expectedNextKey: "c1", expectedNext: "c1", asOf: ""},
		{input: "b2Xb1c1", expectedNextKey: "b1c1", expectedNext: "b1c1", asOf: "1"},

		// Ensure clean iteration over double tombstone
		{input: "a1Xb2Xb1c1", expectedNextKey: "c1", expectedNext: "c1", asOf: ""},
		{input: "a1Xb2Xb1c1", expectedNextKey: "b1c1", expectedNext: "b1c1", asOf: "1"},

		// ensure tombstone is skipped after an AOST skip
		{input: "b3c2Xc1d1", expectedNextKey: "d1", expectedNext: "d1", asOf: "2"},
		{input: "b3c2Xc1d1", expectedNextKey: "c1d1", expectedNext: "c1d1", asOf: "1"},

		// Ensure key before delete tombstone gets read if under AOST
		{input: "b2b1Xc1", expectedNextKey: "b2c1", expectedNext: "b2c1", asOf: ""},
		{input: "b2b1Xc1", expectedNextKey: "c1", expectedNext: "c1", asOf: "1"},
	}

	for i, test := range tests {
		name := fmt.Sprintf("Test %d: %s, AOST %s", i, test.input, test.asOf)
		t.Run(name, func(t *testing.T) {
			batch := pebble.NewBatch()
			defer batch.Close()
			populateBatch(t, batch, test.input)
			iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
			defer iter.Close()

			subtests := []iterSubtest{
				{"NextKey", test.expectedNextKey, (SimpleMVCCIterator).NextKey},
				{"Next", test.expectedNext, (SimpleMVCCIterator).Next},
			}
			for _, subtest := range subtests {
				t.Run(subtest.name, func(t *testing.T) {
					asOf := hlc.Timestamp{}
					if test.asOf != "" {
						asOf.WallTime = int64(test.asOf[0])
					}
					it := NewReadAsOfIterator(iter, asOf)
					iterateSimpleMultiIter(t, it, subtest)
				})
			}
		})
	}
}

func TestReadAsOfIteratorSeek(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pebble, err := Open(context.Background(), InMemory(), CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatal(err)
	}
	defer pebble.Close()

	tests := []struct {
		input    string
		seekKey  string
		expected string
		asOf     string
	}{
		// Ensure vanilla seek works
		{"a3a2a1", "a1", "a1", ""},
		{"a3a2a1", "a1", "a1", "2"},

		// Ensure out of bounds seek fails gracefully
		{"a1", "b1", "notOK", ""},

		// Ensure the asOf timestamp moves the iterator during a seek
		{"a2a1", "a2", "a1", "1"},
		{"a2b1", "a2", "b1", "1"},

		// Ensure seek does not return on a tombstone
		{"a3Xa1b1", "a3", "b1", ""},

		// Ensure seek does not return on a key shadowed by a tombstone
		{"a3Xa2a1b1", "a2", "b1", ""},
		{"a3Xa2a1b1", "a2", "b1", "3"},
		{"a3a2Xa1b1", "a1", "b1", ""},
		{"a3a2Xa1b2Xb1c1", "a1", "c1", ""},

		// Ensure we can seek to a key right before a tombstone
		{"a2Xa1b2b1Xc1", "a1", "b2", ""},

		// Ensure AOST 'next' takes precendence over tombstone 'nextkey'
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
			iter := batch.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: roachpb.KeyMax})
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
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if !ok {
				if test.expected == "notOK" {
					return
				}
				t.Fatalf("unexpected error: seek not ok")
			}
			output.Write(it.UnsafeKey().Key)
			output.WriteByte(byte(it.UnsafeKey().Timestamp.WallTime))
			if actual := output.String(); actual != test.expected {
				t.Errorf("got %q expected %q", actual, test.expected)
			}
		})
	}
}
