// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package engineccl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMultiIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rocksDB := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer rocksDB.Close()

	// Each `input` is turned into an iterator and these are passed to a new
	// MultiIterator, which is fully iterated (using either NextKey or Next) and
	// turned back into a string in the same format as `input`. This is compared
	// to expectedNextKey or expectedNext (or if len(errRE) > 0, the expected
	// string is ignored and an error matching the regex is required.)
	//
	// Input is a string containing key, timestamp, value tuples: first a single
	// character key, then a single character timestamp walltime. If the
	// character after the timestamp is an X, this entry is a deletion
	// tombstone, otherwise the value is the same as the timestamp.
	tests := []struct {
		inputs          []string
		expectedNextKey string
		expectedNext    string
		errRE           string
	}{
		{[]string{}, "", "", ""},

		{[]string{"a1"}, "a1", "a1", ""},
		{[]string{"a1b1"}, "a1b1", "a1b1", ""},
		{[]string{"a2a1"}, "a2", "a2a1", ""},
		{[]string{"a2a1b1"}, "a2b1", "a2a1b1", ""},

		{[]string{"a1", "a2"}, "a2", "a2a1", ""},
		{[]string{"a2", "a1"}, "a2", "a2a1", ""},
		{[]string{"a1", "b2"}, "a1b2", "a1b2", ""},
		{[]string{"b2", "a1"}, "a1b2", "a1b2", ""},
		{[]string{"a1b2", "b3"}, "a1b3", "a1b3b2", ""},
		{[]string{"a1c2", "b3"}, "a1b3c2", "a1b3c2", ""},

		{[]string{"a1", "a2X"}, "a2X", "a2Xa1", ""},
		{[]string{"a1", "a2X", "a3"}, "a3", "a3a2Xa1", ""},
		{[]string{"a1", "a2Xb2"}, "a2Xb2", "a2Xa1b2", ""},
		{[]string{"a1b2", "a2X"}, "a2Xb2", "a2Xa1b2", ""},

		{[]string{"a1", "a1"}, "", "", "two entries for the same key and timestamp"},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%q", test.inputs)
		t.Run(name, func(t *testing.T) {
			var iters []engine.SimpleIterator
			for _, input := range test.inputs {
				batch := rocksDB.NewBatch()
				defer batch.Close()
				for i := 0; ; {
					if i == len(input) {
						break
					}
					k := []byte{input[i]}
					ts := hlc.Timestamp{WallTime: int64(input[i+1])}
					var v []byte
					if i+2 < len(input) && input[i+2] == 'X' {
						v = nil
						i++
					} else {
						v = []byte{input[i+1]}
					}
					i += 2
					if err := batch.Put(engine.MVCCKey{Key: k, Timestamp: ts}, v); err != nil {
						t.Fatalf("%+v", err)
					}
				}
				iter := batch.NewIterator(false)
				defer iter.Close()
				iters = append(iters, iter)
			}

			subtests := []struct {
				name     string
				expected string
				fn       func(engine.SimpleIterator)
			}{
				{"NextKey", test.expectedNextKey, (engine.SimpleIterator).NextKey},
				{"Next", test.expectedNext, (engine.SimpleIterator).Next},
			}
			for _, subtest := range subtests {
				t.Run(subtest.name, func(t *testing.T) {
					var output bytes.Buffer
					it := MakeMultiIterator(iters)
					for it.Seek(engine.MVCCKey{Key: keys.MinKey}); ; subtest.fn(it) {
						ok, err := it.Valid()
						if !testutils.IsError(err, test.errRE) {
							t.Fatalf("expected '%s' error got: %+v", test.errRE, err)
						}
						if !ok {
							break
						}
						output.Write(it.UnsafeKey().Key)
						output.WriteByte(byte(it.UnsafeKey().Timestamp.WallTime))
						if len(it.UnsafeValue()) == 0 {
							output.WriteRune('X')
						}
					}
					if actual := output.String(); actual != subtest.expected {
						t.Errorf("got %q expected %q", actual, subtest.expected)
					}
				})
			}
		})
	}
}
