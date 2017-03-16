// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

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
)

func TestMultiIterator(t *testing.T) {
	rocksDB := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer rocksDB.Close()

	// Each `input` is turned into an iterator and these are passed to a new
	// MultiIterator, which is fully iterated and turned back into a string in
	// the same format as `input`. This is compared to `expected` (or if
	// len(errRE) > 0, `expected` is ignored and an error matching the regex is
	// required.)
	//
	// Input is a string containing key, timestamp, value tuples: first a single
	// character key, then a single character timestamp walltime. If the
	// character after the timestamp is an X, this entry is a deletion
	// tombstone, otherwise the value is the same as the timestamp.
	tests := []struct {
		inputs   []string
		expected string
		errRE    string
	}{
		{[]string{}, "", ""},

		{[]string{"a1"}, "a1", ""},
		{[]string{"a1b1"}, "a1b1", ""},
		{[]string{"a2a1"}, "a2", ""},
		{[]string{"a2a1b1"}, "a2b1", ""},

		{[]string{"a1", "a2"}, "a2", ""},
		{[]string{"a2", "a1"}, "a2", ""},
		{[]string{"a1", "b2"}, "a1b2", ""},
		{[]string{"b2", "a1"}, "a1b2", ""},
		{[]string{"a1b2", "b3"}, "a1b3", ""},
		{[]string{"a1c2", "b3"}, "a1b3c2", ""},

		{[]string{"a1", "a2X"}, "", ""},
		{[]string{"a1", "a2X", "a3"}, "a3", ""},
		{[]string{"a1", "a2Xb2"}, "b2", ""},
		{[]string{"a1b2", "a2X"}, "b2", ""},

		{[]string{"a1", "a1"}, "", "two entries for the same key and timestamp"},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%q -> %s", test.inputs, test.expected)
		t.Run(name, func(t *testing.T) {
			var iters []engine.Iterator
			for _, input := range test.inputs {
				batch := rocksDB.NewBatch()
				defer batch.Close()
				for i := 0; ; {
					if i == len(input) {
						break
					}
					k := []byte{input[i]}
					t := hlc.Timestamp{WallTime: int64(input[i+1])}
					var v []byte
					if i+2 < len(input) && input[i+2] == 'X' {
						v = nil
						i++
					} else {
						v = []byte{input[i+1]}
					}
					i += 2
					batch.Put(engine.MVCCKey{Key: k, Timestamp: t}, v)
				}
				iter := batch.NewIterator(false)
				defer iter.Close()
				iters = append(iters, iter)
			}
			var output bytes.Buffer
			f := MakeMultiIterator(iters)
			for f.Seek(engine.MVCCKey{Key: keys.MinKey}); f.Valid(); f.NextKey() {
				output.Write(f.UnsafeKey().Key)
				output.WriteByte(byte(f.UnsafeKey().Timestamp.WallTime))
			}
			if err := f.Error(); len(test.errRE) > 0 {
				if !testutils.IsError(err, test.errRE) {
					t.Fatalf("expected '%s' error got: %+v", test.errRE, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if actual := output.String(); actual != test.expected {
				t.Errorf("got %q expected %q", actual, test.expected)
			}
		})
	}
}
