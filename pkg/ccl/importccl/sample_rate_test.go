// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSampleRate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numRows = 10000
		keySize = 100
		valSize = 50
	)

	tests := []struct {
		sampleSize float64
		expected   int
	}{
		{0, numRows},
		{100, numRows},
		{1000, 1448},
		{10000, 126},
		{100000, 13},
		{1000000, 1},
	}
	kv := roachpb.KeyValue{
		Key:   bytes.Repeat([]byte("0"), keySize),
		Value: roachpb.Value{RawBytes: bytes.Repeat([]byte("0"), valSize)},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.sampleSize), func(t *testing.T) {
			sr := sampleRate{
				rnd:        rand.New(rand.NewSource(0)),
				sampleSize: tc.sampleSize,
			}

			var sampled int
			for i := 0; i < numRows; i++ {
				if sr.sample(kv) {
					sampled++
				}
			}
			if sampled != tc.expected {
				t.Fatalf("got %d, expected %d", sampled, tc.expected)
			}
		})
	}
}
