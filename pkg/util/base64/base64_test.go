// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base64

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestEncoder(t *testing.T) {
	const (
		iters  = 1000
		maxLen = 1000
	)

	rng, _ := randutil.NewTestRand()

	var enc Encoder
	enc.Init(base64.StdEncoding)

	if s := enc.String(); s != "" {
		t.Errorf("expected empty string, got %q", s)
	}

	for i := 0; i < iters; i++ {
		// Generate a random byte slice.
		b := randutil.RandBytes(rng, rng.Intn(maxLen+1))

		// Write b in randomly sized chunks.
		rest := b
		for len(rest) > 0 {
			chunk := rng.Intn(len(rest)) + 1
			enc.Write(rest[:chunk])
			rest = rest[chunk:]
		}

		// Decode the encoded string.
		s := enc.String()
		dec, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}

		// Check that the decoded string equals the original byte slice.
		if !bytes.Equal(b, dec) {
			t.Errorf("failed round-trip encoding/decoding of %v", b)
		}
	}
}
