// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import (
	"testing"
)

func TestEncryptionTypeJWKAlgorithmRoundTrip(t *testing.T) {
	for key := range EncryptionType_name {
		et := EncryptionType(key)
		// Plaintext doesn't have a JWK algorithm.
		if et == EncryptionType_Plaintext {
			continue
		}
		t.Run(et.String(), func(t *testing.T) {
			alg, err := et.JWKAlgorithm()
			if err != nil {
				t.Fatalf("JWKAlgorithm() failed: %v", err)
			}
			roundTripped, err := EncryptionTypeFromJWKAlgorithm(alg)
			if err != nil {
				t.Fatalf("EncryptionTypeFromJWKAlgorithm(%q) failed: %v", alg, err)
			}
			if roundTripped != et {
				t.Errorf("round-trip failed: started with %v, got JWK algorithm %q, converted back to %v",
					et, alg, roundTripped)
			}
		})
	}
}
