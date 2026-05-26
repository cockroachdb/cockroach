// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJWKAlgorithmRoundtrip verifies that JWKAlgorithm and
// EncryptionTypeFromJWKAlgorithm are inverses of each other for all
// encryption types that support JWK.
func TestJWKAlgorithmRoundtrip(t *testing.T) {
	types := []EncryptionType{
		EncryptionType_AES128_CTR,
		EncryptionType_AES192_CTR,
		EncryptionType_AES256_CTR,
		EncryptionType_AES_128_CTR_V2,
		EncryptionType_AES_192_CTR_V2,
		EncryptionType_AES_256_CTR_V2,
	}
	for _, et := range types {
		t.Run(et.String(), func(t *testing.T) {
			alg, err := et.JWKAlgorithm()
			require.NoError(t, err)
			got, err := EncryptionTypeFromJWKAlgorithm(alg)
			require.NoError(t, err)
			require.Equal(t, et, got,
				"JWKAlgorithm() returned %q which maps back to %s, expected %s", alg, got, et)
		})
	}
}
