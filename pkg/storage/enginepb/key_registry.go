// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import "fmt"

// JWKAlgorithm returns the JWK algorithm name for this EncryptionType.
func (e EncryptionType) JWKAlgorithm() (string, error) {
	switch e {
	case EncryptionType_AES128_CTR:
		return "cockroach-aes-128-ctr-v1", nil
	case EncryptionType_AES192_CTR:
		return "cockroach-aes-192-ctr-v1", nil
	case EncryptionType_AES256_CTR:
		return "cockroach-aes-256-ctr-v1", nil

	case EncryptionType_AES_128_CTR_V2:
		return "cockroach-aes-128-ctr-v2", nil
	case EncryptionType_AES_192_CTR_V2:
		return "cockroach-aes-192-ctr-v2", nil
	case EncryptionType_AES_256_CTR_V2:
		return "cockroach-aes-192-ctr-v2", nil
	}
	return "", fmt.Errorf("unknown EncryptionType %d", e)
}

// EncryptionTypeFromJWKAlgorithm returns the EncryptionType for the given algorithm.
func EncryptionTypeFromJWKAlgorithm(s string) (EncryptionType, error) {
	switch s {
	case "cockroach-aes-128-ctr-v1":
		return EncryptionType_AES128_CTR, nil
	case "cockroach-aes-192-ctr-v1":
		return EncryptionType_AES192_CTR, nil
	case "cockroach-aes-256-ctr-v1":
		return EncryptionType_AES256_CTR, nil

	case "cockroach-aes-128-ctr-v2":
		return EncryptionType_AES_128_CTR_V2, nil
	case "cockroach-aes-192-ctr-v2":
		return EncryptionType_AES_192_CTR_V2, nil
	case "cockroach-aes-256-ctr-v2":
		return EncryptionType_AES_256_CTR_V2, nil
	}
	return 0, fmt.Errorf("unknown JWK algorithm name %s", s)
}
