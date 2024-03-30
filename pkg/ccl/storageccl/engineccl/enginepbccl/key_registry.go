// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package enginepbccl

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
