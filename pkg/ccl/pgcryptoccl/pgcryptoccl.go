// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcryptoccl

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/pgcryptoccl/pgcryptocipherccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/pgcrypto"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

func init() {
	pgcrypto.Decrypt = decrypt
	pgcrypto.DecryptIV = decryptIV
	pgcrypto.Encrypt = encrypt
	pgcrypto.EncryptIV = encryptIV
}

// EnterpriseLicenseCheckFeatureName is the feature name used in
// enterprise license check errors.
var EnterpriseLicenseCheckFeatureName = fmt.Sprintf(
	"this cryptographic function (%s)",
	docs.URL("functions-and-operators#cryptographic-functions"),
)

func checkEnterpriseEnabledForCryptoFunction(evalCtx *eval.Context) error {
	return utilccl.CheckEnterpriseEnabled(
		evalCtx.Settings,
		EnterpriseLicenseCheckFeatureName,
	)
}

func decrypt(
	_ context.Context, evalCtx *eval.Context, data []byte, key []byte, cipherType string,
) ([]byte, error) {
	if err := checkEnterpriseEnabledForCryptoFunction(evalCtx); err != nil {
		return nil, err
	}
	return pgcryptocipherccl.Decrypt(data, key, nil /* iv */, cipherType)
}

func decryptIV(
	_ context.Context, evalCtx *eval.Context, data []byte, key []byte, iv []byte, cipherType string,
) ([]byte, error) {
	if err := checkEnterpriseEnabledForCryptoFunction(evalCtx); err != nil {
		return nil, err
	}
	return pgcryptocipherccl.Decrypt(data, key, iv, cipherType)
}

func encrypt(
	_ context.Context, evalCtx *eval.Context, data []byte, key []byte, cipherType string,
) ([]byte, error) {
	if err := checkEnterpriseEnabledForCryptoFunction(evalCtx); err != nil {
		return nil, err
	}
	return pgcryptocipherccl.Encrypt(data, key, nil /* iv */, cipherType)
}

func encryptIV(
	_ context.Context, evalCtx *eval.Context, data []byte, key []byte, iv []byte, cipherType string,
) ([]byte, error) {
	if err := checkEnterpriseEnabledForCryptoFunction(evalCtx); err != nil {
		return nil, err
	}
	return pgcryptocipherccl.Encrypt(data, key, iv, cipherType)
}
