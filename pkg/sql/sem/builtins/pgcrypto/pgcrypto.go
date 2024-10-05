// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgcrypto

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

var Decrypt = func(_ context.Context, _ *eval.Context, data []byte, key []byte, cipherType string) ([]byte, error) {
	return nil, pgerror.New(
		pgcode.CCLRequired,
		"decrypt can only be used with a CCL distribution",
	)
}

var DecryptIV = func(_ context.Context, _ *eval.Context, data []byte, key []byte, iv []byte, cipherType string) ([]byte, error) {
	return nil, pgerror.New(
		pgcode.CCLRequired,
		"decrypt_iv can only be used with a CCL distribution",
	)
}

var Encrypt = func(_ context.Context, _ *eval.Context, data []byte, key []byte, cipherType string) ([]byte, error) {
	return nil, pgerror.New(
		pgcode.CCLRequired,
		"encrypt can only be used with a CCL distribution",
	)
}

var EncryptIV = func(_ context.Context, _ *eval.Context, data []byte, key []byte, iv []byte, cipherType string) ([]byte, error) {
	return nil, pgerror.New(
		pgcode.CCLRequired,
		"encrypt_iv can only be used with a CCL distribution",
	)
}
