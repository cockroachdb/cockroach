// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cloudkmsimpl

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudkms"
	"github.com/stretchr/testify/require"
)

type testKMSEnv struct {
}

var _ cloudkms.KMSEnv = &testKMSEnv{}

func (e *testKMSEnv) ClusterSettings() *cluster.Settings {
	return cluster.NoSettings
}

func (e *testKMSEnv) KMSConfig() *base.KMSConfig {
	return &base.KMSConfig{}
}

func testEncryptDecrypt(t *testing.T, kmsURI string) {
	ctx := context.Background()
	env := testKMSEnv{}
	kms, err := cloudkms.KMSFromURI(ctx, kmsURI, &env)
	require.NoError(t, err)

	t.Run("simple encrypt decrypt", func(t *testing.T) {
		sampleBytes := "hello world"

		encryptedBytes, err := kms.Encrypt([]byte(sampleBytes))
		require.NoError(t, err)

		decryptedBytes, err := kms.Decrypt(encryptedBytes)
		require.NoError(t, err)

		require.True(t, bytes.Equal(decryptedBytes, []byte(sampleBytes)))
	})
}
