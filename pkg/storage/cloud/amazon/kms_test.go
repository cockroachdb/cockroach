// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package amazon

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/stretchr/testify/require"
)

type testKMSEnv struct {
	settings         *cluster.Settings
	externalIOConfig *base.ExternalIODirConfig
}

var _ cloud.KMSEnv = &testKMSEnv{}

func (e *testKMSEnv) ClusterSettings() *cluster.Settings {
	return e.settings
}

func (e *testKMSEnv) KMSConfig() *base.ExternalIODirConfig {
	return e.externalIOConfig
}

func testEncryptDecrypt(t *testing.T, kmsURI string, env testKMSEnv) {
	ctx := context.Background()
	kms, err := cloud.KMSFromURI(kmsURI, &env)
	require.NoError(t, err)

	t.Run("simple encrypt decrypt", func(t *testing.T) {
		sampleBytes := "hello world"

		encryptedBytes, err := kms.Encrypt(ctx, []byte(sampleBytes))
		require.NoError(t, err)

		decryptedBytes, err := kms.Decrypt(ctx, encryptedBytes)
		require.NoError(t, err)

		require.True(t, bytes.Equal(decryptedBytes, []byte(sampleBytes)))
	})
}
