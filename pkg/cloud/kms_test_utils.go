// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/stretchr/testify/require"
)

// TestKMSEnv holds the KMS configuration and the cluster settings
type TestKMSEnv struct {
	Settings         *cluster.Settings
	ExternalIOConfig *base.ExternalIODirConfig
	DB               isql.DB
	Username         username.SQLUsername
}

var _ KMSEnv = &TestKMSEnv{}

// ClusterSettings returns the cluster settings
func (e *TestKMSEnv) ClusterSettings() *cluster.Settings {
	return e.Settings
}

// KMSConfig returns the configurable settings of the KMS
func (e *TestKMSEnv) KMSConfig() *base.ExternalIODirConfig {
	return e.ExternalIOConfig
}

// DBHandle returns the database handle associated with the KMSEnv.
func (e *TestKMSEnv) DBHandle() isql.DB {
	return e.DB
}

// User returns the user associated with the KMSEnv.
func (e *TestKMSEnv) User() username.SQLUsername {
	return e.Username
}

// KMSEncryptDecrypt is the method used to test if the given KMS can
// correctly encrypt and decrypt a string
func KMSEncryptDecrypt(t *testing.T, kmsURI string, env KMSEnv) {
	ctx := context.Background()
	kms, err := KMSFromURI(ctx, kmsURI, env)
	require.NoError(t, err)

	t.Run("simple encrypt decrypt", func(t *testing.T) {
		sampleBytes := "hello world"

		encryptedBytes, err := kms.Encrypt(ctx, []byte(sampleBytes))
		require.NoError(t, err)

		decryptedBytes, err := kms.Decrypt(ctx, encryptedBytes)
		require.NoError(t, err)

		require.True(t, bytes.Equal(decryptedBytes, []byte(sampleBytes)))

		require.NoError(t, kms.Close())
	})
}

// CheckNoKMSAccess verifies that trying to encrypt with the given kmsURI gives
// a permission error.
func CheckNoKMSAccess(t *testing.T, kmsURI string, env KMSEnv) {
	ctx := context.Background()
	kms, err := KMSFromURI(ctx, kmsURI, env)
	require.NoError(t, err)

	_, err = kms.Encrypt(ctx, []byte("test bytes"))
	if err == nil {
		t.Fatalf("expected error when encrypting with kms %s", kmsURI)
	}

	require.Regexp(t, "(PermissionDenied|AccessDenied|PERMISSION_DENIED|failed to acquire a token)", err)
}
