// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/stretchr/testify/require"
)

// TestKMSEnv holds the KMS configuration and the cluster settings
type TestKMSEnv struct {
	Settings         *cluster.Settings
	ExternalIOConfig *base.ExternalIODirConfig
	DB               *kv.DB
	Username         username.SQLUsername
	InternalEx       sqlutil.InternalExecutor
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
func (e *TestKMSEnv) DBHandle() *kv.DB {
	return e.DB
}

// User returns the user associated with the KMSEnv.
func (e *TestKMSEnv) User() username.SQLUsername {
	return e.Username
}

// InternalExecutor returns the internal executor associated with the KMSEnv.
func (e *TestKMSEnv) InternalExecutor() sqlutil.InternalExecutor {
	return e.InternalEx
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

	require.Regexp(t, "(PermissionDenied|AccessDenied|PERMISSION_DENIED)", err)
}
