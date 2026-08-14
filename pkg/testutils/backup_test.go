// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketForProject(t *testing.T) {
	require.Equal(t, "roachtest-cdc-output", BucketForProject("roachtest-cdc-output", ""))
	require.Equal(
		t,
		"roachtest-cdc-output-crl-e2e-infra-staging",
		BucketForProject("roachtest-cdc-output", "crl-e2e-infra-staging"),
	)
}

func TestBackupTestingBucketForProject(t *testing.T) {
	t.Setenv(backupTestingBucketEnvVar, "")
	require.Equal(t, defaultBackupBucket, BackupTestingBucketForProject(""))
	require.Equal(
		t,
		"cockroachdb-backup-testing-crl-e2e-infra-staging",
		BackupTestingBucketForProject("crl-e2e-infra-staging"),
	)

	t.Setenv(backupTestingBucketEnvVar, "explicit-bucket")
	require.Equal(t, "explicit-bucket", BackupTestingBucketForProject("ignored-project"))
}

func TestBackupTestingBucketLongTTLForProject(t *testing.T) {
	t.Setenv(backupTestingBucketLongTTLEnvVar, "")
	require.Equal(t, longTTLBackupTestingBucket, BackupTestingBucketLongTTLForProject(""))
	require.Equal(
		t,
		"cockroachdb-backup-testing-long-ttl-crl-e2e-infra-staging",
		BackupTestingBucketLongTTLForProject("crl-e2e-infra-staging"),
	)

	t.Setenv(backupTestingBucketLongTTLEnvVar, "explicit-long-ttl-bucket")
	require.Equal(
		t,
		"explicit-long-ttl-bucket",
		BackupTestingBucketLongTTLForProject("ignored-project"),
	)
}
