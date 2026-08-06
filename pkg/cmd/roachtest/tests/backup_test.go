// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetGCSBackupPathUsesInfraProject(t *testing.T) {
	t.Setenv(AssumeRoleGCSCredentials, `{}`)
	t.Setenv(AssumeRoleGCSServiceAccount, "service-account")
	t.Setenv("BACKUP_TESTING_BUCKET", "")

	backupPath, err := getGCSBackupPath("test-destination", "crl-e2e-infra-staging")
	require.NoError(t, err)
	parsed, err := url.Parse(backupPath)
	require.NoError(t, err)
	require.Equal(t, "cockroachdb-backup-testing-crl-e2e-infra-staging", parsed.Host)
	require.Equal(t, "/gcs/test-destination", parsed.Path)

	t.Setenv("BACKUP_TESTING_BUCKET", "explicit-backup-bucket")
	backupPath, err = getGCSBackupPath("test-destination", "ignored-project")
	require.NoError(t, err)
	parsed, err = url.Parse(backupPath)
	require.NoError(t, err)
	require.Equal(t, "explicit-backup-bucket", parsed.Host)
}
