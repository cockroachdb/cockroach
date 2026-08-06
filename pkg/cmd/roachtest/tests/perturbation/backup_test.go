// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/stretchr/testify/require"
)

func TestBackupDestination(t *testing.T) {
	t.Setenv("BACKUP_TESTING_BUCKET", "")
	require.Equal(
		t,
		"gs://cockroachdb-backup-testing-crl-e2e-infra-staging/perturbation-backups/test?AUTH=implicit",
		backupDestination(spec.GCE, "crl-e2e-infra-staging", 1, "test"),
	)
	require.Equal(
		t,
		"s3://cockroachdb-backup-testing/perturbation-backups/test?AUTH=implicit",
		backupDestination(spec.AWS, "ignored-project", 1, "test"),
	)
	require.Equal(
		t,
		"azure://cockroachdb-backup-testing/perturbation-backups/test?AUTH=implicit",
		backupDestination(spec.Azure, "ignored-project", 1, "test"),
	)
	require.Equal(
		t,
		"nodelocal://7/perturbation-backups/test?AUTH=implicit",
		backupDestination(spec.Local, "ignored-project", 7, "test"),
	)

	t.Setenv("BACKUP_TESTING_BUCKET", "explicit-backup-bucket")
	require.Equal(
		t,
		"gs://explicit-backup-bucket/perturbation-backups/test?AUTH=implicit",
		backupDestination(spec.GCE, "ignored-project", 1, "test"),
	)
}
