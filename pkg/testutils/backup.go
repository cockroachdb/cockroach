// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import "os"

const (
	defaultBackupBucket       = "cockroachdb-backup-testing"
	backupTestingBucketEnvVar = "BACKUP_TESTING_BUCKET"
)

// BackupTestingBucket returns the name of the GCS bucket that should
// be used in a test run. Most times, this will be the regular public
// bucket. In private test runs, the name of the bucket is passed
// through an environment variable.
func BackupTestingBucket() string {
	if bucket := os.Getenv(backupTestingBucketEnvVar); bucket != "" {
		return bucket
	}

	return defaultBackupBucket
}
