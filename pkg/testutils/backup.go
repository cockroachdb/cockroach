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
	defaultBackupBucket              = "cockroachdb-backup-testing"
	longTTLBackupTestingBucket       = "cockroachdb-backup-testing-long-ttl"
	backupTestingBucketEnvVar        = "BACKUP_TESTING_BUCKET"
	backupTestingBucketLongTTLEnvVar = "BACKUP_TESTING_BUCKET_LONG_TTL"
)

// BackupTestingBucket returns the name of the external storage bucket that
// should be used in a test run. Most times, this will be the regular public
// bucket. In private test runs, the name of the bucket is passed through an
// environment variable.
func BackupTestingBucket() string {
	if bucket := os.Getenv(backupTestingBucketEnvVar); bucket != "" {
		return bucket
	}

	return defaultBackupBucket
}

// BackupTestingBucketLongTTL returns the name of the external storage bucket
// that should be used in a test run where the bucket's content may inform a
// debugging investigation. At the time of this comment, the ttl for the s3 and
// gcs buckets is 20 days.
//
// In private test runs, the name of the bucket is passed through an environment
// variable.
func BackupTestingBucketLongTTL() string {
	if bucket := os.Getenv(backupTestingBucketLongTTLEnvVar); bucket != "" {
		return bucket
	}

	return longTTLBackupTestingBucket
}
