// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"encoding/base64"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

func TestGCEBackupResourcesForProject(t *testing.T) {
	t.Setenv("BACKUP_TESTING_BUCKET", "")

	for _, tc := range []struct {
		name     string
		project  string
		expected gceBackupResources
	}{
		{
			name:    "production",
			project: "crl-e2e-infra",
			expected: gceBackupResources{
				project:                  "crl-e2e-infra",
				backupBucket:             "cockroachdb-backup-testing-crl-e2e-infra",
				kmsKeyA:                  "projects/crl-e2e-infra/locations/us-central1/keyRings/kms-backup-test/cryptoKeys/key-A",
				kmsKeyB:                  "projects/crl-e2e-infra/locations/northamerica-northeast2/keyRings/kms-backup-test-2/cryptoKeys/key-B",
				assumeRoleServiceAccount: "backup-testing@crl-e2e-infra.iam.gserviceaccount.com",
			},
		},
		{
			name:    "staging",
			project: "crl-e2e-infra-staging",
			expected: gceBackupResources{
				project:                  "crl-e2e-infra-staging",
				backupBucket:             "cockroachdb-backup-testing-crl-e2e-infra-staging",
				kmsKeyA:                  "projects/crl-e2e-infra-staging/locations/us-central1/keyRings/kms-backup-test/cryptoKeys/key-A",
				kmsKeyB:                  "projects/crl-e2e-infra-staging/locations/northamerica-northeast2/keyRings/kms-backup-test-2/cryptoKeys/key-B",
				assumeRoleServiceAccount: "backup-testing@crl-e2e-infra-staging.iam.gserviceaccount.com",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, gceBackupResourcesForProject(tc.project))
		})
	}
}

func TestResolveGCEBackupResourcesUsesRoachprodInfraProject(t *testing.T) {
	t.Setenv("BACKUP_TESTING_BUCKET", "")

	resources := resolveGCEBackupResources()
	require.Equal(t, gce.InfraProject(), resources.project)
	require.Equal(t, gceBackupResourcesForProject(gce.InfraProject()), resources)

	// Keep the expected backup-resource default tied to roachprod's default
	// rather than duplicating project-selection logic in the roachtest.
	require.Equal(t, "crl-e2e-infra", gce.DefaultProjectID)
	require.Equal(
		t,
		"cockroachdb-backup-testing-crl-e2e-infra",
		gceBackupResourcesForProject(gce.DefaultProjectID).backupBucket,
	)
}

func TestGCEBackupResourcesPreserveExplicitBucket(t *testing.T) {
	t.Setenv("BACKUP_TESTING_BUCKET", "explicit-backup-bucket")
	require.Equal(
		t,
		"explicit-backup-bucket",
		gceBackupResourcesForProject("ignored-project").backupBucket,
	)
}

func TestGCSKMSURIsUseProjectResourcesAndEphemeralCredentials(t *testing.T) {
	const credentials = `{"client_email":"ephemeral@example.com","private_key":"direct-secret"}`
	setGCEBackupCredentialsEnv(t, credentials)
	resources := gceBackupResourcesForProject("crl-e2e-infra-staging")

	kmsURIA, err := getGCSKMSURI(resources.kmsKeyA)
	require.NoError(t, err)
	requireGCSURI(
		t, kmsURIA, "", "/"+resources.kmsKeyA, credentials, "",
	)

	kmsURIB, err := getGCSKMSURI(resources.kmsKeyB)
	require.NoError(t, err)
	requireGCSURI(
		t, kmsURIB, "", "/"+resources.kmsKeyB, credentials, "",
	)
}

func TestGCSAssumeRoleURIsUseProjectResourcesAndEphemeralCredentials(t *testing.T) {
	const credentials = `{"client_email":"ephemeral@example.com","private_key":"assume-secret"}`
	setGCEBackupCredentialsEnv(t, credentials)
	resources := gceBackupResourcesForProject("crl-e2e-infra-staging")

	backupPath, err := getGCSBackupPath("test-destination", resources)
	require.NoError(t, err)
	requireGCSURI(
		t,
		backupPath,
		resources.backupBucket,
		"/gcs/test-destination",
		credentials,
		resources.assumeRoleServiceAccount,
	)

	kmsURI, err := getGCSKMSAssumeRoleURI(resources)
	require.NoError(t, err)
	requireGCSURI(
		t,
		kmsURI,
		"",
		"/"+resources.kmsKeyA,
		credentials,
		resources.assumeRoleServiceAccount,
	)
}

func TestGCSBackupURIsRequireEphemeralCredentials(t *testing.T) {
	const legacySecret = "legacy-secret-must-not-appear"
	t.Setenv(KMSGCSCredentials, "")
	t.Setenv("GOOGLE_CREDENTIALS_ASSUME_ROLE", legacySecret)
	t.Setenv("GOOGLE_SERVICE_ACCOUNT", legacySecret)
	t.Setenv("GOOGLE_KMS_KEY_A", legacySecret)
	t.Setenv("GOOGLE_KMS_KEY_B", legacySecret)
	resources := gceBackupResourcesForProject("crl-e2e-infra-staging")

	for _, tc := range []struct {
		name  string
		build func() (string, error)
	}{
		{name: "direct KMS", build: func() (string, error) {
			return getGCSKMSURI(resources.kmsKeyA)
		}},
		{name: "assume-role KMS", build: func() (string, error) {
			return getGCSKMSAssumeRoleURI(resources)
		}},
		{name: "assume-role storage", build: func() (string, error) {
			return getGCSBackupPath("test-destination", resources)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uri, err := tc.build()
			require.Empty(t, uri)
			require.ErrorContains(t, err, KMSGCSCredentials)
			require.NotContains(t, err.Error(), legacySecret)
		})
	}
}

func TestAWSBackupURIsUnaffected(t *testing.T) {
	t.Setenv("BACKUP_TESTING_BUCKET", "aws-backup-bucket")
	t.Setenv("AWS_ACCESS_KEY_ID", "direct-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "direct-secret-key")
	t.Setenv(KMSRegionAEnvVar, "us-east-2")
	t.Setenv(KMSKeyARNAEnvVar, "arn:aws:kms:us-east-2:123:key/direct")
	t.Setenv(AssumeRoleAWSKeyIDEnvVar, "assume-access-key")
	t.Setenv(AssumeRoleAWSSecretKeyEnvVar, "assume-secret-key")
	t.Setenv(AssumeRoleAWSRoleEnvVar, "arn:aws:iam::123:role/backup-testing")

	directKMSURI, err := getAWSKMSURI(KMSRegionAEnvVar, KMSKeyARNAEnvVar)
	require.NoError(t, err)
	directKMS, err := url.Parse(directKMSURI)
	require.NoError(t, err)
	require.Equal(t, "aws", directKMS.Scheme)
	require.Equal(t, "/arn:aws:kms:us-east-2:123:key/direct", directKMS.Path)
	require.Equal(t, "direct-access-key", directKMS.Query().Get("AWS_ACCESS_KEY_ID"))
	require.Equal(t, "direct-secret-key", directKMS.Query().Get("AWS_SECRET_ACCESS_KEY"))
	require.Equal(t, "us-east-2", directKMS.Query().Get("REGION"))

	assumeRoleKMSURI, err := getAWSKMSAssumeRoleURI()
	require.NoError(t, err)
	assumeRoleKMS, err := url.Parse(assumeRoleKMSURI)
	require.NoError(t, err)
	require.Equal(t, "assume-access-key", assumeRoleKMS.Query().Get("AWS_ACCESS_KEY_ID"))
	require.Equal(t, "assume-secret-key", assumeRoleKMS.Query().Get("AWS_SECRET_ACCESS_KEY"))
	require.Equal(t, "arn:aws:iam::123:role/backup-testing", assumeRoleKMS.Query().Get("ASSUME_ROLE"))
	require.Equal(t, "us-east-2", assumeRoleKMS.Query().Get("REGION"))

	backupPath, err := getAWSBackupPath("test-destination")
	require.NoError(t, err)
	backup, err := url.Parse(backupPath)
	require.NoError(t, err)
	require.Equal(t, "s3", backup.Scheme)
	require.Equal(t, "aws-backup-bucket", backup.Host)
	require.Equal(t, "/test-destination", backup.Path)
	require.Equal(t, "assume-access-key", backup.Query().Get("AWS_ACCESS_KEY_ID"))
	require.Equal(t, "assume-secret-key", backup.Query().Get("AWS_SECRET_ACCESS_KEY"))
	require.Equal(t, "arn:aws:iam::123:role/backup-testing", backup.Query().Get("ASSUME_ROLE"))
	require.Equal(t, "us-east-2", backup.Query().Get("AWS_REGION"))
}

func setGCEBackupCredentialsEnv(t *testing.T, credentials string) {
	t.Helper()
	t.Setenv(KMSGCSCredentials, credentials)
	// These legacy values should not be required by any GCE URI builder.
	t.Setenv("GOOGLE_CREDENTIALS_ASSUME_ROLE", "")
	t.Setenv("GOOGLE_SERVICE_ACCOUNT", "")
	t.Setenv("GOOGLE_KMS_KEY_A", "")
	t.Setenv("GOOGLE_KMS_KEY_B", "")
}

func requireGCSURI(
	t *testing.T, rawURI, expectedHost, expectedPath, credentials, assumeRole string,
) {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	require.NoError(t, err)
	require.Equal(t, "gs", parsed.Scheme)
	require.Equal(t, expectedHost, parsed.Host)
	require.Equal(t, expectedPath, parsed.Path)
	require.Equal(t, "specified", parsed.Query().Get("AUTH"))
	require.Equal(t, assumeRole, parsed.Query().Get("ASSUME_ROLE"))

	encodedCredentials := parsed.Query().Get("CREDENTIALS")
	decodedCredentials, err := base64.StdEncoding.DecodeString(encodedCredentials)
	require.NoError(t, err)
	require.Equal(t, credentials, string(decodedCredentials))
}
