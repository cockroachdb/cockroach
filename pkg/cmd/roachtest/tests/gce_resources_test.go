// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

func TestGCSBucketForProject(t *testing.T) {
	for _, bucket := range []string{
		"cockroach-corpus",
		"cockroach-fixtures-us-east1",
		"cockroach-jepsen",
		"cockroach-test-artifacts",
		"roachtest-cdc-output",
	} {
		t.Run(bucket, func(t *testing.T) {
			require.Equal(
				t,
				bucket+"-"+gce.DefaultProjectID,
				gcsBucketForProject(bucket, gce.DefaultProjectID),
			)
			require.Equal(
				t,
				bucket+"-crl-e2e-infra-staging",
				gcsBucketForProject(bucket, "crl-e2e-infra-staging"),
			)
		})
	}
}

func TestGCSHTTPSURLForProject(t *testing.T) {
	require.Equal(
		t,
		"https://storage.googleapis.com/cockroach-jepsen-crl-e2e-infra",
		gcsHTTPSURLForProject("cockroach-jepsen", "", gce.DefaultProjectID),
	)
	require.Equal(
		t,
		"https://storage.googleapis.com/cockroach-test-artifacts-crl-e2e-infra-staging/confluent/archive.tgz",
		gcsHTTPSURLForProject(
			"cockroach-test-artifacts", "/confluent/archive.tgz", "crl-e2e-infra-staging",
		),
	)
}

func TestGCEFixtureResources(t *testing.T) {
	t.Setenv("BACKUP_TESTING_BUCKET_LONG_TTL", "")
	project := gce.InfraProject()
	require.Equal(t, "cockroach-fixtures-us-east1-"+project, gceFixtureBucket())
	require.Equal(
		t,
		"gs://cockroach-fixtures-us-east1-"+project+"/fixture?AUTH=implicit",
		gceFixtureURI("fixture?AUTH=implicit"),
	)
	require.Equal(
		t,
		"https://storage.googleapis.com/cockroach-fixtures-us-east1-"+project+"/fixture",
		gceFixtureHTTPSURL("fixture"),
	)
	require.Equal(
		t,
		"--bucket-override=cockroach-fixtures-us-east1-"+project,
		gceFixtureBucketFlag(),
	)
	require.Equal(
		t,
		"cockroachdb-backup-testing-long-ttl-"+project,
		gceLongTTLBackupBucket(),
	)
}

func TestRoachtestArtifactURL(t *testing.T) {
	base := vm.ArtifactsBaseURL()
	require.Equal(t, base+"/confluent/archive.tgz", roachtestArtifactURL("confluent/archive.tgz"))
	require.Equal(t, base+"/confluent/archive.tgz", roachtestArtifactURL("/confluent/archive.tgz"))
}

func TestTPCHBaseURLForProject(t *testing.T) {
	require.Equal(
		t,
		"gs://cockroach-fixtures-us-east1-crl-e2e-infra/tpch-csv/",
		tpchBaseURLForProject("csv", gce.DefaultProjectID),
	)
	require.Equal(
		t,
		"gs://cockroach-fixtures-us-east1-crl-e2e-infra-staging/tpch-parquet/",
		tpchBaseURLForProject("parquet", "crl-e2e-infra-staging"),
	)
}

func TestImportCancellationFilename(t *testing.T) {
	fixtureBaseURI := strings.TrimSuffix(
		tpchBaseURLForProject("csv", "crl-e2e-infra-staging"), "/",
	)
	test := importCancellationTest{fixtureBaseURI: fixtureBaseURI}
	require.Equal(
		t,
		"'gs://cockroach-fixtures-us-east1-crl-e2e-infra-staging/tpch-csv/sf-100/region.tbl?AUTH=implicit'",
		test.makeFilename("region", 1, 1),
	)
	require.Equal(
		t,
		"'gs://cockroach-fixtures-us-east1-crl-e2e-infra-staging/tpch-csv/sf-100/part.tbl.2?AUTH=implicit'",
		test.makeFilename("part", 2, 8),
	)
}
