// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

// gcsBucketForProject returns the project-local bucket hosted by the given GCE
// infrastructure project.
func gcsBucketForProject(bucket, infraProject string) string {
	return testutils.BucketForProject(bucket, infraProject)
}

func gcsBucket(bucket string) string {
	return gce.InfraResourceName(bucket)
}

func gceFixtureBucket() string {
	return gcsBucket("cockroach-fixtures-us-east1")
}

func gceFixtureURI(object string) string {
	return fmt.Sprintf("gs://%s/%s", gceFixtureBucket(), object)
}

func gceFixtureHTTPSURL(object string) string {
	return gcsHTTPSURL("cockroach-fixtures-us-east1", object)
}

func gceFixtureBucketFlag() string {
	return "--bucket-override=" + gceFixtureBucket()
}

func gceLongTTLBackupBucket() string {
	return testutils.BackupTestingBucketLongTTLForProject(gce.InfraProject())
}

func roachtestArtifactURL(object string) string {
	return vm.ArtifactsBaseURL() + "/" + strings.TrimPrefix(object, "/")
}

func gcsHTTPSURL(bucket, object string) string {
	return gcsHTTPSURLForProject(bucket, object, gce.InfraProject())
}

func gcsHTTPSURLForProject(bucket, object, project string) string {
	baseURL := fmt.Sprintf(
		"https://storage.googleapis.com/%s", gcsBucketForProject(bucket, project),
	)
	if object == "" {
		return baseURL
	}
	return baseURL + "/" + strings.TrimPrefix(object, "/")
}
