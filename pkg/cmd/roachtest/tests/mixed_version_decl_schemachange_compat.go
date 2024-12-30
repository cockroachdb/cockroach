// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/release"
)

func registerDeclSchemaChangeCompatMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "schemachange/mixed-versions-compat",
		Owner:   registry.OwnerSQLFoundations,
		Cluster: r.MakeClusterSpec(1),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDeclSchemaChangeCompatMixedVersions(ctx, t, c)
		},
	})
}

// fetchCorpusToTmp fetches corpus for a given version to tmp
func fetchCorpusToTmpDir(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	versionNumber string,
	alternateVersion string,
) (corpusFilePath string, cleanupFn func()) {
	tmpDir, err := os.MkdirTemp("", "corpus")
	if err != nil {
		t.Fatalf("unable to create temp directory for corpus: %v", err)
	}
	corpusFilePath = filepath.Join(tmpDir, "corpus")
	// Callback for cleaning up the temporary directory.
	cleanupFn = func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			t.L().Printf("failed to clean up tmp directory %v", err)
		}
	}
	versionsToCheck := []string{versionNumber}
	if len(alternateVersion) > 0 {
		versionsToCheck = []string{versionNumber, alternateVersion}
	}
	for i, version := range versionsToCheck {
		err = c.RunE(ctx, option.WithNodes(c.Node(1)),
			fmt.Sprintf(" gsutil cp gs://cockroach-corpus/corpus-%s/corpus %s",
				version,
				corpusFilePath))
		if err != nil && i != len(versionsToCheck)-1 {
			t.L().Printf("Failed to fetch corpus %s with error %v, trying the next one",
				version,
				err)
		}
		if err == nil {
			t.L().Printf("Fetched validation corpus for %v", version)
			break
		}
	}
	if err != nil {
		cleanupFn()
		t.Fatalf("Missing validation corpus for %v (%v)", versionNumber, err)
	}
	return corpusFilePath, cleanupFn
}

// validateCorpusFile validates a downloaded corpus file on disk.
func validateCorpusFile(
	ctx context.Context, t test.Test, c cluster.Cluster, binaryName string, corpusPath string,
) {
	details, err := c.RunWithDetailsSingleNode(ctx,
		t.L(),
		option.WithNodes(c.Node(1)),
		fmt.Sprintf("%s debug declarative-corpus-validate %s",
			binaryName,
			corpusPath))
	if err != nil {
		t.Fatalf("%v, %s, %s", err, details.Stdout, details.Stderr)
	}

	// Detect validation failures in standard output first, and dump those out.
	failureRegex := regexp.MustCompile(`failed to validate.*`)
	if matches := failureRegex.FindAllString(details.Stdout, -1); len(matches) > 0 {
		t.Fatalf("Validation of corpus has failed (exit status %d): \n%s",
			details.RemoteExitStatus,
			strings.Join(matches, "\n"))
	}

	// If no error is logged dump out both stdout and std error.
	if details.RemoteExitStatus != 0 {
		t.Fatalf("Validation command failed with exit status %d, output:\n %s\n%s\n",
			details.RemoteExitStatus,
			details.Stdout,
			details.Stderr,
		)
	}
}

// runDeclSchemaChangeCompatMixedVersions does mixed version testing for the
// declarative schema changer planner by testing:
//  1. Mixed version elements generated on the current version and confirm the
//     previous version can plan on them.
//  2. Elements generated on the previous version can be planned on the current
//     version of cockroach.
//  3. Elements from the current version are backwards compatible with the same
//     version (specifically focused on master during development before
//     a version bump).
func runDeclSchemaChangeCompatMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	currentVersion := clusterupgrade.CurrentVersion()
	predecessorVersionStr, err := release.LatestPredecessor(&currentVersion.Version)
	if err != nil {
		t.Fatal(err)
	}
	predecessorVersion := clusterupgrade.MustParseVersion(predecessorVersionStr)

	releaseSeries := func(v *clusterupgrade.Version) string {
		return fmt.Sprintf("%d.%d", v.Major(), v.Minor())
	}

	// Test definitions which indicates which version of the corpus to fetch,
	// and the binary to validate against.
	compatTests := []struct {
		testName               string
		binaryVersion          *clusterupgrade.Version
		corpusVersion          string
		alternateCorpusVersion string
	}{
		{
			testName:               "backwards compatibility",
			binaryVersion:          predecessorVersion,
			corpusVersion:          fmt.Sprintf("mixed-release-%s", releaseSeries(currentVersion)),
			alternateCorpusVersion: "mixed-master",
		},
		{
			testName:      "forwards compatibility",
			binaryVersion: currentVersion,
			corpusVersion: fmt.Sprintf("release-%s", releaseSeries(predecessorVersion)),
		},
		{
			testName:               "same version",
			binaryVersion:          currentVersion,
			corpusVersion:          fmt.Sprintf("release-%s", releaseSeries(currentVersion)),
			alternateCorpusVersion: "master",
		},
	}
	for _, testInfo := range compatTests {
		binaryName := uploadCockroach(ctx, t, c, c.All(), testInfo.binaryVersion)
		corpusPath, cleanupFn := fetchCorpusToTmpDir(ctx, t, c, testInfo.corpusVersion, testInfo.alternateCorpusVersion)
		func() {
			defer cleanupFn()
			validateCorpusFile(ctx, t, c, binaryName, corpusPath)
		}()

	}
}
