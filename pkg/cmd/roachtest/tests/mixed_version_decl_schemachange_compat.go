// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

func registerDeclSchemaChangeCompatMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "schemachange/mixed-versions-compat",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runDeclSchemaChangeCompatMixedVersions(ctx, t, c, *t.BuildVersion())
		},
	})
}

// fetchCorpusToTmp fetches corpus for a given version to tmp
func fetchCorpusToTmpDir(
	ctx context.Context, t test.Test, c cluster.Cluster, versionNumber string,
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
	err = c.RunE(ctx, c.Node(1),
		fmt.Sprintf(" gsutil cp gs://cockroach-corpus/corpus-release-%s/corpus %s",
			versionNumber,
			corpusFilePath))
	if err != nil {
		t.Fatalf("Missing validation corpus for %v (%v)", versionNumber, err)
		cleanupFn()
	}
	t.L().Printf("Fetched validation corpus for %v", versionNumber)
	return corpusFilePath, cleanupFn
}

// validateCorpusFile validates a downloaded corpus file on disk.
func validateCorpusFile(
	ctx context.Context, t test.Test, c cluster.Cluster, binaryName string, corpusPath string,
) {
	details, err := c.RunWithDetailsSingleNode(ctx,
		t.L(),
		c.Node(1),
		fmt.Sprintf("%s debug declarative-corpus-validate %s",
			binaryName,
			corpusPath))
	if err != nil {
		t.Fatalf("%v, %s, %s", err, details.Stdout, details.Stderr)
	}

	// Detect validation failures in standard output first, and dump those out.
	failureRegex := regexp.MustCompile(`failed to validate.*`)
	if matches := failureRegex.FindAllString(details.Stdout, -1); len(matches) > 0 {
		t.Fatalf("Validation of corpus has failed (exit status %s): \n%s",
			details.RemoteExitStatus,
			strings.Join(matches, "\n"))
	}

	// If no error is logged dump out both stdout and std error.
	if details.RemoteExitStatus != "0" {
		t.Fatalf("Validation command failed with exist status %s, output:\n %s\n%s\n",
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
func runDeclSchemaChangeCompatMixedVersions(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}
	// Validate the mixed version of the corpus, against the previous
	// version.
	binaryName := uploadVersion(ctx, t, c, c.All(), predecessorVersion)
	mixedVersionCorpus := fmt.Sprintf("%d.%d-mixed", buildVersion.Major(), buildVersion.Minor())
	corpusPath, cleanupFn := fetchCorpusToTmpDir(ctx, t, c, mixedVersionCorpus)
	if err != nil {
		t.Fatal("failed to locate backwards compatible corpus (%v)", err)
	}
	validateCorpusFile(ctx, t, c, binaryName, corpusPath)
	cleanupFn()
	// Forward compatibility testing of elements generated in the previous
	// version.
	binaryName = uploadVersion(ctx, t, c, c.All(), "")
	versionRegex := regexp.MustCompile(`(\d+\.\d+)`)
	versionNumber := versionRegex.FindStringSubmatch(predecessorVersion)[0]
	corpusPath, cleanupFn = fetchCorpusToTmpDir(ctx, t, c, versionNumber)
	if err != nil {
		t.Fatal("failed to locate backwards compatible corpus (%v)", err)
	}
	validateCorpusFile(ctx, t, c, binaryName, corpusPath)
	cleanupFn()
	// Validate the corpus generated on the same version as well.
	versionNumber = versionRegex.FindStringSubmatch(buildVersion.String())[0]
	corpusPath, cleanupFn = fetchCorpusToTmpDir(ctx, t, c, versionNumber)
	if err != nil {
		t.Fatal("failed to locate same version corpus (%v)", err)
	}
	validateCorpusFile(ctx, t, c, binaryName, corpusPath)
	cleanupFn()
}
