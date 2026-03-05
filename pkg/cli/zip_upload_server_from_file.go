// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"archive/zip"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// runUploadFromZipFile opens an existing debug.zip file and uploads
// its contents to the upload server, bypassing the need for a live
// cluster connection.
func runUploadFromZipFile(ctx context.Context, zipPath string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return errors.Wrapf(err, "opening zip file %s", zipPath)
	}
	defer func() { _ = r.Close() }()

	// Determine the common base directory prefix. Debug zips typically
	// have all entries under a "debug/" prefix.
	prefix := detectZipPrefix(r.File)

	nodeCount := countNodesInZip(r.File, prefix)

	// Parse labels from flags.
	labels := map[string]string{}
	for _, l := range uploadServerCtx.labels {
		for _, kv := range strings.Split(l, ",") {
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) == 2 {
				labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	labels["source"] = "debug-zip-file"

	// Create upload server client.
	timeout := 10 * time.Minute
	if cliCtx.cmdTimeout != 0 {
		timeout = cliCtx.cmdTimeout
	}
	client := newUploadServerClient(uploadServerClientConfig{
		ServerURL: uploadServerCtx.serverURL,
		APIKey:    uploadServerCtx.apiKey,
		Timeout:   timeout,
	})

	fmt.Fprintf(stderr, "Uploading %s to %s...\n", zipPath, uploadServerCtx.serverURL)
	start := timeutil.Now()

	// Create a session with placeholder cluster metadata.
	if err := client.CreateSession(
		ctx,
		"from-debug-zip", // clusterID
		"from-debug-zip", // clusterName
		nodeCount,        // nodeCount
		"from-debug-zip", // crdbVersion
		zipCtx.redact,    // redacted
		labels,
	); err != nil {
		return errors.Wrap(err, "creating upload session")
	}
	fmt.Fprintf(stderr, "  Session ID: %s\n", client.sessionID)

	// Upload each zip entry as an artifact.
	var (
		uploaded  int
		errCount  int
		nodesSeen = map[int32]struct{}{}
	)

	for _, f := range r.File {
		// Skip directories.
		if strings.HasSuffix(f.Name, "/") {
			continue
		}

		// Strip the base directory prefix.
		relPath := strings.TrimPrefix(f.Name, prefix)
		if relPath == "" {
			continue
		}

		nodeID := extractNodeID(relPath)
		artType := inferArtifactTypeFromPath(relPath)

		rc, err := f.Open()
		if err != nil {
			fmt.Fprintf(stderr, "  warning: cannot open %s: %v\n", f.Name, err)
			errCount++
			continue
		}

		uploadErr := client.UploadArtifact(
			ctx,
			relPath,
			nodeID,
			artType,
			"application/octet-stream",
			"", // no idempotency key
			rc,
		)
		rc.Close()

		if uploadErr != nil {
			fmt.Fprintf(stderr, "  warning: upload failed for %s: %v\n", relPath, uploadErr)
			errCount++
			continue
		}

		uploaded++
		nodesSeen[nodeID] = struct{}{}
	}

	// Complete the session.
	nodesCompleted := make([]int32, 0, len(nodesSeen))
	for n := range nodesSeen {
		nodesCompleted = append(nodesCompleted, n)
	}
	if err := client.CompleteSession(ctx, uploaded, nodesCompleted); err != nil {
		return errors.Wrap(err, "completing upload session")
	}

	elapsed := timeutil.Since(start)
	fmt.Fprintf(stderr, "\nUpload complete.\n")
	fmt.Fprintf(stderr, "  Session ID:         %s\n", client.sessionID)
	fmt.Fprintf(stderr, "  Artifacts uploaded:  %d\n", uploaded)
	fmt.Fprintf(stderr, "  Errors:             %d\n", errCount)
	fmt.Fprintf(stderr, "  Duration:           %s\n", elapsed.Round(time.Second))

	if errCount > 0 {
		return errors.Newf("%d artifact(s) failed to upload", errCount)
	}
	return nil
}

// detectZipPrefix returns the common base directory prefix for all
// entries in the zip (e.g. "debug/"). Returns empty string if there
// is no common prefix.
func detectZipPrefix(files []*zip.File) string {
	if len(files) == 0 {
		return ""
	}
	// Find the first non-directory entry to detect the prefix.
	for _, f := range files {
		parts := strings.SplitN(f.Name, "/", 2)
		if len(parts) == 2 {
			return parts[0] + "/"
		}
	}
	return ""
}

// countNodesInZip counts distinct node IDs found under
// {prefix}nodes/{id}/ paths in the zip.
func countNodesInZip(files []*zip.File, prefix string) int {
	seen := map[string]struct{}{}
	for _, f := range files {
		rel := strings.TrimPrefix(f.Name, prefix)
		if strings.HasPrefix(rel, "nodes/") {
			parts := strings.SplitN(rel, "/", 3)
			if len(parts) >= 2 && parts[1] != "" {
				seen[parts[1]] = struct{}{}
			}
		}
	}
	return len(seen)
}

// extractNodeID parses a node ID from a path like "nodes/3/status.json".
// Returns 0 if the path is not under a nodes/ directory.
func extractNodeID(path string) int32 {
	if !strings.HasPrefix(path, "nodes/") {
		return 0
	}
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 2 {
		return 0
	}
	id, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return 0
	}
	return int32(id)
}

// inferArtifactTypeFromPath determines the artifact type based on the
// file path within the debug zip.
func inferArtifactTypeFromPath(path string) artifactType {
	base := filepath.Base(path)
	ext := filepath.Ext(base)

	if ext == ".pprof" {
		// Stacks pprof files are stacks, not profiles.
		if strings.Contains(base, "stacks") {
			return artifactTypeStack
		}
		return artifactTypeProfile
	}

	// Log files live under a logs/ directory.
	if strings.Contains(path, "/logs/") {
		return artifactTypeLog
	}

	// Stack traces.
	if strings.Contains(base, "stacks") && ext == ".txt" {
		return artifactTypeStack
	}

	// LSM stats.
	if strings.HasSuffix(base, "lsm.txt") {
		return artifactTypeEngineStats
	}

	if ext == ".json" {
		return artifactTypeMetadata
	}

	if ext == ".txt" {
		return artifactTypeTable
	}

	return artifactTypeMetadata
}
