// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"google.golang.org/api/iterator"
)

// ReplayDLQ is the Cloud Function entry point. It lists all failed DLQ
// entries, claims each one using GCS preconditions, and replays the
// GitHub issue post.
func ReplayDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucketName := os.Getenv("GITHUB_DLQ_BUCKET")
	if bucketName == "" {
		http.Error(w, "GITHUB_DLQ_BUCKET env var not set", http.StatusInternalServerError)
		return
	}
	dryRun := os.Getenv("GITHUB_DLQ_REPLAY_DRY_RUN") != ""
	token := os.Getenv("GITHUB_API_TOKEN")
	if token == "" && !dryRun {
		http.Error(w, "GITHUB_API_TOKEN env var not set", http.StatusInternalServerError)
		return
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("creating GCS client: %v", err), http.StatusInternalServerError)
		return
	}
	defer func() { _ = client.Close() }()

	bucket := client.Bucket(bucketName)
	var processed, failed, skipped int

	it := bucket.Objects(ctx, &storage.Query{Prefix: "failed/"})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Printf("listing objects: %v", err)
			break
		}
		// Skip directory markers.
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}

		result := replayEntry(ctx, bucket, attrs.Name, token, dryRun)
		switch result {
		case replaySuccess:
			processed++
		case replayFailed:
			failed++
		case replaySkipped:
			skipped++
		}
	}

	summary := fmt.Sprintf("replay complete: processed=%d failed=%d skipped=%d",
		processed, failed, skipped)
	log.Print(summary)
	fmt.Fprint(w, summary)
}

type replayResult int

const (
	replaySuccess replayResult = iota
	replayFailed
	replaySkipped
)

// replayEntry attempts to claim and replay a single DLQ entry.
func replayEntry(
	ctx context.Context, bucket *storage.BucketHandle, objectName string, token string, dryRun bool,
) replayResult {
	// Claim the entry by copying to processing/ with a DoesNotExist
	// precondition. If another instance already claimed it, skip.
	processingName := strings.Replace(objectName, "failed/", "processing/", 1)
	src := bucket.Object(objectName)
	dst := bucket.Object(processingName)

	copier := dst.If(storage.Conditions{DoesNotExist: true}).CopierFrom(src)
	if _, err := copier.Run(ctx); err != nil {
		log.Printf("skipping %s (likely already claimed): %v", objectName, err)
		return replaySkipped
	}

	// Read and deserialize the entry from the original object.
	reader, err := src.NewReader(ctx)
	if err != nil {
		log.Printf("reading %s: %v", objectName, err)
		releaseClaim(ctx, bucket, processingName)
		return replayFailed
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		log.Printf("reading body of %s: %v", objectName, err)
		releaseClaim(ctx, bucket, processingName)
		return replayFailed
	}

	var entry DLQEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		log.Printf("unmarshaling %s: %v", objectName, err)
		releaseClaim(ctx, bucket, processingName)
		return replayFailed
	}

	// Reconstruct PostRequest and Options from the DLQ entry.
	req := issues.PostRequest{
		PackageName:             entry.PackageName,
		TestName:                entry.TestName,
		Labels:                  entry.Labels,
		AdoptIssueLabelMatchSet: entry.AdoptIssueLabelMatchSet,
		TopLevelNotes:           entry.TopLevelNotes,
		Message:                 entry.Message,
		ExtraParams:             entry.ExtraParams,
		Artifacts:               entry.Artifacts,
		MentionOnCreate:         entry.MentionOnCreate,
		HelpCommand:             reconstructHelpCommand(&entry),
	}

	binaryVersion := entry.BinaryVersion
	opts := &issues.Options{
		Token:            token,
		Org:              entry.Org,
		Repo:             entry.Repo,
		SHA:              entry.SHA,
		Branch:           entry.Branch,
		GetBinaryVersion: func() string { return binaryVersion },
	}
	if entry.TeamCityBuildTypeID != "" {
		opts.TeamCityOptions = &issues.TeamCityOptions{
			BuildTypeID: entry.TeamCityBuildTypeID,
			BuildID:     entry.TeamCityBuildID,
			ServerURL:   entry.TeamCityServerURL,
			Tags:        entry.TeamCityTags,
			Goflags:     entry.TeamCityGoflags,
		}
	}

	log.Printf("replaying %s: test=%s org=%s repo=%s", objectName, entry.TestName, entry.Org, entry.Repo)
	if dryRun {
		log.Printf("[dry-run] would post issue for %s (skipping)", objectName)
		return replaySuccess
	}
	if _, err := issues.Post(ctx, log.Default(), issues.UnitTestFormatter, req, opts); err != nil {
		log.Printf("replay failed for %s: %v", objectName, err)
		releaseClaim(ctx, bucket, processingName)
		return replayFailed
	}

	// Move to processed/.
	processedName := strings.Replace(objectName, "failed/", "processed/", 1)
	processedDst := bucket.Object(processedName)
	if _, err := processedDst.CopierFrom(src).Run(ctx); err != nil {
		log.Printf("copying to processed/ for %s: %v", objectName, err)
		// The issue was posted successfully, so don't release the claim.
		// This prevents double-posting on retry.
		return replaySuccess
	}

	// Clean up failed/ and processing/ copies.
	if err := src.Delete(ctx); err != nil {
		log.Printf("deleting %s: %v", objectName, err)
	}
	if err := dst.Delete(ctx); err != nil {
		log.Printf("deleting %s: %v", processingName, err)
	}

	log.Printf("successfully replayed %s", objectName)
	return replaySuccess
}

// releaseClaim deletes the processing/ copy to release the claim.
func releaseClaim(ctx context.Context, bucket *storage.BucketHandle, name string) {
	if err := bucket.Object(name).Delete(ctx); err != nil {
		log.Printf("releasing claim %s: %v", name, err)
	}
}

// reconstructHelpCommand builds a partial HelpCommand from the stored
// inputs. It generates the README and Grafana links but skips the
// Datadog link (which requires runtime flags unavailable here).
func reconstructHelpCommand(entry *DLQEntry) func(*issues.Renderer) {
	return func(r *issues.Renderer) {
		issues.HelpCommandAsLink(
			"roachtest README",
			"https://github.com/cockroachdb/cockroach/blob/master/pkg/cmd/roachtest/README.md",
		)(r)
		issues.HelpCommandAsLink(
			"How To Investigate (internal)",
			"https://cockroachlabs.atlassian.net/l/c/SSSBr8c7",
		)(r)
		if entry.HelpClusterName != "" && entry.HelpCloud == "gce" {
			issues.HelpCommandAsLink(
				"Grafana",
				fmt.Sprintf("https://go.crdb.dev/roachtest-grafana/%s/%s/%d/%d",
					vm.SanitizeLabel(entry.HelpRunID),
					vm.SanitizeLabel(entry.HelpTestName),
					entry.HelpStart.UnixMilli(),
					entry.HelpEnd.Add(2*time.Minute).UnixMilli()),
			)(r)
		}
	}
}
