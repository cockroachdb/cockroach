// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// dlq-replay is a command-line tool that walks a roachtest GitHub DLQ
// bucket and re-fires any failed issue posts. Run it manually after a
// GitHub outage has resolved.
//
// Usage:
//
//	GITHUB_API_TOKEN=<token> ./bin/dlq-replay --bucket=<bucket-name>
//
// Optional flags:
//
//	--branch=master   only process entries under failed/<branch>/
//	--skip-github-post  claim and verify entries without posting to GitHub
//
// GCS authentication uses Application Default Credentials.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/dlq"
)

var (
	flagBucket         = flag.String("bucket", "", "GCS bucket containing the DLQ (required)")
	flagBranch         = flag.String("branch", "", "only process entries under failed/<branch>/")
	flagSkipGitHubPost = flag.Bool("skip-github-post", false, "claim and verify entries without posting to GitHub")
)

func main() {
	flag.Parse()
	if *flagBucket == "" {
		fmt.Fprintln(os.Stderr, "--bucket is required")
		flag.Usage()
		os.Exit(2)
	}
	if !*flagSkipGitHubPost && os.Getenv("GITHUB_API_TOKEN") == "" {
		fmt.Fprintln(os.Stderr,
			"GITHUB_API_TOKEN env var is required (use --skip-github-post to skip posting)")
		os.Exit(2)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "creating GCS client: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	fmt.Fprintf(os.Stdout, "dlq-replay starting: bucket=%s branch=%q skip_github_post=%v\n",
		*flagBucket, *flagBranch, *flagSkipGitHubPost)
	logger := dlq.NewDefaultLogger()

	result, err := dlq.Replay(ctx, dlq.ReplayOptions{
		Bucket:         client.Bucket(*flagBucket),
		BranchFilter:   *flagBranch,
		SkipGitHubPost: *flagSkipGitHubPost,
		Logger:         logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay failed: %v\n", err)
		os.Exit(1)
	}
	if result.Failed > 0 {
		os.Exit(1)
	}
}
