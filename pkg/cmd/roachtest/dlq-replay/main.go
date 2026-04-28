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
//	--dry-run         claim and verify entries without posting
//
// GCS authentication uses Application Default Credentials.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/dlq"
)

var (
	flagBucket = flag.String("bucket", "", "GCS bucket containing the DLQ (required)")
	flagBranch = flag.String("branch", "", "only process entries under failed/<branch>/")
	flagDryRun = flag.Bool("dry-run", false, "claim and verify entries without posting")
)

func main() {
	flag.Parse()
	if *flagBucket == "" {
		fmt.Fprintln(os.Stderr, "--bucket is required")
		flag.Usage()
		os.Exit(2)
	}
	if !*flagDryRun && os.Getenv("GITHUB_API_TOKEN") == "" {
		fmt.Fprintln(os.Stderr,
			"GITHUB_API_TOKEN env var is required (use --dry-run to skip posting)")
		os.Exit(2)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("creating GCS client: %v", err)
	}
	defer func() { _ = client.Close() }()

	logger := stdoutLogger{}
	logger.Printf("dlq-replay starting: bucket=%s branch=%q dry_run=%v",
		*flagBucket, *flagBranch, *flagDryRun)

	result, err := dlq.Replay(ctx, dlq.ReplayOptions{
		Bucket:       client.Bucket(*flagBucket),
		BranchFilter: *flagBranch,
		DryRun:       *flagDryRun,
		Logger:       logger,
	})
	if err != nil {
		log.Fatalf("replay failed: %v", err)
	}
	if result.Failed > 0 {
		os.Exit(1)
	}
}

// stdoutLogger writes one line per call to stdout.
type stdoutLogger struct{}

func (stdoutLogger) Printf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, format+"\n", args...)
}
