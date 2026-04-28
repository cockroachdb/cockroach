// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is a throwaway local test server for the DLQ replay function.
// Usage:
//
//	GITHUB_DLQ_REPLAY_DRY_RUN=1 GITHUB_DLQ_BUCKET=roachtest-github-dlq-dev ./dev build pkg/cmd/roachtest/dlq/test_server
//	GITHUB_DLQ_REPLAY_DRY_RUN=1 GITHUB_DLQ_BUCKET=roachtest-github-dlq-dev ./bin/test_server
//	curl localhost:8080
package main

import (
	"log"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/dlq"
)

func main() {
	http.HandleFunc("/", dlq.ReplayDLQ)
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
