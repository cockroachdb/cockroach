// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os"
)

// summaryWriter appends Markdown blocks to the GitHub Actions job summary
// file, when one is configured. The release binary runs inside the bazel
// docker container, which can't see $GITHUB_STEP_SUMMARY, so the GHA wrapper
// points path at the mounted /artifacts dir and folds the file into the job
// summary after the run — surfacing per-ticket outcomes there rather than
// only in the logs.
//
// It is embedded in each command's runner so callers write blocks via
// r.append(...). Best-effort by design: a no-op when path is empty (local or
// dry runs without the wrapper), and write failures are logged, never fatal —
// the summary is informational and must not fail the run.
type summaryWriter struct {
	// path names the Markdown file to append to, or "" to disable summary
	// writing entirely. Set from the command's --summary-file flag.
	path string
}

// append writes block to the summary file, creating it if necessary. A no-op
// when no summary file is configured. Failures are logged and swallowed — the
// summary is informational and must never affect the workflow's exit code.
func (s summaryWriter) append(block string) {
	if s.path == "" {
		return
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("failed to open summary file %s: %v", s.path, err)
		return
	}
	defer func() {
		// Close flushes buffered writes, so a full /artifacts mount can
		// surface here even when WriteString reported success. Log it to honor
		// the "write failures are logged" contract; it remains non-fatal.
		if cerr := f.Close(); cerr != nil {
			log.Printf("failed to close summary file %s: %v", s.path, cerr)
		}
	}()
	if _, err := f.WriteString(block); err != nil {
		log.Printf("failed to write summary file %s: %v", s.path, err)
	}
}

// shortSHA returns the first 12 characters of a commit SHA for display, or the
// whole string if it's shorter (e.g. a test fixture).
func shortSHA(sha string) string {
	if len(sha) <= 12 {
		return sha
	}
	return sha[:12]
}

// commitURL builds the github.com commit link for a SHA in repo (owner/name).
// Used in summary blocks so an operator can jump straight to the commit.
func commitURL(repo, sha string) string {
	return fmt.Sprintf("https://github.com/%s/commit/%s", repo, sha)
}
