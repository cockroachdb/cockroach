// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// logSink will report any inspect errors directly to cockroach.log.
type logSink struct {
	foundIssue bool
}

var _ inspectLogger = &logSink{}

// logIssue implements the inspectLogger interface.
func (c *logSink) logIssue(ctx context.Context, issue *inspectIssue) error {
	c.foundIssue = true
	log.Dev.Errorf(ctx, "inspect issue: %+v", issue)
	return nil
}

// hasIssues implements the inspectLogger interface.
func (c *logSink) hasIssues() bool {
	return c.foundIssue
}
