// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// TestSpec is a spec for a roachtest.
type TestSpec struct {
	Skip string // if non-empty, test will be skipped
	// When Skip is set, this can contain more text to be printed in the logs
	// after the "--- SKIP" line.
	SkipDetails string

	Name string
	// Owner is the name of the team responsible for signing off on failures of
	// this test that happen in the release process. This must be one of a limited
	// set of values (the keys in the roachtestTeams map).
	Owner Owner
	// The maximum duration the test is allowed to run before it is considered
	// failed. If not specified, the default timeout is 10m before the test's
	// associated cluster expires. The timeout is always truncated to 10m before
	// the test's cluster expires.
	Timeout time.Duration
	// Tags is a set of tags associated with the test that allow grouping
	// tests. If no tags are specified, the set ["default"] is automatically
	// given.
	Tags []string
	// Cluster provides the specification for the cluster to use for the test.
	Cluster spec.ClusterSpec

	// UseIOBarrier controls the local-ssd-no-ext4-barrier flag passed to
	// roachprod when creating a cluster. If set, the flag is not passed, and so
	// you get durable writes. If not set (the default!), the filesystem is
	// mounted without the barrier.
	//
	// The default (false) is chosen because it the no-barrier option is needed
	// explicitly by some tests (particularly benchmarks, ironically, since they'd
	// rather measure other things than I/O) and the vast majority of other tests
	// don't care - there's no durability across machine crashes that roachtests
	// care about.
	UseIOBarrier bool

	// NonReleaseBlocker indicates that a test failure should not be tagged as a
	// release blocker. Use this for tests that are not yet stable but should
	// still be run regularly.
	NonReleaseBlocker bool

	// RequiresLicense indicates that the test requires an
	// enterprise license to run correctly. Use this to ensure
	// tests will fail-early if COCKROACH_DEV_LICENSE is not set
	// in the environment.
	RequiresLicense bool

	// Run is the test function.
	Run func(ctx context.Context, t test.Test, c cluster.Cluster)
}

// MatchOrSkip returns true if the filter matches the test. If the filter does
// not match the test because the tag filter does not match, the test is
// matched, but marked as skipped.
//
// TODO(tbg): it's gross that this sets t.Skip, let the caller do this.
func (t *TestSpec) MatchOrSkip(filter *TestFilter) bool {
	if !filter.Name.MatchString(t.Name) {
		return false
	}
	if len(t.Tags) == 0 {
		if !filter.Tag.MatchString("default") {
			t.Skip = fmt.Sprintf("%s does not match [default]", filter.RawTag)
		}
		return true
	}
	for _, t := range t.Tags {
		if filter.Tag.MatchString(t) {
			return true
		}
	}
	t.Skip = fmt.Sprintf("%s does not match %s", filter.RawTag, t.Tags)
	return true
}
