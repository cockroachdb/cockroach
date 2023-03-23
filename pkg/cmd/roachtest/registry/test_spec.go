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
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// LibGEOS is a list of native libraries for libgeos.
var LibGEOS = []string{"libgeos", "libgeos_c"}

// PrometheusNameSpace is the namespace which all metrics exposed on the roachtest
// endpoint should use.
var PrometheusNameSpace = "roachtest"

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
	// NativeLibs specifies the native libraries required to be present on
	// the cluster during execution.
	NativeLibs []string

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

	// EncryptionSupport encodes to what extent tests supports
	// encryption-at-rest. See the EncryptionSupport type for details.
	// Encryption support is opt-in -- i.e., if the TestSpec does not
	// pass a value to this field, it will be assumed that the test
	// cannot be run with encryption enabled.
	EncryptionSupport EncryptionSupport

	// SkipPostValidations is a bit-set of post-validations that should be skipped
	// after the test completes. This is useful for tests that are known to be
	// incompatible with some validations. By default, tests will run all
	// validations.
	SkipPostValidations PostValidation

	// Run is the test function.
	Run func(ctx context.Context, t test.Test, c cluster.Cluster)
}

// PostValidation is a type of post-validation that runs after a test completes.
type PostValidation int

const (
	// PostValidationReplicaDivergence detects replica divergence (i.e. ranges in
	// which replicas have arrived at the same log position with different
	// states).
	PostValidationReplicaDivergence PostValidation = 1 << iota
	// PostValidationInvalidDescriptors checks if there exists any descriptors in
	// the crdb_internal.invalid_objects virtual table.
	PostValidationInvalidDescriptors
)

// MatchType is the type of match a file has to a TestFilter.
type MatchType int

const (
	// Matched means that the file passes the filter and the tags.
	Matched MatchType = iota
	// FailedFilter means that the file fails the filter.
	FailedFilter
	// FailedTags means that the file passed the filter but failed the tags
	// match.
	FailedTags
)

// Match returns Matched if the filter matches the test. If the filter does
// not match the test because the tag filter does not match, the test is
// marked as FailedTags.
func (t *TestSpec) Match(filter *TestFilter) MatchType {
	if !filter.Name.MatchString(t.Name) {
		return FailedFilter
	}
	if len(t.Tags) == 0 {
		if !filter.Tag.MatchString("default") {
			return FailedTags
		}
		return Matched
	}
	for _, t := range t.Tags {
		if filter.Tag.MatchString(t) {
			return Matched
		}
	}
	return FailedTags
}

// PromSub replaces all non prometheus friendly chars with "_". Note,
// before creating a metric, read up on prom metric naming conventions:
// https://prometheus.io/docs/practices/naming/
func PromSub(raw string) string {
	invalidPromRE := regexp.MustCompile("[^a-zA-Z0-9_]")
	return invalidPromRE.ReplaceAllLiteralString(raw, "_")
}
