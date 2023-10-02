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
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// TestFilter holds the name and tag filters for filtering tests.
// See NewTestFilter.
type TestFilter struct {
	Name *regexp.Regexp

	// Cloud, if set, restricts the set of tests to those compatible with this cloud.
	Cloud string

	// Suite, if set, restricts the set of tests to those that are part of this suite.
	Suite string

	// Owner, if set, restricts the set of tests to those with this owner.
	Owner Owner

	// OnlyBenchmarks, if set, restricts the set of tests to benchmarks.
	OnlyBenchmarks bool

	// Multiple `tag:` parameters can be passed for which only one needs to match, but the
	// value of a single `tag:` parameter can be a comma-separated list of tags which all need
	// to match.
	// e.g. `tag:foo,bar` matches tests with tags `foo` and `bar`, and `tag:foo tag:bar` matches
	// tests with either tag `foo` or tag `bar`.
	//
	// This set contains each tag, so the above examples would be represented as `["foo,bar"]` and
	// `["foo", "bar"]` respectively..
	Tags map[string]struct{}
}

// TestFilterOption can be passed to NewTestFilter.
type TestFilterOption func(tf *TestFilter)

// WithCloud restricts the set of tests to those compatible with this cloud.
func WithCloud(cloud string) TestFilterOption {
	return func(tf *TestFilter) { tf.Cloud = cloud }
}

// WithSuite restricts the set of tests to those that are part of this suite.
func WithSuite(suite string) TestFilterOption {
	return func(tf *TestFilter) { tf.Suite = suite }
}

// WithOwner restricts the set of tests to those with this owner.
func WithOwner(owner Owner) TestFilterOption {
	return func(tf *TestFilter) { tf.Owner = owner }
}

func OnlyBenchmarks() TestFilterOption {
	return func(tf *TestFilter) { tf.OnlyBenchmarks = true }
}

// NewTestFilter initializes a new filter. The strings are interpreted as
// regular expressions. As a special case, a `tag:` prefix implies that the
// remainder of the string filters tests by tag, and not by name.
func NewTestFilter(regexps []string, options ...TestFilterOption) *TestFilter {
	var name []string
	tags := make(map[string]struct{})
	for _, v := range regexps {
		if strings.HasPrefix(v, "tag:") {
			tags[strings.TrimPrefix(v, "tag:")] = struct{}{}
		} else {
			name = append(name, v)
		}
	}

	makeRE := func(strs []string) *regexp.Regexp {
		switch len(strs) {
		case 0:
			return regexp.MustCompile(`.`)
		case 1:
			return regexp.MustCompile(strs[0])
		default:
			for i := range strs {
				strs[i] = "(" + strs[i] + ")"
			}
			return regexp.MustCompile(strings.Join(strs, "|"))
		}
	}

	tf := &TestFilter{
		Name: makeRE(name),
		Tags: tags,
	}
	for _, o := range options {
		o(tf)
	}
	return tf
}

// MatchFailReason describes the reason(s) a filter did not match the test.
type MatchFailReason struct {
	// If true, the filter requires a benchmark and the test is not one.
	IsNotBenchmark bool
	// If true, the test name does not match the filter regexp.
	NameMismatch bool
	// If true, the test owner does not match the owner in the filter.
	OwnerMismatch bool
	// If true, the test is not part of the suite in the filter.
	NotPartOfSuite bool
	// If true, the test is not compatible with the cloud in the filter.
	CloudNotCompatible bool
	// If true, the tags don't match those in the filter.
	TagsMismatch bool
}

// Matches returns true if the filter matches the test. If the test doesn't
// match, returns the reason(s).
func (filter *TestFilter) Matches(t *TestSpec) (matches bool, reason MatchFailReason) {
	reason.IsNotBenchmark = filter.OnlyBenchmarks && !t.Benchmark
	reason.NameMismatch = !filter.Name.MatchString(t.Name)
	reason.OwnerMismatch = filter.Owner != "" && t.Owner != filter.Owner
	reason.NotPartOfSuite = filter.Suite != "" && !t.Suites.Contains(filter.Suite)
	reason.CloudNotCompatible = filter.Cloud != "" && !t.CompatibleClouds.Contains(filter.Cloud)

	if len(filter.Tags) > 0 {
		// Tags will go away soon.
		matchesTags := func() bool {
			for tag := range filter.Tags {
				// If the tag is a single CSV e.g. "foo,bar,baz", we match all the tags
				if matchesAll(t.Tags, strings.Split(tag, ",")) {
					return true
				}
			}
			return false
		}()
		if !matchesTags {
			reason.TagsMismatch = true
		}
	}
	// We have a match if all fields are false.
	return reason == MatchFailReason{}, reason
}

// MatchFailReasonString returns a user-friendly string describing the reason(s)
// a filter failed to match a test (returned by Matches). Returns the empty
// string if the reason is zero.
//
// Sample results:
//   - does not match regex "foo"
//   - does not match regex "foo" and is not part of the "nightly" suite
//   - does not match regex "foo", is not part of the "nightly" suite, and is
//     not compatible with cloud "gce"
func (filter *TestFilter) MatchFailReasonString(r MatchFailReason) string {
	var reasons []string
	appendIf := func(b bool, format string, args ...interface{}) {
		if b {
			reasons = append(reasons, fmt.Sprintf(format, args...))
		}
	}
	appendIf(r.IsNotBenchmark, "is not a benchmark")
	appendIf(r.NameMismatch, "does not match regex %q", filter.Name)
	appendIf(r.OwnerMismatch, "does not have owner %q", filter.Owner)
	appendIf(r.NotPartOfSuite, "is not part of the %q suite", filter.Suite)
	appendIf(r.CloudNotCompatible, "is not compatible with %s", filter.cloudStr())
	appendIf(r.TagsMismatch, "does not match tags")

	if len(reasons) <= 2 {
		// 0 reasons: ""
		// 1 reason:  "reason0"
		// 2 reasons: "reason0 and reason1"
		return strings.Join(reasons, " and ")
	}
	// 3 or more reasons: "reason0, reason1, reason2, and reason3
	return strings.Join(reasons[:len(reasons)-1], ", ") + ", and " + reasons[len(reasons)-1]
}

// Filter returns the test specs in the given list that match the filter (in the
// same order).
func (filter *TestFilter) Filter(tests []TestSpec) []TestSpec {
	var res []TestSpec
	for i := range tests {
		if ok, _ := filter.Matches(&tests[i]); ok {
			res = append(res, tests[i])
		}
	}
	return res
}

// NoMatchesHint identifies some common situations when a filter matches no tests.
//
// The hints help us produce helpful error message. We want to zero in on most
// common problems. For example, if there is a typo in one aspect of the filter,
// we want the message to highlight that.
type NoMatchesHint int

const (
	// WrongCloud indicates that some tests match all aspects of the filter except
	// the cloud. Since cloud compatibility was added recently, we expect this to
	// be a common case.
	WrongCloud = 1 + iota

	// NoSuchName indicates tha the name regexp is invalid, i.e. no tests (with
	// any owner, suite, cloud, etc) match the regexp.
	NoSuchName

	// NoSuchSuite indicates that the suite is invalid, i.e. there are no tests
	// (with ay name, owner, cloud, etc) that match the suite.
	NoSuchSuite

	// NoTestsWithNameAndSuite indicates that there are no tests that match both
	// the name regexp and the suite.
	NoTestsWithNameAndSuite

	// NoSuchOwner indicates that the owner is invalid, i.e. there are no tests
	// (with any name, suite, cloud, etc) that match the owner.
	NoSuchOwner

	// NoTestsWithNameAndOwner indicates that there are no tests that match both
	// the name regexp and the owner.
	NoTestsWithNameAndOwner

	// NoHint indicates any other situation that leads to a match failure.
	NoHint
)

// FilterWithHint returns the test specs in the given list that match the filter
// (in the same order). If there are no matches, returns a hint about what might
// be wrong with the filter.
func (filter *TestFilter) FilterWithHint(tests []TestSpec) ([]TestSpec, NoMatchesHint) {
	res := filter.Filter(tests)
	if len(res) > 0 {
		return res, 0
	}

	// noFilter matches all tests/benchmarks.
	noFilter := TestFilter{Name: regexp.MustCompile(`.`), OnlyBenchmarks: filter.OnlyBenchmarks}

	// 1. Is the cloud likely the problem?
	// We want to see if the desired tests exist but are not compatible with the
	// given cloud (which is a recent feature). We use all fields from the
	// original filter except the cloud and see if we get matches.
	if filter.Cloud != "" {
		noCloudFilter := *filter
		noCloudFilter.Cloud = ""
		if n := len(noCloudFilter.Filter(tests)); n > 0 {
			return nil, WrongCloud
		}
	}

	// In the checks below, the idea is to create a relaxed filter - one that only
	// sets one or two fields from the original filter - and see if that still
	// gets us no matches - in which chase that small number of fields are the
	// problem.

	// 2. Is the Name regexp the problem?
	// We check if no tests match the regexp, in which case, the regexp is the problem.
	if filter.Name.String() != "." {
		nameOnlyFilter := noFilter
		nameOnlyFilter.Name = filter.Name
		if len(nameOnlyFilter.Filter(tests)) == 0 {
			return nil, NoSuchName
		}
	}

	// Check potential problems related to the suite.
	if filter.Suite != "" {
		// 3. Is the suite incorrect?
		// We check if no tests match the suite, in which case the suite is the problem.
		suiteOnlyFilter := noFilter
		suiteOnlyFilter.Suite = filter.Suite
		if len(suiteOnlyFilter.Filter(tests)) == 0 {
			return nil, NoSuchSuite
		}

		// 4. Is the suite+regexp incorrect?
		// We check if no tests match the suite AND the regexp.
		suiteAndNameFilter := suiteOnlyFilter
		suiteAndNameFilter.Name = filter.Name
		if len(suiteAndNameFilter.Filter(tests)) == 0 {
			return nil, NoTestsWithNameAndSuite
		}
	}

	// Check potential problems related to the owner.
	if filter.Owner != "" {
		// 5. Is the owner incorrect?
		// We check if no tests match the owner, in which case the owner is the problem.
		ownerOnlyFilter := noFilter
		ownerOnlyFilter.Owner = filter.Owner
		if len(ownerOnlyFilter.Filter(tests)) == 0 {
			return nil, NoSuchOwner
		}
		// 6. Is the owner+regexp incorrect?
		// We check if no tests match the owner AND the regexp.
		ownerAndNameFilter := ownerOnlyFilter
		ownerAndNameFilter.Name = filter.Name
		if len(ownerAndNameFilter.Filter(tests)) == 0 {
			return nil, NoTestsWithNameAndOwner
		}
	}

	// We failed to produce a useful message. It's an uncommon combination of
	// criteria that leads to no tests matching.
	return nil, NoHint
}

// NoMatchesHintString returns a user-friendly string describing the hint.
func (filter *TestFilter) NoMatchesHintString(h NoMatchesHint) string {
	noun := filter.noun()
	switch h {
	case WrongCloud:
		return fmt.Sprintf(
			"no %s match criteria; some %s match but are not compatible with %s",
			noun, noun, filter.cloudStr(),
		)
	case NoSuchName:
		return fmt.Sprintf("no %s match regexp %q", noun, filter.Name)
	case NoSuchSuite:
		return fmt.Sprintf("no %s in suite %q", noun, filter.Suite)
	case NoTestsWithNameAndSuite:
		return fmt.Sprintf("no %s in suite %q match regexp %q", noun, filter.Suite, filter.Name)
	case NoSuchOwner:
		return fmt.Sprintf("no %s with owner %q", noun, filter.Owner)
	case NoTestsWithNameAndOwner:
		return fmt.Sprintf("no %s with owner %q match regexp %q", noun, filter.Owner, filter.Name)
	default:
		return fmt.Sprintf("no %s match criteria", noun)
	}
}

// String returns a possibly multi-line string that describes the filter in a
// user-friendly way. A verb like "Listing " or "Running " can be prepended.
//
// Examples:
// 1. all tests
// 2. all benchmarks matching regexp "foo"
// 3. all tests compatible with GCE
// 4. tests which:
//   - are compatible with GCE, and
//   - have owner "foo", and
//   - match regexp "bar"
func (filter *TestFilter) String() string {
	var criteria []string
	appendIf := func(test bool, format string, args ...interface{}) {
		if test {
			criteria = append(criteria, fmt.Sprintf(format, args...))
		}
	}
	appendIf(filter.Name.String() != ".", "match regex %q", filter.Name)
	appendIf(filter.Cloud != "", "are compatible with %s", filter.cloudStr())
	appendIf(filter.Suite != "", "are part of the %q suite", filter.Suite)
	appendIf(filter.Owner != "", "have owner %q", filter.Owner)

	if len(filter.Tags) > 0 {
		var tags []string
		for tag := range filter.Tags {
			tags = append(tags, tag)
		}
		sort.Strings(tags)
		tagsStr := strings.Join(tags, " OR ")
		appendIf(true, "match tag(s) %s", tagsStr)
	}

	noun := filter.noun()
	switch len(criteria) {
	case 0:
		// Example: "all tests"
		return "all " + noun
	case 1:
		// Example: "all tests which are compatible with GCE"
		return noun + " which " + criteria[0]
	case 2:
		// Example: "all tests which match regex "foo" and have owner "bar"
		return noun + " which " + strings.Join(criteria, " and ")
	default:
		// Example:
		// 	tests which:
		// 	 - match regex "foo", and
		// 	 - are compatible with GCE, and
		// 	 - are part of the "nightly" suite
		return "tests which:\n - " + strings.Join(criteria, ", and\n - ")
	}
}

// cloudStr returns the cloud as a user-friendly string.
func (filter *TestFilter) cloudStr() string {
	if filter.Cloud == "" {
		return ""
	}
	runes := []rune(strings.ToLower(filter.Cloud))
	if len(runes) < 4 {
		// e.g. GCE, AWS
		for i := range runes {
			runes[i] = unicode.ToUpper(runes[i])
		}
	} else {
		// e.g. Local, Azure
		runes[0] = unicode.ToUpper(runes[0])
	}
	return string(runes)
}

// noun returns "tests" or "benchmarks" depending on the OnlyBenchmarks field.
func (filter *TestFilter) noun() string {
	if filter.OnlyBenchmarks {
		return "benchmarks"
	}
	return "tests"
}
