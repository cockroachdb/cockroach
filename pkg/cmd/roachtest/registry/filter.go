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

// Matches returns true if the filter matches the test. If not, returns a
// reason, for example:
//   - does not match regex "foo"
//   - does not match regex "foo" and is not part of the "nightly" suite
//   - does not match regex "foo", is not part of the "nightly" suite, and is
//     not compatible with cloud "gce"
func (filter *TestFilter) Matches(t *TestSpec) (matches bool, reason string) {
	var reasons []string
	if filter.OnlyBenchmarks && !t.Benchmark {
		reasons = append(reasons, "is not a benchmark")
	}
	if !filter.Name.MatchString(t.Name) {
		reasons = append(reasons, fmt.Sprintf("does not match regex %q", filter.Name))
	}

	if filter.Owner != "" && t.Owner != filter.Owner {
		reasons = append(reasons, fmt.Sprintf("does not have owner %q", filter.Owner))
	}

	if filter.Suite != "" && !t.Suites.Contains(filter.Suite) {
		reasons = append(reasons, fmt.Sprintf("is not part of the %q suite", filter.Suite))
	}

	if filter.Cloud != "" && !t.CompatibleClouds.Contains(filter.Cloud) {
		reasons = append(reasons, fmt.Sprintf("is not compatible with cloud %q", filter.Cloud))
	}

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
			reasons = append(reasons, "does not match tags")
		}
	}

	switch n := len(reasons); n {
	case 0:
		return true, ""
	case 1:
		return false, reasons[0]
	case 2:
		return false, reasons[0] + " and " + reasons[1]
	default:
		return false, strings.Join(reasons[:n-1], ", ") + ", and " + reasons[n-1]
	}
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

// FilterWithErr returns the test specs in the given list that match the filter
// (in the same order). If there are no matches, returns an error message that
// contains helpful information in most cases.
func (filter *TestFilter) FilterWithErr(tests []TestSpec) ([]TestSpec, error) {
	res := filter.Filter(tests)
	if len(res) > 0 {
		return res, nil
	}

	// Try to produce a helpful error message. We want to zero in on most common
	// problems. For example, if there is a typo in one aspect of the filter, we
	// want the error to highlight that.
	//
	// In most of the checks below, the idea is to create a relaxed filter - one
	// that only sets one or two fields from the original filter - and see if that
	// still gets us no matches - in which chase that small number of fields are
	// the problem.

	noun := filter.noun()

	// noFilter matches all tests/benchmarks.
	noFilter := TestFilter{Name: regexp.MustCompile(`.`), OnlyBenchmarks: filter.OnlyBenchmarks}

	// 1. Is the Name regexp the problem?
	// We check if no tests match the regexp, in which case, the regexp is the problem.
	if filter.Name.String() != "." {
		nameOnlyFilter := noFilter
		nameOnlyFilter.Name = filter.Name
		if len(nameOnlyFilter.Filter(tests)) == 0 {
			return nil, fmt.Errorf("no %s match regexp %q", noun, filter.Name)
		}
	}

	// Check potential problems related to the suite.
	if filter.Suite != "" {
		// 2. Is the suite incorrect?
		// We check if no tests match the suite, in which case the suite is the problem.
		suiteOnlyFilter := noFilter
		suiteOnlyFilter.Suite = filter.Suite
		if len(suiteOnlyFilter.Filter(tests)) == 0 {
			return nil, fmt.Errorf("no %s in suite %q", noun, filter.Suite)
		}

		// 3. Is the suite+regexp incorrect?
		// We check if no tests match the suite AND the regexp.
		suiteAndNameFilter := suiteOnlyFilter
		suiteAndNameFilter.Name = filter.Name
		if len(suiteAndNameFilter.Filter(tests)) == 0 {
			return nil, fmt.Errorf("no %s in suite %q match regexp %q", noun, filter.Suite, filter.Name)
		}
	}

	// Check potential problems related to the owner.
	if filter.Owner != "" {
		// 4. Is the owner incorrect?
		// We check if no tests match the owner, in which case the owner is the problem.
		ownerOnlyFilter := noFilter
		ownerOnlyFilter.Owner = filter.Owner
		if len(ownerOnlyFilter.Filter(tests)) == 0 {
			return nil, fmt.Errorf("no %s with owner %q", noun, filter.Owner)
		}
		// 5. Is the owner+regexp incorrect?
		// We check if no tests match the owner AND the regexp.
		ownerAndNameFilter := ownerOnlyFilter
		ownerAndNameFilter.Name = filter.Name
		if len(ownerAndNameFilter.Filter(tests)) == 0 {
			return nil, fmt.Errorf("no %s with owner %q match regexp %q", noun, filter.Owner, filter.Name)
		}
	}

	// 6. Is the cloud likely the problem?
	// This check is a little different from the previous ones. We want to see if
	// the desired tests are not compatible with the given cloud (which is a more
	// recent feature). We use all fields from the original filter except the
	// cloud and see if we get matches.
	if filter.Cloud != "" {
		noCloudFilter := *filter
		noCloudFilter.Cloud = ""
		if n := len(noCloudFilter.Filter(tests)); n > 0 {
			return nil, fmt.Errorf(
				"no %s match criteria; %d %s match but are not compatible with %s",
				noun, n, noun, filter.cloudStr(),
			)
		}
	}

	// We failed to produce a useful message. It's an uncommon combination of
	// criteria that leads to no tests matching.
	return nil, fmt.Errorf("no %s match criteria", noun)
}

// Describe returns a multi-line string that describes the filter in a user-friendly way.
// A verb like "Listing " or "Running" can be prepended to the first line. A
// period can be prepended to the last line.
//
// Examples:
// 1. all tests
// 2. all benchmarks matching regexp "foo"
// 3. all tests compatible with GCE
// 4. tests which:
//   - are compatible with GCE, and
//   - have owner "foo", and
//   - match regexp "bar"
func (filter *TestFilter) Describe() []string {
	b2i := map[bool]int{false: 0, true: 1}
	numCriteria := b2i[filter.Name.String() != "."] +
		b2i[filter.Cloud != ""] +
		b2i[filter.Suite != ""] +
		b2i[filter.Owner != ""] +
		b2i[len(filter.Tags) > 0]

	noun := filter.noun()

	if numCriteria == 0 {
		return []string{fmt.Sprintf("all %s", noun)}
	}

	var tags []string
	for tag := range filter.Tags {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	tagsStr := strings.Join(tags, " OR ")

	// Special cases for a single criterion.
	if numCriteria == 1 {
		switch {
		case filter.Name.String() != ".":
			return []string{fmt.Sprintf("all %s matching regexp %q", noun, filter.Name)}
		case filter.Cloud != "":
			return []string{fmt.Sprintf("all %s compatible with %s", noun, filter.cloudStr())}
		case filter.Suite != "":
			return []string{fmt.Sprintf("all %s in suite %q", noun, filter.Suite)}
		case filter.Owner != "":
			return []string{fmt.Sprintf("all %s with owner %q", noun, filter.Owner)}
		case len(filter.Tags) > 0:
			return []string{fmt.Sprintf("all %s with tag(s) %s", noun, tagsStr)}
		}
	}

	// Special cases for cloud + one other criterion.
	if numCriteria == 2 && filter.Cloud != "" {
		switch {
		case filter.Name.String() != ".":
			return []string{fmt.Sprintf("%s compatible with %s matching regexp %q", noun, filter.cloudStr(), filter.Name)}
		case filter.Suite != "":
			return []string{fmt.Sprintf("%s compatible with %s in suite %q", noun, filter.cloudStr(), filter.Suite)}
		case filter.Owner != "":
			return []string{fmt.Sprintf("%s compatible with %s with owner %q", noun, filter.cloudStr(), filter.Owner)}
		}
	}

	// List all criteria.
	res := []string{fmt.Sprintf("%s which:", noun)}
	appendIf := func(test bool, format string, args ...interface{}) {
		if test {
			res = append(res, fmt.Sprintf(" - "+format, args...))
		}
	}
	appendIf(filter.Cloud != "", "are compatible with %s", filter.cloudStr())
	appendIf(filter.Suite != "", "are part of the %q suite", filter.Suite)
	appendIf(filter.Owner != "", "have owner %q", filter.Owner)
	appendIf(filter.Name.String() != ".", "match regex %q", filter.Name)
	appendIf(len(filter.Tags) > 0, "match tag(s) %s", tagsStr)
	for i := 1; i < len(res)-1; i++ {
		res[i] = res[i] + ", and"
	}
	return res
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
