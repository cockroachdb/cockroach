// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"github.com/bufbuild/buf/private/pkg/normalpath"
)

// Matcher is a path matcher.
//
// This will cause a Bucket to operate as if it only contains matching paths.
type Matcher interface {
	Mapper
	isMatcher()
}

// MatchPathExt returns a Matcher for the extension.
func MatchPathExt(ext string) Matcher {
	return pathMatcherFunc(func(path string) bool {
		return normalpath.Ext(path) == ext
	})
}

// MatchPathEqual returns a Matcher for the path.
func MatchPathEqual(equalPath string) Matcher {
	return pathMatcherFunc(func(path string) bool {
		return path == equalPath
	})
}

// MatchPathEqualOrContained returns a Matcher for the path that matches
// on paths equal or contained by equalOrContainingPath.
func MatchPathEqualOrContained(equalOrContainingPath string) Matcher {
	return pathMatcherFunc(func(path string) bool {
		return normalpath.EqualsOrContainsPath(equalOrContainingPath, path, normalpath.Relative)
	})
}

// MatchPathContained returns a Matcher for the directory that matches
// on paths by contained by containingDir.
func MatchPathContained(containingDir string) Matcher {
	return pathMatcherFunc(func(path string) bool {
		return normalpath.ContainsPath(containingDir, path, normalpath.Relative)
	})
}

// MatchOr returns an Or of the Matchers.
func MatchOr(matchers ...Matcher) Matcher {
	return orMatcher(matchers)
}

// MatchAnd returns an And of the Matchers.
func MatchAnd(matchers ...Matcher) Matcher {
	return andMatcher(matchers)
}

// MatchNot returns an Not of the Matcher.
func MatchNot(matcher Matcher) Matcher {
	return notMatcher{matcher}
}

// ***** private *****

// We limit or/and/not to Matchers as composite logic must assume
// the the input path is not modified, so that we can always return it
//
// We might want to just remove Matcher implementing Mapper for simplification,
// and just have a Matches function, then handle chaining them separately.

type pathMatcherFunc func(string) bool

func (f pathMatcherFunc) MapPath(path string) (string, bool) {
	matches := f(path)
	return path, matches
}

func (f pathMatcherFunc) MapPrefix(prefix string) (string, bool) {
	// always returns true, path matchers do not check prefixes
	return prefix, true
}

func (f pathMatcherFunc) UnmapFullPath(fullPath string) (string, bool, error) {
	matches := f(fullPath)
	return fullPath, matches, nil
}

func (pathMatcherFunc) isMatcher() {}
func (pathMatcherFunc) isMapper()  {}

type orMatcher []Matcher

func (o orMatcher) MapPath(path string) (string, bool) {
	for _, matcher := range o {
		if _, matches := matcher.MapPath(path); matches {
			return path, true
		}
	}
	return "", false
}

func (o orMatcher) MapPrefix(prefix string) (string, bool) {
	for _, matcher := range o {
		if _, matches := matcher.MapPrefix(prefix); matches {
			return prefix, true
		}
	}
	return "", false
}

func (o orMatcher) UnmapFullPath(fullPath string) (string, bool, error) {
	for _, matcher := range o {
		_, matches, err := matcher.UnmapFullPath(fullPath)
		if err != nil {
			return "", false, err
		}
		if matches {
			return fullPath, true, nil
		}
	}
	return fullPath, false, nil
}

func (orMatcher) isMatcher() {}
func (orMatcher) isMapper()  {}

type andMatcher []Matcher

func (a andMatcher) MapPath(path string) (string, bool) {
	for _, matcher := range a {
		if _, matches := matcher.MapPath(path); !matches {
			return path, false
		}
	}
	return path, true
}

func (a andMatcher) MapPrefix(prefix string) (string, bool) {
	for _, matcher := range a {
		if _, matches := matcher.MapPrefix(prefix); !matches {
			return prefix, false
		}
	}
	return prefix, true
}

func (a andMatcher) UnmapFullPath(fullPath string) (string, bool, error) {
	for _, matcher := range a {
		_, matches, err := matcher.UnmapFullPath(fullPath)
		if err != nil {
			return "", false, err
		}
		if !matches {
			return fullPath, false, nil
		}
	}
	return fullPath, true, nil
}

func (andMatcher) isMatcher() {}
func (andMatcher) isMapper()  {}

type notMatcher struct {
	delegate Matcher
}

func (n notMatcher) MapPath(path string) (string, bool) {
	_, matches := n.delegate.MapPath(path)
	return path, !matches
}

func (n notMatcher) MapPrefix(prefix string) (string, bool) {
	_, matches := n.delegate.MapPath(prefix)
	return prefix, !matches
}

func (n notMatcher) UnmapFullPath(fullPath string) (string, bool, error) {
	_, matches, err := n.delegate.UnmapFullPath(fullPath)
	if err != nil {
		return "", false, err
	}
	return fullPath, !matches, nil
}

func (notMatcher) isMatcher() {}
func (notMatcher) isMapper()  {}
