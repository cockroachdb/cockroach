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

package internal

import (
	"strings"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/git"
	"github.com/bufbuild/buf/private/pkg/normalpath"
)

var (
	_ ParsedGitRef = &gitRef{}

	gitSchemePrefixToGitScheme = map[string]GitScheme{
		"http://":  GitSchemeHTTP,
		"https://": GitSchemeHTTPS,
		"file://":  GitSchemeLocal,
		"ssh://":   GitSchemeSSH,
		"git://":   GitSchemeGit,
	}
)

type gitRef struct {
	format            string
	path              string
	gitScheme         GitScheme
	gitName           git.Name
	depth             uint32
	recurseSubmodules bool
	subDirPath        string
}

func newGitRef(
	format string,
	path string,
	gitName git.Name,
	depth uint32,
	recurseSubmodules bool,
	subDirPath string,
) (*gitRef, error) {
	gitScheme, path, err := getGitSchemeAndPath(format, path)
	if err != nil {
		return nil, err
	}
	if depth == 0 {
		return nil, NewDepthZeroError()
	}
	subDirPath, err = normalpath.NormalizeAndValidate(subDirPath)
	if err != nil {
		return nil, err
	}
	if subDirPath == "." {
		subDirPath = ""
	}
	return newDirectGitRef(
		format,
		path,
		gitScheme,
		gitName,
		recurseSubmodules,
		depth,
		subDirPath,
	), nil
}

func newDirectGitRef(
	format string,
	path string,
	gitScheme GitScheme,
	gitName git.Name,
	recurseSubmodules bool,
	depth uint32,
	subDirPath string,
) *gitRef {
	return &gitRef{
		format:            format,
		path:              path,
		gitScheme:         gitScheme,
		gitName:           gitName,
		depth:             depth,
		recurseSubmodules: recurseSubmodules,
		subDirPath:        subDirPath,
	}
}

func (r *gitRef) Format() string {
	return r.format
}

func (r *gitRef) Path() string {
	return r.path
}

func (r *gitRef) GitScheme() GitScheme {
	return r.gitScheme
}

func (r *gitRef) GitName() git.Name {
	return r.gitName
}

func (r *gitRef) Depth() uint32 {
	return r.depth
}

func (r *gitRef) RecurseSubmodules() bool {
	return r.recurseSubmodules
}

func (r *gitRef) SubDirPath() string {
	return r.subDirPath
}

func (*gitRef) ref()       {}
func (*gitRef) bucketRef() {}
func (*gitRef) gitRef()    {}

func getGitSchemeAndPath(format string, path string) (GitScheme, string, error) {
	if path == "" {
		return 0, "", NewNoPathError()
	}
	if app.IsDevStderr(path) {
		return 0, "", NewInvalidPathError(format, path)
	}
	if path == "-" || app.IsDevNull(path) || app.IsDevStdin(path) || app.IsDevStdout(path) {
		return 0, "", NewInvalidPathError(format, path)
	}
	for prefix, gitScheme := range gitSchemePrefixToGitScheme {
		if strings.HasPrefix(path, prefix) {
			path := strings.TrimPrefix(path, prefix)
			if gitScheme == GitSchemeLocal {
				path = normalpath.Normalize(path)
			}
			if path == "" {
				return 0, "", NewNoPathError()
			}
			return gitScheme, path, nil
		}
	}
	if strings.Contains(path, "://") {
		return 0, "", NewInvalidPathError(format, path)
	}
	return GitSchemeLocal, normalpath.Normalize(path), nil
}
