// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package codeowners

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/internal/reporoot"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/errors"
	"github.com/zabawaba99/go-gitignore"
)

// Rule is a single rule within a CODEOWNERS file.
type Rule struct {
	Pattern string
	Owners  []team.Alias
}

// CodeOwners is a struct encapsulating a CODEOWNERS file.
type CodeOwners struct {
	rules []Rule
	teams map[team.Alias]team.Team
}

// GetTeamForAlias returns the team matching the given Alias.
func (co *CodeOwners) GetTeamForAlias(alias team.Alias) team.Team {
	return co.teams[alias]
}

// LoadCodeOwners parses a CODEOWNERS file and returns the CodeOwners struct.
func LoadCodeOwners(r io.Reader, teams map[team.Alias]team.Team) (*CodeOwners, error) {
	s := bufio.NewScanner(r)
	ret := &CodeOwners{
		teams: teams,
	}
	lineNum := 1
	for s.Scan() {
		lineNum++
		if s.Err() != nil {
			return nil, s.Err()
		}
		t := strings.Replace(s.Text(), "#!", "", -1)
		if strings.HasPrefix(t, "#") {
			continue
		}
		t = strings.TrimSpace(t)
		if len(t) == 0 {
			continue
		}

		fields := strings.Fields(t)
		rule := Rule{Pattern: fields[0]}
		for _, field := range fields[1:] {
			// @cockroachdb/kv[-noreview] --> cockroachdb/kv.
			owner := team.Alias(strings.TrimSuffix(strings.TrimPrefix(field, "@"), "-noreview"))

			if _, ok := teams[owner]; !ok {
				return nil, errors.Newf("owner %s does not exist", owner)
			}
			rule.Owners = append(rule.Owners, owner)
		}
		ret.rules = append(ret.rules, rule)
	}
	return ret, nil
}

// DefaultLoadCodeOwners loads teams from .github/CODEOWNERS
// (relative to the repo root).
func DefaultLoadCodeOwners() (*CodeOwners, error) {
	teams, err := team.DefaultLoadTeams()
	if err != nil {
		return nil, err
	}
	var dirPath string
	if os.Getenv("BAZEL_TEST") != "" {
		// NB: The test needs to depend on the CODEOWNERS file.
		var err error
		dirPath, err = bazel.RunfilesPath()
		if err != nil {
			return nil, err
		}
	} else {
		dirPath = reporoot.GetFor(".", ".github/CODEOWNERS")
	}
	if dirPath == "" {
		return nil, errors.Errorf("CODEOWNERS not found")
	}
	path := filepath.Join(dirPath, ".github", "CODEOWNERS")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return LoadCodeOwners(f, teams)
}

// Match matches the given file to the rules and returns the owning team(s),
// according to the supplied *relative* path (which must be relative to the
// repository root, i.e. like the CODEOWNERS file).
//
// Returns the zero value if there are no owning teams or the passed-in path is
// absolute.
func (co *CodeOwners) Match(filePath string) []team.Team {
	if filepath.IsAbs(filePath) {
		// NB: don't try to look up the repository root here.
		// The callers should do that.
		return nil
	}
	filePath = string(filepath.Separator) + filepath.Clean(filePath)
	// filePath is now "absolute relative to the repo root", i.e.
	// `/pkg/acceptance`, and the cleaning helps avoid inputs like
	// `./pkg/acceptance` not working (when `pkg/acceptance` would).
	//
	// Keep matching until we hit the root directory.
	lastFilePath := ""
	for filePath != lastFilePath {
		// Rules are matched backwards.
		for i := len(co.rules) - 1; i >= 0; i-- {
			rule := co.rules[i]
			// For subdirectories, CODEOWNERS will add ** automatically for matches.
			// As such, if the pattern ends with a directory (i.e. '/'), add the ** operator implicitly.
			if gitignore.Match(rule.Pattern, filePath) {
				teams := make([]team.Team, len(rule.Owners))
				for i, owner := range rule.Owners {
					teams[i] = co.teams[owner]
				}
				return teams
			}
		}
		lastFilePath = filePath
		filePath = filepath.Dir(filePath)
	}
	return nil
}

// GetTestOwner looks up the file containing the given test and returns
// the owning teams. It does not return errors, but instead simply returns what
// it can. In case no owning team is found, "test-eng" team is returned.
// In addition, we return some diagnostic strings that can be logged or
// discarded.
func (co *CodeOwners) GetTestOwner(
	packageName, testName string,
) (_teams []team.Team, _logs []string) {
	var filename string
	var err error
	filename, _logs, err = getFile(packageName, testName)
	if err != nil {
		_logs = append(_logs, fmt.Sprintf("error getting file:line for %s.%s: %s", packageName, testName, err))
		// Let's continue so that we can assign the "catch-all" owner.
	}
	_teams = co.Match(filename)

	if _teams == nil {
		// N.B. if no owning team is found, we default to 'test-eng'. This should be a rare exception rather than the rule.
		testEng := co.GetTeamForAlias("cockroachdb/test-eng")
		if testEng.Name() == "" {
			panic("test-eng team could not be found in TEAMS.yaml")
		}

		_logs = append(_logs, fmt.Sprintf("assigning %s.%s to 'test-eng' as catch-all", packageName, testName))
		_teams = []team.Team{testEng}
	}
	return
}

type PackageAndTest struct {
	PackageName, TestName string
}

// This should be set to some non-nil map in test scenarios. This is used to
// mock out the results of the getFile function.
var FileMapForTesting map[PackageAndTest]string

// getFile returns the file (relative to repo root) for the given test. The
// package name is assumed relative to the repo root as well, i.e. pkg/foo/bar.
// Also returns logs to optionally print.
func getFile(packageName, testName string) (res string, logs []string, err error) {
	if FileMapForTesting != nil {
		var ok bool
		res, ok = FileMapForTesting[PackageAndTest{PackageName: packageName, TestName: testName}]
		if ok {
			return
		}
		return "", nil, errors.Newf("could not find testing line:num for %s.%s", packageName, testName)
	}
	// Search the source code for the email address of the last committer to touch
	// the first line of the source code that contains testName. Then, ask GitHub
	// for the GitHub username of the user with that email address by searching
	// commits in cockroachdb/cockroach for commits authored by the address.
	subtests := strings.Split(testName, "/")
	testName = subtests[0]
	packageName = strings.TrimPrefix(packageName, "github.com/cockroachdb/cockroach/")
	for {
		if !strings.Contains(packageName, "pkg") {
			err = errors.Newf("could not find test %s", testName)
			return
		}
		cmd := exec.Command(`/bin/bash`, `-c`,
			fmt.Sprintf(`cd "$(git rev-parse --show-toplevel)" && git grep --no-index -n 'func %s(' '%s/*_test.go'`,
				testName, packageName))
		// This command returns output such as:
		// ../ccl/storageccl/export_test.go:31:func TestExportCmd(t *testing.T) {
		out, err_ := cmd.CombinedOutput()
		if err_ != nil {
			logs = append(logs,
				fmt.Sprintf("couldn't find test %s in %s: %s %+v",
					testName, packageName, string(out), err_))
			packageName = filepath.Dir(packageName)
			continue
		}
		re := regexp.MustCompile(`(.*):(.*):`)
		// The first 2 :-delimited fields are the filename and line number.
		matches := re.FindSubmatch(out)
		if matches == nil {
			logs = append(logs,
				fmt.Sprintf("couldn't find filename/line number for test %s in %s: %s %+v",
					testName, packageName, string(out), err_))
			packageName = filepath.Dir(packageName)
			continue
		}
		res = string(matches[1])
		return
	}
}
