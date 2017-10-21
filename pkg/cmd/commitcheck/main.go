// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package main

import (
	"bytes"
	"fmt"
	"go/build"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ghemawat/stream"
	"github.com/pkg/errors"
)

// validScopes is a set of all valid scopes for a commit message of the form
// scope[,scope]: message. It is populated by init with every directory in crdb.
var validScopes = map[string]struct{}{
	// "*" is special and indicates "all scopes."
	"*": {},

	// Following are pseudo-scopes that don't map neatly to a directory.
	"mvcc": {},
}

// impliedScopePrefixes lists scope prefixes that may be omitted from the
// commit message. For example, including "pkg/sql/" as an implied scope prefix
// allows "pkg/sql/distsqlrun" to be shortened to "distsqlrun".
var impliedScopePrefixes = []string{
	"build/",
	"c-deps/",
	"docs/",
	"pkg/",
	"pkg/ccl/",
	"pkg/cmd/",
	"pkg/internal/",
	"pkg/sql/",
	"pkg/storage/",
	"pkg/testutils/",
	"pkg/util/",
}

// allowFileScope lists directories in which files and their basenames can be
// used directly as scopes. For example, if the "scripts" directory allows file
// scope and contains a file named "azworker.sh", both "azworker" and
// "azworker.sh" become valid scopes.
var allowFileScope = []string{
	".", ".github", "scripts",
}

func fileScopeAllowed(dir string) bool {
	for _, d := range allowFileScope {
		if d == dir {
			return true
		}
	}
	return false
}

func exit(args ...interface{}) {
	fmt.Fprint(os.Stderr, args...)
	os.Exit(1)
}

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

func init() {
	rootPkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		exit(err)
	}

	lsFiles := exec.Command("git", "ls-files")
	lsFiles.Dir = rootPkg.Dir
	stdout, err := lsFiles.StdoutPipe()
	if err != nil {
		exit(err)
	}
	stderr := new(bytes.Buffer)
	lsFiles.Stderr = stderr
	filter := stream.ReadLines(stdout)

	if err := lsFiles.Start(); err != nil {
		log.Fatal(err)
	}

	if err := stream.ForEach(filter, func(s string) {
		dir := filepath.Dir(s)
		if dir != "." {
			for _, prefix := range impliedScopePrefixes {
				validScopes[strings.TrimPrefix(dir, prefix)] = struct{}{}
			}
		}
		if fileScopeAllowed(dir) {
			base := filepath.Base(s)
			validScopes[base] = struct{}{}
			if baseNoExt := strings.TrimSuffix(base, filepath.Ext(base)); baseNoExt != "" {
				validScopes[baseNoExt] = struct{}{}
			}
		}
	}); err != nil {
		exit(err)
	}

	if err := lsFiles.Wait(); err != nil {
		exitf("%s failed: %s\n%s", strings.Join(lsFiles.Args, " "), err, stderr.String())
	}
}

// Everything after scissorsLine in a Git commit message is ignored. This
// behavior is built into Git itself [0]; when using `git commit --verbose`,
// for example, the commit message template includes a scissorsLine.
//
// [0]: https://git-scm.com/docs/git-commit#git-commit-scissors
const scissorsLine = "# ------------------------ >8 ------------------------"

func checkCommitMessage(message string) error {
	if strings.HasPrefix(message, `Revert "`) {
		return nil
	}

	lines := strings.Split(message, "\n")
	for i, line := range lines {
		if line == scissorsLine {
			break
		} else if max := 72; len(line) > max && line[0] != '#' {
			return errors.Errorf("line %d exceeds maximum line length of %d characters", i+1, max)
		}
	}
	if len(lines) > 1 && lines[1] != "" {
		return errors.New("missing blank line after summary")
	}
	summary := lines[0]

	if summary == "" {
		return errors.New("missing summary")
	}
	if summary[len(summary)-1] == '.' {
		return errors.New("summary should not end in period")
	}

	splits := strings.SplitN(summary, ":", 2)
	if len(splits) != 2 {
		return errors.New("summary does not match format `scope[,scope]: message`")
	}
	scopes, description := strings.Split(splits[0], ","), strings.TrimSpace(splits[1])

	for _, scope := range scopes {
		if trimmedScope := strings.TrimSpace(scope); trimmedScope != scope {
			return errors.Errorf("scope %q has extraneous whitespace", trimmedScope)
		}
		if _, ok := validScopes[scope]; !ok {
			return errors.Errorf("unknown scope %q", scope)
		}
	}

	if description == "" {
		return errors.New("summary missing text after colon")
	}
	if ch, _ := utf8.DecodeRuneInString(description); unicode.IsUpper(ch) {
		return errors.New("text after colon in summary should not begin with capital letter")
	}

	return nil
}

const noLintEnvVar = "COCKROACH_NO_LINT_COMMITS"

func main() {
	if len(os.Args) != 2 {
		exitf("usage: %s COMMIT-MSG-FILE\n", os.Args[0])
	}

	if _, noLint := os.LookupEnv(noLintEnvVar); noLint {
		os.Exit(0)
	}

	message, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		exitf("failed to read commit message file: %s\n", err)
	}

	if err := checkCommitMessage(string(message)); err != nil {
		fmt.Printf(`commit message warning: %s

A good commit message has the following form:

    scope[,scope]: short summary in imperative tense

    A detailed description follows the summary here, after a blank line, and
    explains the rationale for the change. It should be wrapped at 72
    characters, with blank lines between paragraphs.
		
    The scope at the beginning of the summary indicates the package (e.g.,
    "storage") primarily impacted by the change. If multiple packages are
    affected, separate them with commas and no whitespace. Commits which affect
    no package in particular can use the special scope "*". Note that certain
    obvious prefixes ("pkg/", "pkg/sql/", "scripts/", et al.) can be omitted
    from the scope.

    See CONTRIBUTING.md for details.

If you believe this warning is in error, please update this linter. If you wish
to suppress this linter, set %s=1 in your environment.`, err, noLintEnvVar)
		fmt.Println()
	}
}
