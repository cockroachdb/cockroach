// Copyright 2016 The Cockroach Authors.
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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
)

const (
	pkgEnv = "PKG"
)

func main() {
	ctx := context.Background()
	if err := listFailures(ctx, os.Stdin, postIssue); err != nil {
		log.Fatal(err)
	}
}

func postIssue(ctx context.Context, packageName, testName, testMessage, authorEmail string) error {
	const detail = " under stress"
	return issues.Post(ctx, detail, packageName, testName, testMessage, authorEmail)
}

func listFailures(
	ctx context.Context,
	input io.Reader,
	f func(ctx context.Context, packageName, testName, testMessage, authorEmail string) error,
) error {
	var inputBuf bytes.Buffer
	input = io.TeeReader(input, &inputBuf)

	// The `go test -json` output stream is a newline-separated sequence of
	// structs. Seek to the first such struct to ignore non-JSON preamble, such
	// as what is produced by our Makefile machinery.
	buf := bufio.NewReader(input)
	for {
		b, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if bytes.HasPrefix(b, []byte{'{'}) {
			input = io.MultiReader(bytes.NewReader(b), buf)
			break
		}
	}

	dec := json.NewDecoder(input)
	dec.DisallowUnknownFields()

	type TestEvent struct {
		//lint:ignore U1000 we disallow unknown fields.
		Time    time.Time // encodes as an RFC3339-format string
		Action  string
		Package string
		Test    string
		//lint:ignore U1000 we disallow unknown fields.
		Elapsed float64 // seconds
		Output  string
	}

	type ID struct {
		Package string
		Test    string
	}

	failures := make(map[ID][]TestEvent)

	for {
		var te TestEvent
		if err := dec.Decode(&te); err != nil {
			if err == io.EOF {
				break
			}
			if err, ok := err.(*json.SyntaxError); ok {
				// make: *** [Makefile:892: stress] Error 1
				if strings.Contains(err.Error(), "invalid character 'm' looking for beginning of value") {
					break
				}
			}
			return err
		}

		packageName := te.Package
		if len(packageName) == 0 {
			var ok bool
			packageName, ok = os.LookupEnv(pkgEnv)
			if !ok {
				return errors.Errorf("package name environment variable %s is not set", pkgEnv)
			}
		}
		// Events for the overall package test do not set Test.
		if len(te.Test) > 0 {
			id := ID{
				Package: packageName,
				Test:    te.Test,
			}
			switch te.Action {
			case "output":
				failures[id] = append(failures[id], te)
			case "pass", "skip":
				delete(failures, id)
			}
		}
	}

	if len(failures) == 0 {
		// We're only invoked upon failure. If we couldn't find a failing Go test,
		// assume that a failure occurred before running Go and post an issue about
		// that.
		const unknown = "(unknown)"
		packageName, ok := os.LookupEnv(pkgEnv)
		if !ok {
			packageName = unknown
		}
		if _, err := inputBuf.ReadFrom(input); err != nil {
			log.Printf("failed to read remaining test output: %s\n", err)
		}
		if err := f(ctx, packageName, unknown, inputBuf.String(), ""); err != nil {
			return errors.Wrap(err, "failed to post issue")
		}
	} else {
		for id, tes := range failures {
			authorEmail, err := getAuthorEmail(ctx, id.Package, id.Test)
			if err != nil {
				log.Printf("unable to determine test author email: %s\n", err)
			}
			var outputs []string
			for _, te := range tes {
				outputs = append(outputs, te.Output)
			}
			message := strings.Join(outputs, "")
			if err := f(ctx, id.Package, id.Test, message, authorEmail); err != nil {
				return errors.Wrap(err, "failed to post issue")
			}
		}
	}
	return nil
}

func getAuthorEmail(ctx context.Context, packageName, testName string) (string, error) {
	// Search the source code for the email address of the last committer to touch
	// the first line of the source code that contains testName. Then, ask GitHub
	// for the GitHub username of the user with that email address by searching
	// commits in cockroachdb/cockroach for commits authored by the address.
	subtests := strings.Split(testName, "/")
	testName = subtests[0]
	packageName = strings.TrimPrefix(packageName, "github.com/cockroachdb/cockroach/")
	cmd := exec.Command(`/bin/bash`, `-c`,
		fmt.Sprintf(`git grep -n "func %s" $(git rev-parse --show-toplevel)/%s/*_test.go`,
			testName, packageName))
	// This command returns output such as:
	// ../ccl/storageccl/export_test.go:31:func TestExportCmd(t *testing.T) {
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", errors.Errorf("couldn't find test %s in %s: %s %s",
			testName, packageName, err, string(out))
	}
	re := regexp.MustCompile(`(.*):(.*):`)
	// The first 2 :-delimited fields are the filename and line number.
	matches := re.FindSubmatch(out)
	if matches == nil {
		return "", errors.Errorf("couldn't find filename/line number for test %s in %s: %s",
			testName, packageName, string(out))
	}
	filename := matches[1]
	linenum := matches[2]

	// Now run git blame.
	cmd = exec.Command(`/bin/bash`, `-c`,
		fmt.Sprintf(`git blame --porcelain -L%s,+1 %s | grep author-mail`,
			linenum, filename))
	// This command returns output such as:
	// author-mail <jordan@cockroachlabs.com>
	out, err = cmd.CombinedOutput()
	if err != nil {
		return "", errors.Errorf("couldn't find author of test %s in %s: %s %s",
			testName, packageName, err, string(out))
	}
	re = regexp.MustCompile("author-mail <(.*)>")
	matches = re.FindSubmatch(out)
	if matches == nil {
		return "", errors.Errorf("couldn't find author email of test %s in %s: %s",
			testName, packageName, string(out))
	}
	return string(matches[1]), nil
}
