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
	"bytes"
	"context"
	"io"
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/pkg/errors"
	"github.com/tebeka/go2xunit/lib"
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

func postIssue(ctx context.Context, packageName, testName, testMessage string) error {
	const detail = " under stress"
	return issues.Post(ctx, detail, packageName, testName, testMessage)
}

func listFailures(
	ctx context.Context,
	input io.Reader,
	f func(ctx context.Context, packageName, testName, testMessage string) error,
) error {
	var inputBuf bytes.Buffer
	input = io.TeeReader(input, &inputBuf)

	suites, err := lib.ParseGotest(input, "")
	if err != nil {
		return errors.Wrap(err, "failed to parse `go test` output")
	}

	posted := false
	for _, suite := range suites {
		packageName := suite.Name
		if packageName == "" {
			var ok bool
			packageName, ok = os.LookupEnv(pkgEnv)
			if !ok {
				log.Fatalf("package name environment variable %s is not set", pkgEnv)
			}
		}
		for _, test := range suite.Tests {
			switch test.Status {
			case lib.Failed:
				if err := f(ctx, packageName, test.Name, test.Message); err != nil {
					return errors.Wrap(err, "failed to post issue")
				}
				posted = true
			}
		}
	}

	if !posted {
		// We're only invoked upon failure. If we couldn't find a failing Go test,
		// assume that a failure occurred before running Go and post an issue about
		// that.
		const unknown = "(unknown)"
		packageName, ok := os.LookupEnv(pkgEnv)
		if !ok {
			packageName = unknown
		}
		if err := f(ctx, packageName, unknown, inputBuf.String()); err != nil {
			return errors.Wrap(err, "failed to post issue")
		}
	}
	return nil
}
