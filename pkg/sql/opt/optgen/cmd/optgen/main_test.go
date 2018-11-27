// Copyright 2018 The Cockroach Authors.
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
	"errors"
	"flag"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

var (
	testDataGlob = flag.String("d", "testdata/[^.]*", "test data glob")
)

func TestOptgen(t *testing.T) {
	paths, err := filepath.Glob(*testDataGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", *testDataGlob)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				var buf bytes.Buffer

				gen := optgen{useGoFmt: true, maxErrors: 2, stdErr: &buf}

				gen.globResolver = func(pattern string) ([]string, error) {
					switch pattern {
					case "test.opt":
						return []string{"test.opt"}, nil
					case "all":
						return []string{"test.opt", "test2.opt"}, nil
					case "not-found.opt":
						return []string{"notfound.opt"}, nil
					default:
						return nil, errors.New("invalid source")
					}
				}

				// Resolve input file to the data-driven input text.
				gen.fileResolver = func(name string) (io.Reader, error) {
					switch name {
					case "test.opt":
						return strings.NewReader(d.Input), nil
					case "test2.opt":
						return strings.NewReader(""), nil
					default:
						return nil, errors.New("invalid filename")
					}
				}

				args := make([]string, len(d.CmdArgs))
				for i := range args {
					args[i] = d.CmdArgs[i].String()
				}
				gen.run(args...)

				// Suppress DO NOT EDIT so that reviewable will still show the
				// file by default.
				return strings.Replace(buf.String(), "DO NOT EDIT.", "[omitted]", -1)
			})
		})
	}
}
