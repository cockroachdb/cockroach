// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"flag"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
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
			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
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
