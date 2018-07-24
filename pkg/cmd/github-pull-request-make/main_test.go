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
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-github/github"
	"github.com/kr/pretty"
)

func TestPkgsFromDiff(t *testing.T) {
	for filename, expPkgs := range map[string]map[string]pkg{
		"testdata/10305.diff": {
			filepath.Join("pkg", "roachpb"): {tests: []string{"TestLeaseVerify"}},
		},
		"testdata/skip.diff": {
			filepath.Join("pkg", "ccl", "storageccl"): {tests: []string{"TestPutS3"}},
		},
		// This PR had some churn and renamed packages. This was formerly problematic
		// because nonexistent packages would be emitted.
		"testdata/27595.diff": {
			filepath.Join("pkg", "storage", "closedts", "transport"): {tests: []string{"TestTransportConnectOnRequest", "TestTransportClientReceivesEntries"}},
			filepath.Join("pkg", "storage", "closedts", "container"): {tests: []string{"TestContainer"}},
			filepath.Join("pkg", "storage", "closedts", "storage"):   {tests: []string{"TestConcurrent"}},
		},
	} {
		t.Run(filename, func(t *testing.T) {
			f, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			pkgs, err := pkgsFromDiff(f)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(pkgs, expPkgs) {
				t.Errorf("expected %s, got %s", expPkgs, pkgs)
			}
		})
	}
}

func TestPkgsFromDiffHelper(t *testing.T) {
	// This helper can easily generate new test cases.
	t.Skip("only for manual use")

	ctx := context.Background()
	client := ghClient(ctx)

	const prNum = 27595

	diff, _, err := client.PullRequests.GetRaw(
		ctx,
		"cockroachdb",
		"cockroach",
		prNum,
		github.RawOptions{Type: github.Diff},
	)
	if err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	name := filepath.Join(wd, "testdata", strconv.Itoa(prNum)+".diff")
	if err := ioutil.WriteFile(name, []byte(diff), 0644); err != nil {
		t.Fatal(err)
	}

	pkgs, err := pkgsFromDiff(strings.NewReader(diff))
	if err != nil {
		t.Fatal(err)
	}
	t.Errorf("read the following information:\n%v\n\ndiff at %s", pretty.Sprint(pkgs), name)
}
