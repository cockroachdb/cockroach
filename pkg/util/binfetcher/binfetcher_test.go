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

package binfetcher

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestDownload(t *testing.T) {
	if testutils.NightlyStress() {
		t.Skip()
	}
	t.Skip("disabled by default because downloading files in CI is a silly idea")

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tests := []Options{
		// Cockroach.
		{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "v1.0.4",
			GOOS:    "linux",
		},
		{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "v1.0.6",
			GOOS:    "darwin",
		},
		{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "darwin",
		},
		{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "v1.1.3",
			GOOS:    "windows",
		},
		{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "windows",
		},
		// TODO(tschottdorf): This bucket has a one week GC policy, so
		// hard-coding particular SHAs isn't a good idea.
		//
		// {
		// 	Binary:  "cockroach",
		// 	Dir:     dir,
		// 	Version: "bd828feaa309578142fe7ad2d89ee1b70adbd52d",
		// 	GOOS:    "linux",
		// },
		{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "linux",
		},

		// Load generators.
		{
			Binary:  "loadgen/kv",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "linux",
		},
		{
			Binary:  "loadgen/tpcc",
			Dir:     dir,
			Version: "619a768955d5e2cb0b3ae77a8950eec5cd06c041",
			GOOS:    "linux",
		},
		{
			Component: "loadgen",
			Binary:    "ycsb",
			Dir:       dir,
			Version:   "LATEST",
			GOOS:      "linux",
		},
	}

	checkScriptEquivalent := func(t *testing.T, opts Options) {
		if err := opts.init(); err != nil {
			t.Fatal(err)
		}

		script, cleanup := generateForTesting(t, opts)
		defer cleanup()

		exp := opts.URL.String()

		out, err := exec.Command(script,
			"-o", opts.GOOS, "-a", opts.GOARCH, "-d", opts.Dir, "-c", opts.Component, "-s", opts.Suffix,
			opts.Binary, opts.Version,
		).CombinedOutput()

		outS := strings.TrimSpace(string(out))

		if err != nil {
			t.Fatalf("error: %s\noutput:\n%s", err, out)
		} else if exp != outS {
			t.Fatalf("wanted %q, got %q", exp, outS)
		}
	}

	// Run twice to check that that doesn't cause errors.
	for i := 0; i < 2; i++ {
		for j, opts := range tests {
			t.Run(fmt.Sprintf("num=%d,case=%d", i, j), func(t *testing.T) {
				ctx := context.Background()

				s, err := Download(ctx, opts)
				if err != nil {
					t.Fatal(err)
				}
				if stat, err := os.Stat(s); err != nil {
					t.Fatal(err)
				} else if stat.Size() == 0 {
					t.Fatal("empty file")
				}

				checkScriptEquivalent(t, opts)
			})
		}
	}
}

func generateForTesting(t *testing.T, opts Options) (filename string, cleanup func()) {
	dir, err := ioutil.TempDir("", "generate")
	if err != nil {
		t.Fatal(err)
	}

	newFile := filepath.Join(dir, "url.sh")

	if err := ioutil.WriteFile(newFile, []byte(opts.Generated()), 0755); err != nil {
		t.Fatal(err)
	}

	return newFile, func() {
		_ = os.RemoveAll(dir)
	}
}

func TestGenerateNoChange(t *testing.T) {
	newFileGeneric, cleanupGeneric := generateForTesting(t, Options{})
	defer cleanupGeneric()

	newFileCockroach, cleanupCockroach := generateForTesting(t, Options{Binary: "cockroach"})
	defer cleanupCockroach()

	{
		orig, err := ioutil.ReadFile(OutputFileGeneric)
		if err != nil {
			t.Fatal(err)
		}

		gen, err := ioutil.ReadFile(newFileGeneric)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(orig, gen) {
			t.Errorf("need to run `go generate` for this package to update %s", OutputFileGeneric)
		}
	}

	{
		orig, err := ioutil.ReadFile(OutputFileCockroach)
		if err != nil {
			t.Fatal(err)
		}

		gen, err := ioutil.ReadFile(newFileCockroach)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(orig, gen) {
			t.Errorf("need to run `go generate` for this package to update %s", OutputFileCockroach)
		}
	}
}
