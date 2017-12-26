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
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"

	"golang.org/x/net/context"
)

func TestDownload(t *testing.T) {
	if testutils.NightlyStress() {
		t.Skip()
	}

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tests := []Options{
		// Cockroach.
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "v1.0.4",
			GOOS:    "linux",
		},
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "v1.0.6",
			GOOS:    "darwin",
		},
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "darwin",
		},
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "v1.1.3",
			GOOS:    "windows",
		},
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "windows",
		},
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "bd828feaa309578142fe7ad2d89ee1b70adbd52d",
			GOOS:    "linux",
		},
		Options{
			Binary:  "cockroach",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "linux",
		},

		// Load generators.
		Options{
			Binary:  "loadgen/kv",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "linux",
		},
		Options{
			Binary:  "loadgen/tpcc",
			Dir:     dir,
			Version: "619a768955d5e2cb0b3ae77a8950eec5cd06c041",
			GOOS:    "linux",
		},
		Options{
			Component: "loadgen",
			Binary:    "ycsb",
			Dir:       dir,
			Version:   "LATEST",
			GOOS:      "linux",
		},
	}
	// Run twice to check that that doesn't cause errors.
	for i := 0; i < 1; i++ {
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
			})
		}
	}
}
