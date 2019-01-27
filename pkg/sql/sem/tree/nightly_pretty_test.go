// Copyright 2019 The Cockroach Authors.
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

// +build nightly

package tree_test

import (
	"path/filepath"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TestPrettyData reads in a single SQL statement from a file, formats it at
// all line lengths, and compares that output to a known-good output file. It
// is most useful when changing or implementing the doc interface for a node,
// and should be used to compare and verify the changed output.
func TestPrettyData(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join("testdata", "pretty", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := tree.DefaultPrettyCfg()
	cfg.Align = tree.PrettyNoAlign
	t.Run("ref", func(t *testing.T) {
		runTestPrettyData(t, "ref", cfg, matches, false /*short*/)
	})
	cfg.Align = tree.PrettyAlignAndDeindent
	t.Run("align-deindent", func(t *testing.T) {
		runTestPrettyData(t, "align-deindent", cfg, matches, false /*short*/)
	})
	cfg.Align = tree.PrettyAlignOnly
	t.Run("align-only", func(t *testing.T) {
		runTestPrettyData(t, "align-only", cfg, matches, false /*short*/)
	})
}
