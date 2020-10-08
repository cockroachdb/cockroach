// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build nightly

package tree_test

import (
	"path/filepath"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestPrettyData reads in a single SQL statement from a file, formats it at
// all line lengths, and compares that output to a known-good output file. It
// is most useful when changing or implementing the doc interface for a node,
// and should be used to compare and verify the changed output.
func TestPrettyData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	matches, err := filepath.Glob(filepath.Join("testdata", "pretty", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := testPrettyCfg
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
