// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser_test

import (
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

func TestUnusedKeywords(t *testing.T) {
	grammarPath := "./sql.y"
	if bazel.BuiltWithBazel() {
		var err error
		grammarPath, err = bazel.Runfile("sql.y")
		if err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("./unused_keywords.sh", grammarPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 0 {
		t.Fatalf("found some unused keywords:\n%s", output)
	}
}
