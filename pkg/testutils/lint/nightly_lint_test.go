// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build lint,nightly

package lint

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/urlcheck/lib/urlcheck"
	sqlparser "github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func TestNightlyLint(t *testing.T) {
	_, pkgSpecified := os.LookupEnv("PKG")

	// RoachLint is expensive memory-wise and thus should not run with t.Parallel().
	//
	// Note: It is too expensive RAM-wise to run roachlint on every CI
	// until issue https://github.com/cockroachdb/cockroach/issues/42594
	// has been addressed.
	t.Run("TestRoachLint", func(t *testing.T) {
		vetCmd(t, crdb.Dir, "roachlint", []string{pkgScope}, []stream.Filter{
			// Ignore generated files.
			stream.GrepNot(`pkg/.*\.pb\.go:`),
			stream.GrepNot(`pkg/col/coldata/.*\.eg\.go:`),
			stream.GrepNot(`pkg/col/colserde/arrowserde/.*_generated\.go:`),
			stream.GrepNot(`pkg/sql/colexec/.*\.eg\.go:`),
			stream.GrepNot(`pkg/sql/colexec/.*_generated\.go:`),
			stream.GrepNot(`pkg/sql/pgwire/hba/conf.go:`),

			// Ignore types that can change by system.
			stream.GrepNot(`pkg/util/sysutil/sysutil_unix.go:`),

			// Ignore tests.
			// TODO(mjibson): remove this ignore.
			stream.GrepNot(`pkg/.*_test\.go:`),
		})
	})

	// TestHelpURLs checks that all help texts have a valid documentation URL.
	t.Run("TestHelpURLs", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		if pkgSpecified {
			t.Skip("PKG specified")
		}

		t.Parallel()
		var buf bytes.Buffer
		for key, body := range sqlparser.HelpMessages {
			msg := sqlparser.HelpMessage{Command: key, HelpMessageBody: body}
			buf.WriteString(msg.String())
		}
		cmd := exec.Command("grep", "-nE", urlcheck.URLRE)
		cmd.Stdin = &buf
		if err := urlcheck.CheckURLsFromGrepOutput(cmd); err != nil {
			t.Fatal(err)
		}
	})
}
