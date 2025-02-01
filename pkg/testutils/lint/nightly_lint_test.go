// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build lint && nightly

package lint

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/urlcheck/lib/urlcheck"
	sqlparser "github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

func TestNightlyLint(t *testing.T) {
	_, pkgSpecified := os.LookupEnv("PKG")

	// TestHelpURLs checks that all help texts have a valid documentation URL.
	t.Run("TestHelpURLs", func(t *testing.T) {
		skip.UnderShort(t)
		if pkgSpecified {
			skip.IgnoreLint(t, "PKG specified")
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
