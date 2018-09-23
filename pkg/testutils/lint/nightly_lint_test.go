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
