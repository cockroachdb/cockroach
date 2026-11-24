// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestGenMan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Generate man pages in a temp directory.
	manpath := t.TempDir()
	if err := Run([]string{"gen", "man", "--path=" + manpath}); err != nil {
		t.Fatal(err)
	}

	// Ensure we have a sane number of man pages.
	count := 0
	err := filepath.WalkDir(manpath, func(path string, d fs.DirEntry, err error) error {
		if strings.HasSuffix(path, ".1") && !d.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if min := 20; count < min {
		t.Errorf("number of man pages (%d) < minimum (%d)", count, min)
	}
}

func TestGenAutocomplete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Get a unique path to which we can write our autocomplete files.
	acdir := t.TempDir()

	for _, tc := range []struct {
		shell  string
		expErr string
	}{
		{shell: ""},
		{shell: "bash"},
		{shell: "fish"},
		{shell: "zsh"},
		{shell: "bad", expErr: `invalid argument "bad" for "cockroach gen autocomplete"`},
	} {
		t.Run("shell="+tc.shell, func(t *testing.T) {
			const minsize = 1000
			acpath := filepath.Join(acdir, "output-"+tc.shell)

			args := []string{"gen", "autocomplete", "--out=" + acpath}
			if len(tc.shell) > 0 {
				args = append(args, tc.shell)
			}
			err := Run(args)
			if tc.expErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, tc.expErr) {
					t.Fatalf("expected error %s, found %v", tc.expErr, err)
				}
				return
			}

			info, err := os.Stat(acpath)
			if err != nil {
				t.Fatal(err)
			}
			if size := info.Size(); size < minsize {
				t.Fatalf("autocomplete file size (%d) < minimum (%d)", size, minsize)
			}
		})
	}
}
