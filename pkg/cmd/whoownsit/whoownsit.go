// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// whoownsit looks for OWNERS in the directory and parenting directories
// until it finds an owner for a given file.
//
// Usage: ./whoownsit [<file_or_dir> ...]
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/internal/reporoot"
)

var walk = flag.Bool("walk", false, "recursively print ownership")
var dirsOnly = flag.Bool("dirs-only", false, "print ownership only for directories")

func main() {
	flag.Parse()

	codeOwners, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		log.Fatalf("failing to load code owners: %v", err)
	}

	for _, path := range flag.Args() {
		if filepath.IsAbs(path) {
			var err error
			path, err = filepath.Rel(reporoot.Get(), path)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		if err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !*dirsOnly || info.IsDir() {
				matches := codeOwners.Match(path)
				var aliases []string
				for _, match := range matches {
					aliases = append(aliases, string(match.Name()))
				}
				if len(aliases) == 0 {
					aliases = append(aliases, "-")
				}
				fmt.Println(strings.Join(aliases, ","), " ", path)
			}
			if !*walk {
				return filepath.SkipDir
			}
			return nil
		}); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}
