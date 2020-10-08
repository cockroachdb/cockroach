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

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
)

func main() {
	flag.Parse()

	codeOwners, err := codeowners.DefaultLoadCodeOwners()
	if err != nil {
		log.Fatalf("failing to load code owners: %v", err)
	}

	for _, path := range flag.Args() {
		owners, err := codeOwners.Match(path)
		if err != nil {
			log.Fatalf("failed finding owner for %q: %v", path, err)
		}
		if len(flag.Args()) > 1 {
			fmt.Printf("%s: ", path)
		}
		if len(owners) == 0 {
			fmt.Printf("no owners")
		} else {
			for i, owner := range owners {
				if i > 0 {
					fmt.Printf(", ")
				}
				fmt.Print(owner.Alias)
			}
		}
		fmt.Printf("\n")
	}
}
