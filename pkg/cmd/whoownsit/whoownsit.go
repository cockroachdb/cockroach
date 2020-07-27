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

	"github.com/cockroachdb/cockroach/pkg/internal/owner"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
)

func main() {
	flag.Parse()

	teamsFile, err := os.Open("TEAMS")
	if err != nil {
		log.Fatalf("unable to load TEAMS: %v", err)
	}
	defer func() { _ = teamsFile.Close() }()
	teams, err := team.LoadTeams(teamsFile)
	if err != nil {
		log.Fatalf("failed to parse TEAMS: %v", err)
	}

	for _, fileOrDir := range flag.Args() {
		owners, err := owner.FindOwners(fileOrDir, teams)
		if err != nil {
			log.Fatalf("failed finding owner for %q: %v", fileOrDir, err)
		}
		if len(flag.Args()) > 1 {
			fmt.Printf("%s: ", fileOrDir)
		}
		if len(owners) == 0 {
			fmt.Printf("no owners")
		} else {
			for i, owner := range owners {
				if i > 0 {
					fmt.Printf(", ")
				}
				fmt.Print(owner.Name)
			}
		}
		fmt.Printf("\n")
	}
}
