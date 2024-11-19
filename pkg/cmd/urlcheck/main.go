// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/cockroachdb/cockroach/pkg/cmd/urlcheck/lib/urlcheck"
)

func main() {
	cmd := exec.Command("git", "grep", "-nE", urlcheck.URLRE)
	if err := urlcheck.CheckURLsFromGrepOutput(cmd); err != nil {
		log.Fatalf("%+v\nFAIL", err)
	}
	fmt.Println("PASS")
}
