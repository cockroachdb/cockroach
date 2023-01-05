// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"fmt"
	"os"
)

// readManifest reads the manifest file from the benchDir directory and returns
// a list of packages for which test binaries have been built.
func readManifest(benchDir string) ([]string, error) {
	file, err := os.Open(fmt.Sprintf("%s/roachbench.manifest", benchDir))
	if err != nil {
		return nil, err
	}

	defer func(file *os.File) {
		if defErr := file.Close(); defErr != nil {
			l.Errorf("Failed to close manifest file - %v", err)
		}
	}(file)

	entries := make([]string, 0)
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		entries = append(entries, sc.Text())
	}

	if err = sc.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}
