// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"log"
	"os"
	"path"
)

func updateVersionTxt(workDir string, version string, f string) error {
	dest := path.Join(workDir, f)
	if _, err := os.Stat(dest); err != nil {
		log.Printf("version file %s does not exists, skipping", dest)
		return nil
	}
	contents := []byte(version + "\n")
	if err := os.WriteFile(dest, contents, 0644); err != nil {
		return fmt.Errorf("cannot write version.txt: %w", err)
	}
	return nil
}
