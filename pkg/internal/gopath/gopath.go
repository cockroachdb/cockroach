// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package gopath contains utilities to get the current GOPATH.
package gopath

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// Get returns the guessed GOPATH.
func Get() string {
	p := os.Getenv("GOPATH")
	if p != "" {
		return p
	}
	home, err := envutil.HomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(home, "go")
}
