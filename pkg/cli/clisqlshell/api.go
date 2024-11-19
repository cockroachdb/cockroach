// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
)

// Shell represents an interactive shell
type Shell interface {
	// RunInteractive runs the shell.
	RunInteractive(cmdIn, cmdOut, cmdErr *os.File) (exitErr error)
}

// URLParser represents a function able to convert user-supplied
// strings to a URL object.
type URLParser = func(url string) (*pgurl.URL, error)
