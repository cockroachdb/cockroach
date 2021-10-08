// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
