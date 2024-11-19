// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmdutil

import (
	"log"
	"os"
)

// RequireEnv returns the value of the environment variable s. If s is unset or
// blank, RequireEnv prints an error message and exits.
func RequireEnv(s string) string {
	v := os.Getenv(s)
	if v == "" {
		log.Fatalf("missing required environment variable %q", s)
	}
	return v
}
