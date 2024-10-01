// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import "os"

// getDefaultHost gets the default value for the host to connect to. pgx already
// has logic that would inspect PGHOST, but the problem is that if PGHOST not
// defined, pgx would default to using a unix socket. That is not desired, so
// here we make the CLI fallback to use "localhost" if PGHOST is not defined.
func getDefaultHost() string {
	if h := os.Getenv("PGHOST"); h == "" {
		return "localhost"
	}
	return ""
}

// getEnvOrDefault is a helper function that returns the value of an environment
// variable or a default value if the environment variable is not set.
func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
