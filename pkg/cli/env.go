// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
