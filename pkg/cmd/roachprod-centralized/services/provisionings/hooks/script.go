// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
)

// envVarNameRegex validates that environment variable names contain only
// alphanumeric characters and underscores, starting with a letter or
// underscore. This prevents shell injection via crafted variable names.
var envVarNameRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// buildScript constructs a bash script that:
//  1. Decodes and exports base64-encoded environment variables (safe for
//     multiline values and special characters).
//  2. Runs the provided command.
//
// The script is delivered via stdin to "bash -s" so no file is written to
// disk. Environment variable values are base64-encoded to prevent shell
// injection and safely handle multiline content. Variable names are
// validated against POSIX naming rules.
func buildScript(envVars map[string]string, command string) (string, error) {
	var sb strings.Builder
	sb.WriteString("#!/bin/bash\nset -euo pipefail\n")

	for name, value := range envVars {
		if !envVarNameRegex.MatchString(name) {
			return "", fmt.Errorf(
				"invalid env var name %q: must match [A-Za-z_][A-Za-z0-9_]*",
				name,
			)
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(value))
		sb.WriteString(fmt.Sprintf(
			"export %s=\"$(echo '%s' | base64 -d)\"\n", name, encoded,
		))
	}

	sb.WriteString(command)
	sb.WriteString("\n")
	return sb.String(), nil
}
