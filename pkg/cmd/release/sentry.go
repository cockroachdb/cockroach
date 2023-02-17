// Copyright 2019 The Cockroach Authors.
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
	"os/exec"
)

// generatePanic downloads the cockroachdb binary and forces a panic crash in order to trigger a Sentry event,
// which should file a GitHub issue.
func generatePanic(tag string) error {
	// the call usually exits non-zero on panic, but we don't need to fail in that case, thus "|| true"
	// TODO: do not hardcode the URL
	script := fmt.Sprintf(`
set -exuo pipefail
curl --fail --silent --show-error --output /dev/stdout --url https://storage.googleapis.com/cockroach-builds-artifacts-prod/cockroach-%s.linux-amd64.tgz | tar -xz
./cockroach-%s.linux-amd64/cockroach demo --insecure -e "select crdb_internal.force_panic('testing')" || true
`, tag, tag)
	cmd := exec.Command("bash", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to generate panic. output was: %s, err: %w", string(out), err)
	}
	fmt.Println(string(out))
	return nil
}
