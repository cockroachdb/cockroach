// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import "github.com/spf13/cobra"

// connectCmd triggers a TLS initialization handshake and writes
// certificates in the specified certs-dir for use with start.
var connectCmd = &cobra.Command{
	Use:   "connect --certs-dir=<path to cockroach certs dir> --init-token=<shared secret> --join=<host 1>,<host 2>,...,<host N>",
	Short: "build TLS certificates for use with the start command",
	Long: `
Connects to other nodes and negotiates an initialization bundle for use with
secure inter-node connections.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runConnect),
}

// runConnect connects to other nodes and negotiates an initialization bundle
// for use with secure inter-node connections.
func runConnect(cmd *cobra.Command, args []string) error {
	// TODO(bilal): Implement TLS init handshake.
	// https://github.com/cockroachdb/cockroach/issues/60632
	return nil
}
