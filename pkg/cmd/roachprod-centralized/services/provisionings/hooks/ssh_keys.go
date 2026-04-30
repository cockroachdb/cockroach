// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/ssh"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// SSHKeysSetupExecutor implements the ssh-keys-setup hook type. It fetches
// engineer SSH public keys from an AuthorizedKeysProvider and writes them to
// ~/.ssh/authorized_keys on all target machines. The file is written in full
// (replace, not append) for idempotency.
type SSHKeysSetupExecutor struct {
	sshClient    ssh.ISSHClient
	keysProvider AuthorizedKeysProvider
}

// NewSSHKeysSetupExecutor creates a new ssh-keys-setup executor.
func NewSSHKeysSetupExecutor(
	sshClient ssh.ISSHClient, keysProvider AuthorizedKeysProvider,
) *SSHKeysSetupExecutor {
	return &SSHKeysSetupExecutor{
		sshClient:    sshClient,
		keysProvider: keysProvider,
	}
}

// Execute fetches authorized keys and writes them to all target machines.
func (e *SSHKeysSetupExecutor) Execute(
	ctx context.Context, l *logger.Logger, hctx HookContext,
) error {
	l.Info("fetching authorized keys",
		slog.String("hook", hctx.Declaration.Name),
	)

	authorizedKeys, err := e.keysProvider.GetAuthorizedKeys(ctx)
	if err != nil {
		return errors.Wrap(err, "fetch authorized keys")
	}

	l.Info("authorized keys fetched",
		slog.String("hook", hctx.Declaration.Name),
		slog.Int("bytes", len(authorizedKeys)),
	)

	// Build a script that writes the full authorized_keys file. The content
	// is base64-encoded to safely transmit via stdin without shell
	// interpretation issues.
	encoded := base64.StdEncoding.EncodeToString(authorizedKeys)
	script := fmt.Sprintf(
		"#!/bin/bash\nset -euo pipefail\n"+
			"mkdir -p ~/.ssh && chmod 700 ~/.ssh\n"+
			"echo '%s' | base64 -d > ~/.ssh/authorized_keys\n"+
			"chmod 600 ~/.ssh/authorized_keys\n",
		encoded,
	)

	return runOnMachines(
		ctx, l, e.sshClient,
		hctx.MachineIPs, hctx.SSHUser, hctx.SSHPrivateKey,
		script, defaultMaxConcurrency, hctx.Declaration.Name,
	)
}
