// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

// HookExecutor is the interface that hook type implementations must satisfy.
// Each hook type (run-command, ssh-keys-setup) has its own executor.
type HookExecutor interface {
	Execute(ctx context.Context, l *logger.Logger, hctx HookContext) error
}

// HookContext carries all resolved values needed to execute a single hook
// invocation.
type HookContext struct {
	Provisioning  provmodels.Provisioning
	Declaration   provmodels.HookDeclaration
	HookEnv       map[string]string // resolved env vars for this hook
	SSHPrivateKey []byte            // PEM-encoded private key
	MachineIPs    []string          // pre-evaluated from jq expression
	SSHUser       string            // from declaration or ssh config
}
