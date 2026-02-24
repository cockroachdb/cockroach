// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"fmt"
	"strings"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/errors"
)

// ValidateHookEnvironment checks that all environment variable references in
// the template's hook configuration can be resolved against the given
// environment. Returns a user-facing error listing all missing variables.
//
// Called at provisioning creation time to fail fast before infrastructure is
// created. Without this, a missing SSH key variable or hook env mapping would
// only surface after tofu apply has already created cloud resources.
func ValidateHookEnvironment(
	meta provmodels.TemplateMetadata, resolvedEnv envtypes.ResolvedEnvironment,
) error {
	if meta.SSH == nil && len(meta.Hooks) == 0 {
		return nil
	}

	available := make(map[string]bool, len(resolvedEnv.Variables))
	for _, v := range resolvedEnv.Variables {
		available[v.Key] = true
	}

	var missing []string

	// Check SSH private key variable.
	if meta.SSH != nil && meta.SSH.PrivateKeyVar != "" {
		if !available[meta.SSH.PrivateKeyVar] {
			missing = append(missing, fmt.Sprintf(
				"ssh.private_key_var %q", meta.SSH.PrivateKeyVar,
			))
		}
	}

	// Check hook env mappings.
	for _, hook := range meta.Hooks {
		for hookVar, envKey := range hook.Env {
			if !available[envKey] {
				missing = append(missing, fmt.Sprintf(
					"hook %q env %q references %q",
					hook.Name, hookVar, envKey,
				))
			}
		}
	}

	if len(missing) > 0 {
		return errors.Newf(
			"environment %q is missing variables required by template hooks: %s",
			resolvedEnv.Name, strings.Join(missing, "; "),
		)
	}
	return nil
}
