// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vars

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/errors"
)

// BuildVarMapsInput holds all inputs needed to assemble the variable maps for
// an OpenTofu execution. It is designed to be usable both by the local CLI
// command (PR4) and the future task handler (PR6).
type BuildVarMapsInput struct {
	// ResolvedEnv is the resolved environment with secrets fetched from
	// secret managers. Zero value means no environment is used (local
	// command with --var flags only).
	ResolvedEnv types.ResolvedEnvironment

	// UserVars are user-provided variables from --var flags or
	// Provisioning.Variables. Values may be string, float64, bool, or
	// complex types (maps/slices). Complex types are JSON-serialized.
	// Nil is treated as empty (no user-provided variables).
	UserVars map[string]interface{}

	// TemplateVars is the parsed template variable schema. Used to
	// determine which environment variables should be passed as -var
	// flags (only those matching a declared variable block). Nil is
	// treated as empty — no env vars will be passed as -var flags, and
	// conditional auto-injected variables (prov_name, environment,
	// owner) will not be injected.
	TemplateVars map[string]provisionings.TemplateOption

	// Identifier is the 8-char provisioning identifier. Always injected
	// into vars unconditionally.
	Identifier string

	// TemplateType is the template name, used to construct prov_name
	// ("{type}-{identifier}"). Only used if the template declares a
	// "prov_name" variable.
	TemplateType string

	// Environment is the environment name. Conditionally injected if the
	// template declares an "environment" variable.
	Environment string

	// Owner is the principal email. Conditionally injected if the
	// template declares an "owner" variable.
	Owner string

	// BackendEnvVars are additional environment variables to inject for
	// the backend (e.g., GOOGLE_BACKEND_CREDENTIALS for GCS). The
	// caller is responsible for constructing these based on the backend
	// type, keeping BuildVarMaps backend-agnostic.
	BackendEnvVars map[string]string
}

// BuildVarMaps assembles the two variable maps consumed by the OpenTofu
// executor:
//
//   - vars: key=value pairs passed as -var flags on the command line.
//     These are visible in process listings, so secrets must NOT be
//     included here.
//   - envVars: key=value pairs set as process environment variables.
//     Includes both KEY=VALUE and TF_VAR_KEY=VALUE for all resolved
//     environment variables.
//
// Precedence (highest to lowest):
//  1. Auto-injected variables (identifier, prov_name, environment, owner)
//  2. User-provided variables (--var flags / Provisioning.Variables)
//  3. Non-secret environment variables matching a declared template
//     variable (via -var flags)
//  4. All environment variables (via TF_VAR_* env vars)
//  5. Template defaults (handled by OpenTofu itself)
//
// See the reference doc §5 for the full algorithm design.
func BuildVarMaps(
	input BuildVarMapsInput,
) (vars map[string]string, envVars map[string]string, err error) {
	envVars = make(map[string]string)
	vars = make(map[string]string)

	// Step 1: All environment variables become process env vars AND
	// TF_VAR_ env vars. OpenTofu ignores TF_VAR_ vars that don't match
	// a declared variable, so passing everything is safe.
	for _, v := range input.ResolvedEnv.Variables {
		envVars[v.Key] = v.Value
		envVars["TF_VAR_"+v.Key] = v.Value
	}

	// Step 2: Non-secret environment variables that match a declared
	// template variable are ALSO passed as -var flags. We check against
	// the parsed template schema because tofu errors on unknown -var
	// flags.
	for _, v := range input.ResolvedEnv.Variables {
		if v.IsSecret {
			continue // secrets never go on the command line
		}
		if _, declared := input.TemplateVars[v.Key]; declared {
			vars[v.Key] = v.Value
		}
	}

	// Step 3: User-provided variables override environment vars if the
	// same key exists. Convert from map[string]interface{} to
	// map[string]string using JSON for complex types.
	for key, val := range input.UserVars {
		formatted, fmtErr := formatVarValue(val)
		if fmtErr != nil {
			return nil, nil, errors.Wrapf(fmtErr, "format variable %s", key)
		}
		vars[key] = formatted
	}

	// Step 4: Auto-injected variables always take precedence over user
	// vars.
	//
	// "identifier" is unconditionally injected — every template must
	// declare it.
	vars["identifier"] = input.Identifier

	// "prov_name", "environment", and "owner" are conditionally injected
	// — only if the template declares them as variable blocks.
	if _, declared := input.TemplateVars["prov_name"]; declared {
		vars["prov_name"] = input.TemplateType + "-" + input.Identifier
	}
	if _, declared := input.TemplateVars["environment"]; declared {
		vars["environment"] = input.Environment
	}
	if _, declared := input.TemplateVars["owner"]; declared {
		vars["owner"] = input.Owner
	}

	// Step 4.1: Validate that all user-provided variable names match a
	// declared template variable or an auto-injected variable. This
	// catches typos and undeclared variables before the slow tofu
	// init+plan cycle.
	if input.TemplateVars != nil {
		var unknown []string
		for key := range input.UserVars {
			if _, declared := input.TemplateVars[key]; !declared && !isAutoInjected(key) {
				unknown = append(unknown, key)
			}
		}
		if len(unknown) > 0 {
			sort.Strings(unknown)
			return nil, nil, errors.Newf(
				"unknown variable(s): %s (not declared in template)",
				strings.Join(unknown, ", "),
			)
		}
	}

	// Step 4.2: Validate that all required template variables have a
	// value from any source: -var flags, TF_VAR_* env vars, or
	// auto-injection. A required var is considered satisfied if it
	// appears in either the vars map or as a TF_VAR_<name> env var.
	{
		var missing []string
		for name, opt := range input.TemplateVars {
			if !opt.Required {
				continue
			}
			_, inVars := vars[name]
			_, inEnv := envVars["TF_VAR_"+name]
			if !inVars && !inEnv {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return nil, nil, errors.Newf(
				"missing required variable(s): %s",
				strings.Join(missing, ", "),
			)
		}
	}

	// Step 5: Inject backend-specific environment variables provided by
	// the caller (e.g., GOOGLE_BACKEND_CREDENTIALS for GCS).
	for k, v := range input.BackendEnvVars {
		envVars[k] = v
	}

	return vars, envVars, nil
}

// isAutoInjected reports whether a variable name is unconditionally
// auto-injected by BuildVarMaps regardless of template declaration.
// Only "identifier" qualifies — it is always injected into vars.
// Conditionally injected variables (prov_name, environment, owner)
// are only injected when the template declares them, so they must be
// validated against TemplateVars like any other user-provided variable.
func isAutoInjected(name string) bool {
	return name == "identifier"
}

// formatVarValue converts an interface{} value to a string suitable for a
// -var flag. Primitives are formatted directly; complex types are
// JSON-serialized (OpenTofu accepts JSON for complex variable values).
func formatVarValue(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case float64:
		return fmt.Sprintf("%g", v), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	default:
		data, err := json.Marshal(val)
		if err != nil {
			return "", errors.Wrapf(err, "marshal value of type %T", val)
		}
		return string(data), nil
	}
}
