// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vars

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
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
	// conditionally inject auto-injected variables (prov_name,
	// environment, owner) as -var flags, and to validate user-provided
	// variable names and required variables. Nil is treated as empty —
	// conditional auto-injected variables will not be injected.
	TemplateVars map[string]provisionings.TemplateOption

	// Identifier is the 8-char provisioning identifier. Always injected
	// into vars unconditionally.
	Identifier string

	// TemplateType is the template name, used to construct prov_name
	// ("{owner}-{type}-{identifier}"). Only used if the template
	// declares a "prov_name" variable.
	TemplateType string

	// Environment is the environment name. Conditionally injected if the
	// template declares an "environment" variable.
	Environment string

	// Owner is the principal email. Used to construct the owner prefix
	// in prov_name, and conditionally injected as the "owner" variable
	// if declared by the template.
	Owner string
}

// BuildVarMaps assembles the two variable maps consumed by the OpenTofu
// executor:
//
//   - vars: key=value pairs passed as -var flags on the command line.
//     Contains only auto-injected variables (identifier, prov_name,
//     environment, owner). Passing these as -var flags enforces that
//     the template declares the required variable blocks — tofu errors
//     on unknown -var flags.
//   - envVars: key=value pairs set as process environment variables.
//     Includes KEY=VALUE for all types, plus TF_VAR_KEY=VALUE for
//     plaintext, template_secret, and user-provided variables.
//
// Variable type behavior:
//   - plaintext:        raw env + TF_VAR_* env
//   - template_secret:  raw env + TF_VAR_* env
//   - secret:           raw env only (no TF_VAR_*)
//   - secret_file:      raw env only, value written to temp file (env
//     var set to the file path by PrepareCredentialFiles)
//
// Precedence (highest to lowest):
//  1. Auto-injected variables (identifier, prov_name, environment,
//     owner) — passed as -var flags, highest priority in OpenTofu
//  2. User-provided variables (--var flags / Provisioning.Variables)
//     — passed as TF_VAR_* env vars
//  3. Environment variables (via TF_VAR_* env vars, for plaintext and
//     template_secret only)
//  4. Template defaults (handled by OpenTofu itself)
//
// See the reference doc §5 for the full algorithm design.
func BuildVarMaps(
	input BuildVarMapsInput,
) (vars map[string]string, envVars map[string]string, err error) {
	envVars = make(map[string]string)
	vars = make(map[string]string)

	// Step 1: Inject environment variables into envVars based on type.
	//   - All types get raw KEY=VALUE.
	//   - plaintext and template_secret also get TF_VAR_KEY=VALUE
	//     (OpenTofu ignores unknown TF_VAR_ vars).
	//   - secret and secret_file variables are raw env only (provider
	//     credentials).
	for _, v := range input.ResolvedEnv.Variables {
		envVars[v.Key] = v.Value
		if v.Type != envmodels.VarTypeSecret && v.Type != envmodels.VarTypeSecretFile {
			envVars["TF_VAR_"+v.Key] = v.Value
		}
	}

	// Step 2: User-provided variables are passed as TF_VAR_* env vars,
	// overriding environment values if the same key exists. Complex
	// types (maps, slices) are JSON-serialized — OpenTofu accepts JSON
	// for complex variable values via TF_VAR_*.
	for key, val := range input.UserVars {
		formatted, fmtErr := formatVarValue(val)
		if fmtErr != nil {
			return nil, nil, errors.Wrapf(fmtErr, "format variable %s", key)
		}
		envVars["TF_VAR_"+key] = formatted
	}

	// Step 3: Auto-injected variables are passed as -var flags so that
	// tofu validates their presence in the template (tofu errors on
	// unknown -var flags). They always take precedence over user vars
	// and TF_VAR_* env vars because -var has the highest priority in
	// OpenTofu.
	//
	// "identifier" is unconditionally injected — every template must
	// declare it.
	vars["identifier"] = input.Identifier

	// "prov_name", "environment", and "owner" are conditionally injected
	// — only if the template declares them as variable blocks.
	if _, declared := input.TemplateVars["prov_name"]; declared {
		vars["prov_name"] = ProvName(input.Owner, input.TemplateType, input.Identifier)
	}
	if _, declared := input.TemplateVars["environment"]; declared {
		vars["environment"] = input.Environment
	}
	if _, declared := input.TemplateVars["owner"]; declared {
		vars["owner"] = input.Owner
	}

	// Step 3.1: Validate that all user-provided variable names match a
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

	// Step 3.2: Validate that all required template variables have a
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

// ProvName constructs the provisioning name from the owner email, template
// type, and identifier. The owner's email local part is DNS-sanitized and
// prepended as a prefix, matching roachprod's convention that VM names
// start with the owner (e.g., "ludoleroux-gcp-workload-abc12def").
// This ensures the cluster tag, VM names, and cluster name all align,
// which is required for clusters_sync owner extraction.
func ProvName(owner, templateType, identifier string) string {
	localPart := owner
	if i := strings.Index(owner, "@"); i >= 0 {
		localPart = owner[:i]
	}
	prefix := vm.DNSSafeName(localPart)
	if prefix == "" {
		return templateType + "-" + identifier
	}
	return prefix + "-" + templateType + "-" + identifier
}

// formatVarValue converts an interface{} value to a string suitable for a
// TF_VAR_* environment variable. Primitives are formatted directly;
// complex types are JSON-serialized (OpenTofu accepts JSON for complex
// variable values).
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
