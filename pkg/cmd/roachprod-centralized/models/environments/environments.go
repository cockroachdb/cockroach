// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package environments

import "time"

// EnvironmentVarType classifies a variable's handling during OpenTofu execution.
type EnvironmentVarType string

const (
	// VarTypePlaintext indicates a variable whose value is stored as-is.
	// Passed as -var flag, TF_VAR_* env var, and raw env var.
	VarTypePlaintext EnvironmentVarType = "plaintext"
	// VarTypeSecret indicates a variable used for provider credentials or
	// internal service secrets (e.g. AWS_ACCESS_KEY_ID). The value is a
	// secret manager reference resolved at runtime. Passed as a raw env var
	// only — no TF_VAR_* prefix, no -var flag.
	VarTypeSecret EnvironmentVarType = "secret"
	// VarTypeTemplateSecret indicates a variable whose value targets a
	// template variable declared with sensitive = true (e.g. db_password).
	// The value is a secret manager reference resolved at runtime. Passed
	// as TF_VAR_* env var and raw env var, but never as a -var flag.
	VarTypeTemplateSecret EnvironmentVarType = "template_secret"
)

// EnvironmentVariable is a single key/value pair belonging to an environment.
// Variables of type secret or template_secret hold a provider-prefixed
// reference rather than the actual secret; the reference is resolved at
// runtime by a secret resolver.
type EnvironmentVariable struct {
	EnvironmentName string             `json:"environment_name"`
	Key             string             `json:"key"`
	Value           string             `json:"value"`
	Type            EnvironmentVarType `json:"type"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
}

// Environment represents a provisioning environment that holds variables and
// credentials used during terraform execution. Each environment is identified
// by a unique, immutable name (PK).
//
// Owner is the identity (email or service account name) of the principal who
// created the environment. It is set once at creation time and never changed.
// Owner-based permissions (":own" variants) allow principals to manage their
// own environments without needing the broader ":all" permissions.
type Environment struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Owner       string    `json:"owner"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
