// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package environments

import "time"

// EnvironmentVarType classifies a variable as plaintext or a secret reference.
type EnvironmentVarType string

const (
	// VarTypePlaintext indicates a variable whose value is stored as-is.
	VarTypePlaintext EnvironmentVarType = "plaintext"
	// VarTypeSecret indicates a variable whose value is a secret manager
	// reference (e.g. "gcp:projects/cockroach/secrets/key/versions/latest").
	VarTypeSecret EnvironmentVarType = "secret"
)

// EnvironmentVariable is a single key/value pair belonging to an environment.
// Secret-type variables hold a provider-prefixed reference rather than the
// actual secret; the reference is resolved at runtime by a secret resolver.
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
