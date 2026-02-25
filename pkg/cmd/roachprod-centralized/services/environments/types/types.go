// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

const TaskServiceName = "environments"

// MaxEnvironmentNameLen is the maximum length of an environment name. The
// name is embedded in auto-generated GCP Secret Manager secret IDs
// (envName + "--" + key), so it must stay short and use only characters
// valid for GCP secret IDs.
const MaxEnvironmentNameLen = 63

// MaxVariableKeyLen is the maximum length of a variable key. This is derived
// from the GCP Secret Manager secret ID limit (255 chars) minus the maximum
// environment name length (63 chars) minus the "--" separator (2 chars) used
// when constructing secret IDs as "{envName}--{key}".
const MaxVariableKeyLen = 190

// ValidNameRe restricts environment names to 1–MaxEnvironmentNameLen
// characters from the set [a-zA-Z0-9_-].
var ValidNameRe = regexp.MustCompile(
	fmt.Sprintf(`^[a-zA-Z0-9_-]{1,%d}$`, MaxEnvironmentNameLen),
)

// ValidVariableKeyRe restricts variable keys to 1–MaxVariableKeyLen
// characters from the set [a-zA-Z0-9_-]. The key is used as part of
// GCP Secret Manager secret IDs ("{envName}--{key}"), so it must use
// only characters valid for GCP secret IDs.
var ValidVariableKeyRe = regexp.MustCompile(
	fmt.Sprintf(`^[a-zA-Z0-9_-]{1,%d}$`, MaxVariableKeyLen),
)

var (
	// ErrEnvironmentNotFound is the service-level sentinel error.
	ErrEnvironmentNotFound = utils.NewPublicError(fmt.Errorf("environment not found"))
	// ErrEnvironmentAlreadyExists is the service-level sentinel error.
	ErrEnvironmentAlreadyExists = utils.NewPublicError(fmt.Errorf("environment already exists"))
	// ErrEnvironmentHasProvisionings is the service-level sentinel error.
	ErrEnvironmentHasProvisionings = utils.NewPublicError(fmt.Errorf("environment has active provisionings"))
	// ErrInvalidEnvironmentName is returned for invalid environment names.
	ErrInvalidEnvironmentName = utils.NewPublicError(fmt.Errorf("invalid environment name: must match [a-zA-Z0-9_-]"))
	// ErrInvalidVariableKey is returned when a variable key is invalid.
	ErrInvalidVariableKey = utils.NewPublicError(fmt.Errorf("invalid variable key: must match [a-zA-Z0-9_-]"))
	// ErrInvalidVariableType is returned for unsupported variable types.
	ErrInvalidVariableType = utils.NewPublicError(fmt.Errorf("variable type must be 'plaintext', 'secret', or 'template_secret'"))
	// ErrVariableNotFound is the service-level sentinel error.
	ErrVariableNotFound = utils.NewPublicError(fmt.Errorf("environment variable not found"))
	// ErrVariableAlreadyExists is the service-level sentinel error.
	ErrVariableAlreadyExists = utils.NewPublicError(fmt.Errorf("environment variable already exists"))
	// ErrSecretWriteFailed is returned when writing a secret to the secret
	// manager fails (e.g. no GCP project configured).
	ErrSecretWriteFailed = utils.NewPublicError(fmt.Errorf("failed to write secret to secret manager"))
	// ErrSecretVerifyFailed is returned when verifying a secret reference
	// against the secret manager fails.
	ErrSecretVerifyFailed = utils.NewPublicError(fmt.Errorf("failed to verify secret reference"))
)

const (
	// Base permission building blocks (not granted directly).
	PermissionView   = TaskServiceName + ":view"
	PermissionCreate = TaskServiceName + ":create"
	PermissionUpdate = TaskServiceName + ":update"
	PermissionDelete = TaskServiceName + ":delete"

	// Permissions granted to users.
	PermissionViewAll   = PermissionView + ":all"
	PermissionViewOwn   = PermissionView + ":own"
	PermissionUpdateAll = PermissionUpdate + ":all"
	PermissionUpdateOwn = PermissionUpdate + ":own"
	PermissionDeleteAll = PermissionDelete + ":all"
	PermissionDeleteOwn = PermissionDelete + ":own"
)

// IService is the interface for the environments service.
type IService interface {
	// GetEnvironment returns the environment by name. Principals with :all
	// can view any environment; principals with :own can only view
	// environments they own. Returns ErrEnvironmentNotFound if the
	// principal lacks access (to avoid leaking existence).
	GetEnvironment(ctx context.Context, l *logger.Logger, principal *auth.Principal, name string) (envmodels.Environment, error)
	// GetEnvironments returns all environments visible to the principal.
	// Principals with :all see all environments; principals with :own see
	// only their own.
	GetEnvironments(ctx context.Context, l *logger.Logger, principal *auth.Principal) ([]envmodels.Environment, error)
	// CreateEnvironment persists a new environment. The principal's identity
	// is recorded as the environment owner.
	CreateEnvironment(ctx context.Context, l *logger.Logger, principal *auth.Principal, input InputCreateDTO) (envmodels.Environment, error)
	// UpdateEnvironment updates an existing environment. Requires :all or
	// :own (if the principal is the owner).
	UpdateEnvironment(ctx context.Context, l *logger.Logger, principal *auth.Principal, name string, input InputUpdateDTO) (envmodels.Environment, error)
	// DeleteEnvironment removes an environment. Requires :all or :own.
	DeleteEnvironment(ctx context.Context, l *logger.Logger, principal *auth.Principal, name string) error
	// GetEnvironmentResolved returns the environment with all secret values
	// resolved. This is only called internally by the provisioning task handler
	// and is never exposed via API.
	GetEnvironmentResolved(ctx context.Context, l *logger.Logger, name string) (ResolvedEnvironment, error)

	// GetVariable returns a single variable.
	GetVariable(ctx context.Context, l *logger.Logger, principal *auth.Principal, envName, key string) (envmodels.EnvironmentVariable, error)
	// GetVariables returns all variables for an environment.
	GetVariables(ctx context.Context, l *logger.Logger, principal *auth.Principal, envName string) ([]envmodels.EnvironmentVariable, error)
	// CreateVariable creates a new variable. Secret-type variables with a
	// recognized prefix are verified; raw values are auto-written to
	// Secret Manager.
	CreateVariable(ctx context.Context, l *logger.Logger, principal *auth.Principal, envName string, input InputCreateVariableDTO) (envmodels.EnvironmentVariable, error)
	// UpdateVariable updates an existing variable.
	UpdateVariable(ctx context.Context, l *logger.Logger, principal *auth.Principal, envName, key string, input InputUpdateVariableDTO) (envmodels.EnvironmentVariable, error)
	// DeleteVariable removes a variable.
	DeleteVariable(ctx context.Context, l *logger.Logger, principal *auth.Principal, envName, key string) error
}

// InputCreateDTO is the data transfer object for creating an environment.
type InputCreateDTO struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
}

// InputUpdateDTO is the data transfer object for updating an environment.
type InputUpdateDTO struct {
	Description string `json:"description"`
}

// InputCreateVariableDTO is the input for creating a variable.
type InputCreateVariableDTO struct {
	Key   string                       `json:"key" binding:"required"`
	Value string                       `json:"value"`
	Type  envmodels.EnvironmentVarType `json:"type"`
}

// InputUpdateVariableDTO is the input for updating a variable.
type InputUpdateVariableDTO struct {
	Value string                       `json:"value"`
	Type  envmodels.EnvironmentVarType `json:"type"`
}

// ResolvedEnvironment holds an environment whose secret variables have been
// resolved to their actual values. Only used internally by task handlers.
type ResolvedEnvironment struct {
	Name      string
	Variables []ResolvedVariable
}

// ResolvedVariable is a variable with its value fully resolved (secrets fetched
// from the appropriate secret manager).
type ResolvedVariable struct {
	Key   string
	Value string
	Type  envmodels.EnvironmentVarType
}
