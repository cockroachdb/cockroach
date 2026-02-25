// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package environments

import (
	"context"
	"fmt"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

var (
	// ErrEnvironmentNotFound is returned when an environment does not exist.
	ErrEnvironmentNotFound = fmt.Errorf("environment not found")
	// ErrEnvironmentAlreadyExists is returned when creating an environment
	// with a name that is already in use.
	ErrEnvironmentAlreadyExists = fmt.Errorf("environment already exists")
	// ErrEnvironmentHasProvisionings is returned when attempting to delete
	// an environment that is referenced by active provisionings (FK constraint).
	ErrEnvironmentHasProvisionings = fmt.Errorf("environment has active provisionings")
	// ErrVariableNotFound is returned when an environment variable does not
	// exist for the given environment and key.
	ErrVariableNotFound = fmt.Errorf("environment variable not found")
	// ErrVariableAlreadyExists is returned when creating a variable with
	// a key that already exists on the environment.
	ErrVariableAlreadyExists = fmt.Errorf("environment variable already exists")
)

// IEnvironmentsRepository defines the persistence interface for environments.
type IEnvironmentsRepository interface {
	// GetEnvironment retrieves a single environment by name.
	GetEnvironment(ctx context.Context, l *logger.Logger, name string) (envmodels.Environment, error)
	// GetEnvironments retrieves all environments.
	GetEnvironments(ctx context.Context, l *logger.Logger) ([]envmodels.Environment, error)
	// StoreEnvironment persists a new environment.
	StoreEnvironment(ctx context.Context, l *logger.Logger, env envmodels.Environment) error
	// UpdateEnvironment updates an existing environment.
	UpdateEnvironment(ctx context.Context, l *logger.Logger, env envmodels.Environment) error
	// DeleteEnvironment removes an environment by name.
	DeleteEnvironment(ctx context.Context, l *logger.Logger, name string) error

	// GetVariable retrieves a single variable by environment name and key.
	GetVariable(ctx context.Context, l *logger.Logger, envName, key string) (envmodels.EnvironmentVariable, error)
	// GetVariables retrieves all variables for a given environment.
	GetVariables(ctx context.Context, l *logger.Logger, envName string) ([]envmodels.EnvironmentVariable, error)
	// StoreVariable persists a new variable on an environment.
	StoreVariable(ctx context.Context, l *logger.Logger, variable envmodels.EnvironmentVariable) error
	// UpdateVariable updates an existing variable.
	UpdateVariable(ctx context.Context, l *logger.Logger, variable envmodels.EnvironmentVariable) error
	// DeleteVariable removes a variable by environment name and key.
	DeleteVariable(ctx context.Context, l *logger.Logger, envName, key string) error
}
