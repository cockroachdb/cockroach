// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"sort"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MemEnvironmentsRepo is an in-memory implementation of the environments
// repository. Suitable for unit tests and local development.
type MemEnvironmentsRepo struct {
	envs map[string]envmodels.Environment
	vars map[string]map[string]envmodels.EnvironmentVariable
	lock syncutil.Mutex
}

// NewEnvironmentsRepository creates a new in-memory environments repository.
func NewEnvironmentsRepository() *MemEnvironmentsRepo {
	return &MemEnvironmentsRepo{
		envs: make(map[string]envmodels.Environment),
		vars: make(map[string]map[string]envmodels.EnvironmentVariable),
	}
}

// GetEnvironment returns an environment by name.
func (r *MemEnvironmentsRepo) GetEnvironment(
	ctx context.Context, l *logger.Logger, name string,
) (envmodels.Environment, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	env, ok := r.envs[name]
	if !ok {
		return envmodels.Environment{}, environments.ErrEnvironmentNotFound
	}
	return env, nil
}

// GetEnvironments returns all environments.
func (r *MemEnvironmentsRepo) GetEnvironments(
	ctx context.Context, l *logger.Logger,
) ([]envmodels.Environment, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	result := make([]envmodels.Environment, 0, len(r.envs))
	for _, env := range r.envs {
		result = append(result, env)
	}
	return result, nil
}

// StoreEnvironment persists a new environment.
func (r *MemEnvironmentsRepo) StoreEnvironment(
	ctx context.Context, l *logger.Logger, env envmodels.Environment,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, exists := r.envs[env.Name]; exists {
		return environments.ErrEnvironmentAlreadyExists
	}
	now := timeutil.Now()
	env.CreatedAt = now
	env.UpdatedAt = now
	r.envs[env.Name] = env
	return nil
}

// UpdateEnvironment updates an existing environment.
func (r *MemEnvironmentsRepo) UpdateEnvironment(
	ctx context.Context, l *logger.Logger, env envmodels.Environment,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	existing, ok := r.envs[env.Name]
	if !ok {
		return environments.ErrEnvironmentNotFound
	}
	env.Owner = existing.Owner
	env.CreatedAt = existing.CreatedAt
	env.UpdatedAt = timeutil.Now()
	r.envs[env.Name] = env
	return nil
}

// DeleteEnvironment removes an environment by name. Associated variables are
// also deleted (cascade).
//
// NOTE: unlike the CRDB implementation, this does not check for FK references
// from provisionings, so ErrEnvironmentHasProvisionings is never returned.
// This can be revisited once the provisionings domain is implemented.
func (r *MemEnvironmentsRepo) DeleteEnvironment(
	ctx context.Context, l *logger.Logger, name string,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.envs[name]; !ok {
		return environments.ErrEnvironmentNotFound
	}
	delete(r.envs, name)
	delete(r.vars, name)
	return nil
}

// GetVariable returns a single variable by environment name and key.
func (r *MemEnvironmentsRepo) GetVariable(
	ctx context.Context, l *logger.Logger, envName, key string,
) (envmodels.EnvironmentVariable, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	envVars, ok := r.vars[envName]
	if !ok {
		return envmodels.EnvironmentVariable{}, environments.ErrVariableNotFound
	}
	v, ok := envVars[key]
	if !ok {
		return envmodels.EnvironmentVariable{}, environments.ErrVariableNotFound
	}
	return v, nil
}

// GetVariables returns all variables for a given environment, sorted by key.
func (r *MemEnvironmentsRepo) GetVariables(
	ctx context.Context, l *logger.Logger, envName string,
) ([]envmodels.EnvironmentVariable, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	envVars := r.vars[envName]
	result := make([]envmodels.EnvironmentVariable, 0, len(envVars))
	for _, v := range envVars {
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result, nil
}

// StoreVariable persists a new variable on an environment. The parent
// environment must exist.
func (r *MemEnvironmentsRepo) StoreVariable(
	ctx context.Context, l *logger.Logger, variable envmodels.EnvironmentVariable,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.envs[variable.EnvironmentName]; !ok {
		return environments.ErrEnvironmentNotFound
	}
	envVars, ok := r.vars[variable.EnvironmentName]
	if !ok {
		envVars = make(map[string]envmodels.EnvironmentVariable)
		r.vars[variable.EnvironmentName] = envVars
	}
	if _, exists := envVars[variable.Key]; exists {
		return environments.ErrVariableAlreadyExists
	}
	now := timeutil.Now()
	variable.CreatedAt = now
	variable.UpdatedAt = now
	envVars[variable.Key] = variable
	return nil
}

// UpdateVariable updates an existing variable's value and type.
func (r *MemEnvironmentsRepo) UpdateVariable(
	ctx context.Context, l *logger.Logger, variable envmodels.EnvironmentVariable,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	envVars, ok := r.vars[variable.EnvironmentName]
	if !ok {
		return environments.ErrVariableNotFound
	}
	existing, ok := envVars[variable.Key]
	if !ok {
		return environments.ErrVariableNotFound
	}
	variable.EnvironmentName = existing.EnvironmentName
	variable.CreatedAt = existing.CreatedAt
	variable.UpdatedAt = timeutil.Now()
	envVars[variable.Key] = variable
	return nil
}

// DeleteVariable removes a variable by environment name and key.
func (r *MemEnvironmentsRepo) DeleteVariable(
	ctx context.Context, l *logger.Logger, envName, key string,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	envVars, ok := r.vars[envName]
	if !ok {
		return environments.ErrVariableNotFound
	}
	if _, ok := envVars[key]; !ok {
		return environments.ErrVariableNotFound
	}
	delete(envVars, key)
	return nil
}

