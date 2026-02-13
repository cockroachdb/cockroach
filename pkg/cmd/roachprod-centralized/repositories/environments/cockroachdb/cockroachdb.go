// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"log/slog"
	"strings"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// CRDBEnvironmentsRepo is a CockroachDB implementation of the environments
// repository.
type CRDBEnvironmentsRepo struct {
	db *gosql.DB
}

// NewEnvironmentsRepository creates a new CockroachDB environments repository.
func NewEnvironmentsRepository(db *gosql.DB) *CRDBEnvironmentsRepo {
	return &CRDBEnvironmentsRepo{db: db}
}

// GetEnvironment retrieves a single environment by name.
func (r *CRDBEnvironmentsRepo) GetEnvironment(
	ctx context.Context, l *logger.Logger, name string,
) (envmodels.Environment, error) {
	query := `SELECT name, description, owner, created_at, updated_at FROM environments WHERE name = $1`

	var env envmodels.Environment
	err := r.db.QueryRowContext(ctx, query, name).Scan(
		&env.Name,
		&env.Description,
		&env.Owner,
		&env.CreatedAt,
		&env.UpdatedAt,
	)
	if errors.Is(err, gosql.ErrNoRows) {
		return envmodels.Environment{}, environments.ErrEnvironmentNotFound
	}
	if err != nil {
		return envmodels.Environment{}, errors.Wrap(err, "query environment")
	}
	return env, nil
}

// GetEnvironments retrieves all environments ordered by name.
func (r *CRDBEnvironmentsRepo) GetEnvironments(
	ctx context.Context, l *logger.Logger,
) ([]envmodels.Environment, error) {
	query := `SELECT name, description, owner, created_at, updated_at FROM environments ORDER BY name`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "query environments")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetEnvironments"),
				slog.Any("error", err))
		}
	}()

	var result []envmodels.Environment
	for rows.Next() {
		var env envmodels.Environment
		if err := rows.Scan(
			&env.Name,
			&env.Description,
			&env.Owner,
			&env.CreatedAt,
			&env.UpdatedAt,
		); err != nil {
			return nil, errors.Wrap(err, "scan environment row")
		}
		result = append(result, env)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate environment rows")
	}
	return result, nil
}

// StoreEnvironment persists a new environment.
func (r *CRDBEnvironmentsRepo) StoreEnvironment(
	ctx context.Context, l *logger.Logger, env envmodels.Environment,
) error {
	query := `INSERT INTO environments (name, description, owner) VALUES ($1, $2, $3)`
	_, err := r.db.ExecContext(ctx, query, env.Name, env.Description, env.Owner)
	if err != nil {
		if isUniqueViolation(err) {
			return environments.ErrEnvironmentAlreadyExists
		}
		return errors.Wrap(err, "insert environment")
	}
	return nil
}

// UpdateEnvironment updates an existing environment.
func (r *CRDBEnvironmentsRepo) UpdateEnvironment(
	ctx context.Context, l *logger.Logger, env envmodels.Environment,
) error {
	query := `UPDATE environments SET description = $2, updated_at = now() WHERE name = $1`
	result, err := r.db.ExecContext(ctx, query, env.Name, env.Description)
	if err != nil {
		return errors.Wrap(err, "update environment")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "affected rows")
	}
	if rowsAffected == 0 {
		return environments.ErrEnvironmentNotFound
	}
	return nil
}

// DeleteEnvironment removes an environment by name.
func (r *CRDBEnvironmentsRepo) DeleteEnvironment(
	ctx context.Context, l *logger.Logger, name string,
) error {
	query := `DELETE FROM environments WHERE name = $1`
	result, err := r.db.ExecContext(ctx, query, name)
	if err != nil {
		// Check for FK violation (provisionings reference this environment).
		if isForeignKeyViolation(err) {
			return errors.Wrap(
				environments.ErrEnvironmentHasProvisionings,
				"environment has active provisionings",
			)
		}
		return errors.Wrap(err, "delete environment")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "affected rows")
	}
	if rowsAffected == 0 {
		return environments.ErrEnvironmentNotFound
	}
	return nil
}

// GetVariable retrieves a single variable by environment name and key.
func (r *CRDBEnvironmentsRepo) GetVariable(
	ctx context.Context, l *logger.Logger, envName, key string,
) (envmodels.EnvironmentVariable, error) {
	query := `SELECT environment_name, key, value, type, created_at, updated_at FROM environment_variables WHERE environment_name = $1 AND key = $2`

	var v envmodels.EnvironmentVariable
	err := r.db.QueryRowContext(ctx, query, envName, key).Scan(
		&v.EnvironmentName, &v.Key, &v.Value, &v.Type, &v.CreatedAt, &v.UpdatedAt,
	)
	if errors.Is(err, gosql.ErrNoRows) {
		return envmodels.EnvironmentVariable{}, environments.ErrVariableNotFound
	}
	if err != nil {
		return envmodels.EnvironmentVariable{}, errors.Wrap(err, "query variable")
	}
	return v, nil
}

// GetVariables retrieves all variables for a given environment, ordered by key.
func (r *CRDBEnvironmentsRepo) GetVariables(
	ctx context.Context, l *logger.Logger, envName string,
) ([]envmodels.EnvironmentVariable, error) {
	query := `SELECT environment_name, key, value, type, created_at, updated_at FROM environment_variables WHERE environment_name = $1 ORDER BY key`

	rows, err := r.db.QueryContext(ctx, query, envName)
	if err != nil {
		return nil, errors.Wrap(err, "query variables")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetVariables"),
				slog.Any("error", err))
		}
	}()

	var result []envmodels.EnvironmentVariable
	for rows.Next() {
		var v envmodels.EnvironmentVariable
		if err := rows.Scan(
			&v.EnvironmentName, &v.Key, &v.Value, &v.Type, &v.CreatedAt, &v.UpdatedAt,
		); err != nil {
			return nil, errors.Wrap(err, "scan variable row")
		}
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate variable rows")
	}
	return result, nil
}

// StoreVariable persists a new variable on an environment.
func (r *CRDBEnvironmentsRepo) StoreVariable(
	ctx context.Context, l *logger.Logger, variable envmodels.EnvironmentVariable,
) error {
	query := `INSERT INTO environment_variables (environment_name, key, value, type) VALUES ($1, $2, $3, $4)`
	_, err := r.db.ExecContext(ctx, query, variable.EnvironmentName, variable.Key, variable.Value, variable.Type)
	if err != nil {
		if isUniqueViolation(err) {
			return environments.ErrVariableAlreadyExists
		}
		return errors.Wrap(err, "insert variable")
	}
	return nil
}

// UpdateVariable updates an existing variable's value and type.
func (r *CRDBEnvironmentsRepo) UpdateVariable(
	ctx context.Context, l *logger.Logger, variable envmodels.EnvironmentVariable,
) error {
	query := `UPDATE environment_variables SET value = $3, type = $4, updated_at = now() WHERE environment_name = $1 AND key = $2`
	result, err := r.db.ExecContext(ctx, query, variable.EnvironmentName, variable.Key, variable.Value, variable.Type)
	if err != nil {
		return errors.Wrap(err, "update variable")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "affected rows")
	}
	if rowsAffected == 0 {
		return environments.ErrVariableNotFound
	}
	return nil
}

// DeleteVariable removes a variable by environment name and key.
func (r *CRDBEnvironmentsRepo) DeleteVariable(
	ctx context.Context, l *logger.Logger, envName, key string,
) error {
	query := `DELETE FROM environment_variables WHERE environment_name = $1 AND key = $2`
	result, err := r.db.ExecContext(ctx, query, envName, key)
	if err != nil {
		return errors.Wrap(err, "delete variable")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "affected rows")
	}
	if rowsAffected == 0 {
		return environments.ErrVariableNotFound
	}
	return nil
}

// isUniqueViolation checks whether the error is a CockroachDB/Postgres unique
// constraint violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	return strings.Contains(err.Error(), "duplicate key value")
}

// isForeignKeyViolation checks whether the error is a CockroachDB/Postgres
// foreign key constraint violation (SQLSTATE 23503).
func isForeignKeyViolation(err error) bool {
	return strings.Contains(err.Error(), "foreign key") ||
		strings.Contains(err.Error(), "violates foreign key constraint")
}
