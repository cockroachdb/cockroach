// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CRDBProvisioningsRepo is a CockroachDB implementation of the provisionings
// repository.
type CRDBProvisioningsRepo struct {
	db *gosql.DB
}

// NewProvisioningsRepository creates a new CockroachDB provisionings
// repository.
func NewProvisioningsRepository(db *gosql.DB) *CRDBProvisioningsRepo {
	return &CRDBProvisioningsRepo{db: db}
}

// allColumns is the ordered list of columns used in SELECT queries (19 total).
// Every scan helper must use this exact order.
const allColumns = "id, name, environment, template_type, template_checksum, " +
	"template_snapshot, state, identifier, variables, outputs, plan_output, " +
	"error, owner, cluster_name, lifetime_seconds, created_at, updated_at, " +
	"expires_at, last_step"

// scanProvisioning scans a single row into a Provisioning. The row must
// contain the columns listed in allColumns, in that order.
func scanProvisioning(
	scanner interface{ Scan(dest ...interface{}) error },
) (provmodels.Provisioning, error) {
	var p provmodels.Provisioning

	var variables []byte
	var outputs []byte
	var planOutput gosql.NullString
	var clusterName gosql.NullString
	var lifetimeSeconds int64
	var expiresAt gosql.NullTime
	var errorStr gosql.NullString
	var lastStep gosql.NullString

	err := scanner.Scan(
		&p.ID,
		&p.Name,
		&p.Environment,
		&p.TemplateType,
		&p.TemplateChecksum,
		&p.TemplateSnapshot, // BYTEA scans into []byte directly
		&p.State,
		&p.Identifier,
		&variables,
		&outputs,
		&planOutput,
		&errorStr,
		&p.Owner,
		&clusterName,
		&lifetimeSeconds,
		&p.CreatedAt,
		&p.UpdatedAt,
		&expiresAt,
		&lastStep,
	)
	if err != nil {
		return provmodels.Provisioning{}, err
	}

	// Unmarshal JSONB columns.
	if len(variables) > 0 {
		if err := json.Unmarshal(variables, &p.Variables); err != nil {
			return provmodels.Provisioning{}, errors.Wrap(err, "unmarshal variables")
		}
	}
	if p.Variables == nil {
		p.Variables = make(map[string]interface{})
	}
	if len(outputs) > 0 {
		if err := json.Unmarshal(outputs, &p.Outputs); err != nil {
			return provmodels.Provisioning{}, errors.Wrap(err, "unmarshal outputs")
		}
	}
	if p.Outputs == nil {
		p.Outputs = make(map[string]interface{})
	}

	// Nullable columns.
	if planOutput.Valid {
		p.PlanOutput = json.RawMessage(planOutput.String)
	}
	if clusterName.Valid {
		p.ClusterName = clusterName.String
	}
	if expiresAt.Valid {
		p.ExpiresAt = &expiresAt.Time
	}
	if errorStr.Valid {
		p.Error = errorStr.String
	}
	if lastStep.Valid {
		p.LastStep = lastStep.String
	}

	p.Lifetime = time.Duration(lifetimeSeconds) * time.Second

	return p, nil
}

// GetProvisioning retrieves a single provisioning by ID.
func (r *CRDBProvisioningsRepo) GetProvisioning(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (provmodels.Provisioning, error) {
	query := `SELECT ` + allColumns + ` FROM provisionings WHERE id = $1`

	p, err := scanProvisioning(r.db.QueryRowContext(ctx, query, id))
	if errors.Is(err, gosql.ErrNoRows) {
		return provmodels.Provisioning{}, provisionings.ErrProvisioningNotFound
	}
	if err != nil {
		return provmodels.Provisioning{}, errors.Wrap(err, "query provisioning")
	}
	return p, nil
}

// GetProvisionings retrieves provisionings matching the given filters.
func (r *CRDBProvisioningsRepo) GetProvisionings(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]provmodels.Provisioning, int, error) {
	baseQuery := `SELECT ` + allColumns + ` FROM provisionings`

	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(provmodels.Provisioning{}),
	)
	whereClause, args, err := qb.BuildWhere(&filterSet)
	if err != nil {
		return nil, 0, errors.Wrap(err, "build query filters")
	}

	query := baseQuery
	if whereClause != "" {
		query += " " + whereClause
	}
	query += " ORDER BY created_at DESC"

	l.Debug("querying provisionings from database",
		slog.String("query", query),
		slog.Int("args_count", len(args)),
	)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "query provisionings")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetProvisionings"),
				slog.Any("error", err))
		}
	}()

	var result []provmodels.Provisioning
	for rows.Next() {
		p, err := scanProvisioning(rows)
		if err != nil {
			return nil, 0, errors.Wrap(err, "scan provisioning row")
		}
		result = append(result, p)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "iterate provisioning rows")
	}

	return result, len(result), nil
}

// StoreProvisioning persists a new provisioning.
func (r *CRDBProvisioningsRepo) StoreProvisioning(
	ctx context.Context, l *logger.Logger, p provmodels.Provisioning,
) error {
	variablesJSON, err := json.Marshal(p.Variables)
	if err != nil {
		return errors.Wrap(err, "marshal variables")
	}
	outputsJSON, err := json.Marshal(p.Outputs)
	if err != nil {
		return errors.Wrap(err, "marshal outputs")
	}

	var planOutputVal interface{}
	if p.PlanOutput != nil {
		planOutputVal = string(p.PlanOutput)
	}

	var clusterNameVal interface{}
	if p.ClusterName != "" {
		clusterNameVal = p.ClusterName
	}

	var expiresAtVal interface{}
	if p.ExpiresAt != nil {
		expiresAtVal = *p.ExpiresAt
	}

	lifetimeSeconds := int64(p.Lifetime.Seconds())

	query := `INSERT INTO provisionings (
		id, name, environment, template_type, template_checksum,
		template_snapshot, state, identifier, variables, outputs,
		plan_output, error, owner, cluster_name, lifetime_seconds,
		expires_at, last_step
	) VALUES (
		$1, $2, $3, $4, $5,
		$6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15,
		$16, $17
	)`

	_, err = r.db.ExecContext(ctx, query,
		p.ID,
		p.Name,
		p.Environment,
		p.TemplateType,
		p.TemplateChecksum,
		p.TemplateSnapshot, // []byte → BYTEA
		p.State,
		p.Identifier,
		variablesJSON,
		outputsJSON,
		planOutputVal,
		p.Error,
		p.Owner,
		clusterNameVal,
		lifetimeSeconds,
		expiresAtVal,
		p.LastStep,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return provisionings.ErrProvisioningAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return provisionings.ErrEnvironmentNotFound
		}
		return errors.Wrap(err, "insert provisioning")
	}
	return nil
}

// UpdateProvisioning updates an existing provisioning.
func (r *CRDBProvisioningsRepo) UpdateProvisioning(
	ctx context.Context, l *logger.Logger, p provmodels.Provisioning,
) error {
	variablesJSON, err := json.Marshal(p.Variables)
	if err != nil {
		return errors.Wrap(err, "marshal variables")
	}
	outputsJSON, err := json.Marshal(p.Outputs)
	if err != nil {
		return errors.Wrap(err, "marshal outputs")
	}

	var planOutputVal interface{}
	if p.PlanOutput != nil {
		planOutputVal = string(p.PlanOutput)
	}

	var clusterNameVal interface{}
	if p.ClusterName != "" {
		clusterNameVal = p.ClusterName
	}

	var expiresAtVal interface{}
	if p.ExpiresAt != nil {
		expiresAtVal = *p.ExpiresAt
	}

	lifetimeSeconds := int64(p.Lifetime.Seconds())

	query := `UPDATE provisionings SET
		name = $2,
		environment = $3,
		template_type = $4,
		template_checksum = $5,
		template_snapshot = $6,
		state = $7,
		identifier = $8,
		variables = $9,
		outputs = $10,
		plan_output = $11,
		error = $12,
		owner = $13,
		cluster_name = $14,
		lifetime_seconds = $15,
		updated_at = now(),
		expires_at = $16,
		last_step = $17
	WHERE id = $1`

	result, err := r.db.ExecContext(ctx, query,
		p.ID,
		p.Name,
		p.Environment,
		p.TemplateType,
		p.TemplateChecksum,
		p.TemplateSnapshot,
		p.State,
		p.Identifier,
		variablesJSON,
		outputsJSON,
		planOutputVal,
		p.Error,
		p.Owner,
		clusterNameVal,
		lifetimeSeconds,
		expiresAtVal,
		p.LastStep,
	)
	if err != nil {
		return errors.Wrap(err, "update provisioning")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "affected rows")
	}
	if rowsAffected == 0 {
		return provisionings.ErrProvisioningNotFound
	}
	return nil
}

// DeleteProvisioning removes a provisioning by ID.
func (r *CRDBProvisioningsRepo) DeleteProvisioning(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	query := `DELETE FROM provisionings WHERE id = $1`
	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return errors.Wrap(err, "delete provisioning")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "affected rows")
	}
	if rowsAffected == 0 {
		return provisionings.ErrProvisioningNotFound
	}
	return nil
}

// GetExpiredProvisionings returns provisionings where expires_at <= now()
// and state is 'provisioned'. Used by the TTL watcher to schedule destroy
// tasks.
func (r *CRDBProvisioningsRepo) GetExpiredProvisionings(
	ctx context.Context, l *logger.Logger,
) ([]provmodels.Provisioning, error) {
	query := `SELECT ` + allColumns + ` FROM provisionings
		WHERE expires_at IS NOT NULL
		AND expires_at <= now()
		AND state = $1
		ORDER BY expires_at ASC`

	rows, err := r.db.QueryContext(ctx, query, provmodels.ProvisioningStateProvisioned)
	if err != nil {
		return nil, errors.Wrap(err, "query expired provisionings")
	}
	defer func() {
		if err := rows.Close(); err != nil {
			l.Warn("failed to close database rows",
				slog.String("operation", "GetExpiredProvisionings"),
				slog.Any("error", err))
		}
	}()

	var result []provmodels.Provisioning
	for rows.Next() {
		p, err := scanProvisioning(rows)
		if err != nil {
			return nil, errors.Wrap(err, "scan expired provisioning row")
		}
		result = append(result, p)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate expired provisioning rows")
	}
	return result, nil
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
