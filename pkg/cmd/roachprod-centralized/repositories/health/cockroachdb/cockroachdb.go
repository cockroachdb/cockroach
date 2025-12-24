// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	rhealth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CRDBHealthRepo is a CockroachDB implementation of the health repository.
type CRDBHealthRepo struct {
	db *gosql.DB
}

// NewHealthRepository creates a new CockroachDB health repository.
func NewHealthRepository(db *gosql.DB) *CRDBHealthRepo {
	return &CRDBHealthRepo{db: db}
}

// RegisterInstance registers or updates an instance in the database.
func (r *CRDBHealthRepo) RegisterInstance(
	ctx context.Context, l *logger.Logger, instance health.InstanceInfo,
) error {
	metadataJSON, _ := json.Marshal(instance.Metadata)

	query := `
		INSERT INTO instance_health (instance_id, hostname, mode, started_at, last_heartbeat, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (instance_id) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			mode = EXCLUDED.mode,
			started_at = EXCLUDED.started_at,
			last_heartbeat = EXCLUDED.last_heartbeat,
			metadata = EXCLUDED.metadata`

	_, err := r.db.ExecContext(ctx, query,
		instance.InstanceID, instance.Hostname, int(instance.Mode), instance.StartedAt,
		instance.LastHeartbeat, metadataJSON)

	return err
}

// UpdateHeartbeat updates the heartbeat timestamp for an instance.
func (r *CRDBHealthRepo) UpdateHeartbeat(
	ctx context.Context, l *logger.Logger, instanceID string,
) error {
	query := `
		UPDATE instance_health 
		SET last_heartbeat = $1
		WHERE instance_id = $2`

	_, err := r.db.ExecContext(ctx, query, timeutil.Now(), instanceID)
	return err
}

// GetInstance retrieves a specific instance by ID.
func (r *CRDBHealthRepo) GetInstance(
	ctx context.Context, l *logger.Logger, instanceID string,
) (*health.InstanceInfo, error) {
	query := `
		SELECT instance_id, hostname, mode, started_at, last_heartbeat, metadata
		FROM instance_health
		WHERE instance_id = $1`

	var instance health.InstanceInfo
	var metadataJSON []byte
	var mode int

	err := r.db.QueryRowContext(ctx, query, instanceID).Scan(
		&instance.InstanceID, &instance.Hostname, &mode, &instance.StartedAt,
		&instance.LastHeartbeat, &metadataJSON)

	if errors.Is(err, gosql.ErrNoRows) {
		return nil, rhealth.ErrInstanceNotFound
	}
	if err != nil {
		return nil, err
	}

	instance.Mode = health.Mode(mode)

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &instance.Metadata); err != nil {
			l.Error("failed to unmarshal instance metadata",
				slog.String("operation", "GetInstanceByID"),
				slog.String("instance_id", instanceID),
				slog.Any("error", err))
		}
	}

	return &instance, nil
}

// IsInstanceHealthy checks if an instance is healthy within the given timeout.
func (r *CRDBHealthRepo) IsInstanceHealthy(
	ctx context.Context, l *logger.Logger, instanceID string, timeout time.Duration,
) (bool, error) {
	query := `
		SELECT last_heartbeat > $1
		FROM instance_health 
		WHERE instance_id = $2`

	threshold := timeutil.Now().Add(-timeout)
	var healthy bool
	err := r.db.QueryRowContext(ctx, query, threshold, instanceID).Scan(&healthy)

	if errors.Is(err, gosql.ErrNoRows) {
		return false, nil
	}
	return healthy, err
}

// GetHealthyInstances returns all healthy instances within the given timeout.
func (r *CRDBHealthRepo) GetHealthyInstances(
	ctx context.Context, l *logger.Logger, timeout time.Duration,
) ([]health.InstanceInfo, error) {
	query := `
		SELECT instance_id, hostname, mode, started_at, last_heartbeat, metadata
		FROM instance_health
		WHERE last_heartbeat > $1
		ORDER BY last_heartbeat DESC`

	threshold := timeutil.Now().Add(-timeout)
	rows, err := r.db.QueryContext(ctx, query, threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var instances []health.InstanceInfo
	for rows.Next() {
		var instance health.InstanceInfo
		var metadataJSON []byte
		var mode int

		err := rows.Scan(&instance.InstanceID, &instance.Hostname, &mode, &instance.StartedAt,
			&instance.LastHeartbeat, &metadataJSON)
		if err != nil {
			return nil, err
		}

		instance.Mode = health.Mode(mode)

		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &instance.Metadata); err != nil {
				l.Error("failed to unmarshal instance metadata",
					slog.String("operation", "GetHealthyInstances"),
					slog.String("instance_id", instance.InstanceID),
					slog.Any("error", err))
			}
		}

		instances = append(instances, instance)
	}

	return instances, rows.Err()
}

// CleanupDeadInstances removes dead instances that are beyond the retention period.
func (r *CRDBHealthRepo) CleanupDeadInstances(
	ctx context.Context, l *logger.Logger, timeout time.Duration, retentionPeriod time.Duration,
) (int, error) {
	query := `
		DELETE FROM instance_health 
		WHERE last_heartbeat < $1`

	retentionThreshold := timeutil.Now().Add(-retentionPeriod)

	result, err := r.db.ExecContext(ctx, query, retentionThreshold)

	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	return int(rowsAffected), err
}
