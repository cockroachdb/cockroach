// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"fmt"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	// ErrProvisioningNotFound is returned when a provisioning does not exist.
	ErrProvisioningNotFound = fmt.Errorf("provisioning not found")
	// ErrProvisioningAlreadyExists is returned when creating a provisioning
	// that already exists (e.g., identifier collision).
	ErrProvisioningAlreadyExists = fmt.Errorf("provisioning already exists")
	// ErrEnvironmentNotFound is returned when a provisioning references an
	// environment that does not exist (FK constraint violation).
	ErrEnvironmentNotFound = fmt.Errorf("environment not found")
)

// IProvisioningsRepository defines the persistence interface for provisionings.
type IProvisioningsRepository interface {
	// GetProvisioning retrieves a single provisioning by ID.
	GetProvisioning(ctx context.Context, l *logger.Logger, id uuid.UUID) (provmodels.Provisioning, error)
	// GetProvisionings retrieves provisionings matching the given filters,
	// ordered by created_at DESC. Returns the matching provisionings and total
	// count.
	GetProvisionings(ctx context.Context, l *logger.Logger, filters filtertypes.FilterSet) ([]provmodels.Provisioning, int, error)
	// StoreProvisioning persists a new provisioning.
	StoreProvisioning(ctx context.Context, l *logger.Logger, provisioning provmodels.Provisioning) error
	// UpdateProvisioning updates an existing provisioning.
	UpdateProvisioning(ctx context.Context, l *logger.Logger, provisioning provmodels.Provisioning) error
	// DeleteProvisioning removes a provisioning by ID.
	DeleteProvisioning(ctx context.Context, l *logger.Logger, id uuid.UUID) error
	// GetExpiredProvisionings returns provisionings where expires_at <= now()
	// and state is not destroyed or destroying. Used by the GC watcher to
	// schedule destroy tasks.
	GetExpiredProvisionings(ctx context.Context, l *logger.Logger) ([]provmodels.Provisioning, error)
}
