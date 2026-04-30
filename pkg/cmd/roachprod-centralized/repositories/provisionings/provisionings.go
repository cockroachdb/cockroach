// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	// GetProvisioning retrieves a single provisioning by ID, including all
	// fields (template_snapshot, plan_output). Use GetProvisioningSummary
	// when the heavy blob fields are not needed.
	GetProvisioning(ctx context.Context, l *logger.Logger, id uuid.UUID) (provmodels.Provisioning, error)
	// GetProvisioningSummary retrieves a provisioning by ID without loading
	// template_snapshot or plan_output. Suitable for API responses and
	// operations that don't need these large fields.
	GetProvisioningSummary(ctx context.Context, l *logger.Logger, id uuid.UUID) (provmodels.Provisioning, error)
	// GetProvisioningForExecution retrieves a provisioning by ID with
	// template_snapshot (needed to extract to disk) but without plan_output
	// (never used by handlers). This avoids loading the multi-MB plan JSON
	// blob during provision/destroy operations.
	GetProvisioningForExecution(ctx context.Context, l *logger.Logger, id uuid.UUID) (provmodels.Provisioning, error)
	// GetProvisionings retrieves provisionings matching the given filters,
	// ordered by created_at DESC. Returns the matching provisionings and total
	// count.
	GetProvisionings(ctx context.Context, l *logger.Logger, filters filtertypes.FilterSet) ([]provmodels.Provisioning, int, error)
	// GetProvisioningsSummary retrieves provisionings matching the given
	// filters without loading template_snapshot or plan_output. Ordered by
	// created_at DESC.
	GetProvisioningsSummary(ctx context.Context, l *logger.Logger, filters filtertypes.FilterSet) ([]provmodels.Provisioning, int, error)
	// StoreProvisioning persists a new provisioning.
	StoreProvisioning(ctx context.Context, l *logger.Logger, provisioning provmodels.Provisioning) error
	// UpdateProvisioning updates an existing provisioning (all fields).
	UpdateProvisioning(ctx context.Context, l *logger.Logger, provisioning provmodels.Provisioning) error
	// UpdateProvisioningProgress updates only the operational fields that
	// change during provisioning lifecycle: state, last_step, error,
	// outputs. Plan data is persisted separately via StorePlanData.
	// This avoids re-writing large immutable fields like template_snapshot
	// and allows callers to nil them in memory.
	UpdateProvisioningProgress(ctx context.Context, l *logger.Logger, provisioning provmodels.Provisioning) error
	// UpdateProvisioningExpiration updates only expires_at (and updated_at).
	// This avoids accidentally wiping large fields when callers only want to
	// extend the provisioning lifetime.
	UpdateProvisioningExpiration(ctx context.Context, l *logger.Logger, id uuid.UUID, expiresAt *time.Time) error
	// StorePlanData persists the plan output JSON or external ref for a
	// provisioning. planOutput and planOutputRef are mutually exclusive; pass
	// zero values to clear them. This is a targeted update, decoupled from
	// UpdateProvisioningProgress so callers can release the plan blob
	// from memory after persisting it.
	StorePlanData(ctx context.Context, l *logger.Logger, id uuid.UUID, planOutput json.RawMessage, planOutputRef string) error
	// DeleteProvisioning removes a provisioning by ID.
	DeleteProvisioning(ctx context.Context, l *logger.Logger, id uuid.UUID) error
	// GetExpiredProvisionings returns provisionings where expires_at <= now()
	// and state is not destroyed or destroying. Excludes template_snapshot
	// and plan_output since GC only needs metadata. Used by the GC watcher
	// to schedule destroy tasks.
	GetExpiredProvisionings(ctx context.Context, l *logger.Logger) ([]provmodels.Provisioning, error)
}
