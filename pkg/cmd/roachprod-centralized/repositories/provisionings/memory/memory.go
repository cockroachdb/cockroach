// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sort"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// MemProvisioningsRepo is an in-memory implementation of the provisionings
// repository. Suitable for unit tests and local development.
type MemProvisioningsRepo struct {
	data map[uuid.UUID]provmodels.Provisioning
	lock syncutil.Mutex
}

// NewProvisioningsRepository creates a new in-memory provisionings repository.
func NewProvisioningsRepository() *MemProvisioningsRepo {
	return &MemProvisioningsRepo{
		data: make(map[uuid.UUID]provmodels.Provisioning),
	}
}

// GetProvisioning retrieves a single provisioning by ID.
func (r *MemProvisioningsRepo) GetProvisioning(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (provmodels.Provisioning, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	p, ok := r.data[id]
	if !ok {
		return provmodels.Provisioning{}, provisionings.ErrProvisioningNotFound
	}
	return copyProvisioning(p), nil
}

// GetProvisioningSummary retrieves a provisioning by ID without
// TemplateSnapshot or PlanOutput.
func (r *MemProvisioningsRepo) GetProvisioningSummary(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (provmodels.Provisioning, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	p, ok := r.data[id]
	if !ok {
		return provmodels.Provisioning{}, provisionings.ErrProvisioningNotFound
	}
	c := copyProvisioning(p)
	c.TemplateSnapshot = nil
	c.TemplateSnapshotRef = ""
	c.PlanOutput = nil
	c.PlanOutputRef = ""
	return c, nil
}

// GetProvisioningForExecution retrieves a provisioning by ID with
// TemplateSnapshot but without PlanOutput.
func (r *MemProvisioningsRepo) GetProvisioningForExecution(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (provmodels.Provisioning, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	p, ok := r.data[id]
	if !ok {
		return provmodels.Provisioning{}, provisionings.ErrProvisioningNotFound
	}
	c := copyProvisioning(p)
	c.PlanOutput = nil
	return c, nil
}

// GetProvisionings retrieves provisionings, optionally filtered. Uses the
// shared memory filter evaluator for field name translation and operator
// support, matching the behavior of the SQL filter builder in the CRDB repo.
// Results are ordered by created_at DESC.
func (r *MemProvisioningsRepo) GetProvisionings(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]provmodels.Provisioning, int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	evaluator := filters.NewMemoryFilterEvaluatorWithTypeHint(
		reflect.TypeOf(provmodels.Provisioning{}),
	)

	var result []provmodels.Provisioning
	for _, p := range r.data {
		if filterSet.IsEmpty() {
			result = append(result, copyProvisioning(p))
			continue
		}
		matches, err := evaluator.Evaluate(p, &filterSet)
		if err != nil {
			l.Error("error filtering provisioning, skipping",
				slog.String("provisioning_id", p.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if matches {
			result = append(result, copyProvisioning(p))
		}
	}

	// Sort by created_at DESC (newest first).
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	return result, len(result), nil
}

// GetProvisioningsSummary retrieves provisionings matching the given filters
// without TemplateSnapshot or PlanOutput.
func (r *MemProvisioningsRepo) GetProvisioningsSummary(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]provmodels.Provisioning, int, error) {
	provs, count, err := r.GetProvisionings(ctx, l, filterSet)
	if err != nil {
		return nil, 0, err
	}
	for i := range provs {
		provs[i].TemplateSnapshot = nil
		provs[i].TemplateSnapshotRef = ""
		provs[i].PlanOutput = nil
		provs[i].PlanOutputRef = ""
	}
	return provs, count, nil
}

// StoreProvisioning persists a new provisioning.
func (r *MemProvisioningsRepo) StoreProvisioning(
	ctx context.Context, l *logger.Logger, p provmodels.Provisioning,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Check for ID collision.
	if _, exists := r.data[p.ID]; exists {
		return provisionings.ErrProvisioningAlreadyExists
	}
	// Check for identifier uniqueness.
	for _, existing := range r.data {
		if existing.Identifier == p.Identifier {
			return provisionings.ErrProvisioningAlreadyExists
		}
	}

	now := timeutil.Now()
	p.CreatedAt = now
	p.UpdatedAt = now

	if p.Variables == nil {
		p.Variables = make(map[string]interface{})
	}
	if p.Outputs == nil {
		p.Outputs = make(map[string]interface{})
	}

	r.data[p.ID] = copyProvisioning(p)
	return nil
}

// UpdateProvisioning updates an existing provisioning.
func (r *MemProvisioningsRepo) UpdateProvisioning(
	ctx context.Context, l *logger.Logger, p provmodels.Provisioning,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	existing, ok := r.data[p.ID]
	if !ok {
		return provisionings.ErrProvisioningNotFound
	}

	// Preserve immutable fields.
	p.CreatedAt = existing.CreatedAt
	p.UpdatedAt = timeutil.Now()

	if p.Variables == nil {
		p.Variables = make(map[string]interface{})
	}
	if p.Outputs == nil {
		p.Outputs = make(map[string]interface{})
	}

	r.data[p.ID] = copyProvisioning(p)
	return nil
}

// UpdateProvisioningProgress updates only the operational fields that change
// during provisioning lifecycle: state, last_step, error, outputs. Plan data
// is persisted separately via StorePlanData. All other fields are preserved
// from the existing record.
func (r *MemProvisioningsRepo) UpdateProvisioningProgress(
	ctx context.Context, l *logger.Logger, p provmodels.Provisioning,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	existing, ok := r.data[p.ID]
	if !ok {
		return provisionings.ErrProvisioningNotFound
	}

	// Preserve all non-progress fields from the existing record.
	existing.State = p.State
	existing.LastStep = p.LastStep
	existing.Error = p.Error
	existing.Outputs = p.Outputs
	existing.UpdatedAt = timeutil.Now()

	if existing.Outputs == nil {
		existing.Outputs = make(map[string]interface{})
	}

	r.data[p.ID] = copyProvisioning(existing)
	return nil
}

// UpdateProvisioningExpiration updates only ExpiresAt and UpdatedAt.
func (r *MemProvisioningsRepo) UpdateProvisioningExpiration(
	ctx context.Context, l *logger.Logger, id uuid.UUID, expiresAt *time.Time,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	existing, ok := r.data[id]
	if !ok {
		return provisionings.ErrProvisioningNotFound
	}

	if expiresAt != nil {
		t := *expiresAt
		existing.ExpiresAt = &t
	} else {
		existing.ExpiresAt = nil
	}
	existing.UpdatedAt = timeutil.Now()
	r.data[id] = copyProvisioning(existing)
	return nil
}

// StorePlanData persists the plan output JSON and/or external ref for a
// provisioning.
func (r *MemProvisioningsRepo) StorePlanData(
	ctx context.Context,
	l *logger.Logger,
	id uuid.UUID,
	planOutput json.RawMessage,
	planOutputRef string,
) error {
	if planOutput != nil && planOutputRef != "" {
		return errors.New("planOutput and planOutputRef are mutually exclusive")
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	existing, ok := r.data[id]
	if !ok {
		return provisionings.ErrProvisioningNotFound
	}

	existing.PlanOutput = planOutput
	existing.PlanOutputRef = planOutputRef
	existing.UpdatedAt = timeutil.Now()
	r.data[id] = copyProvisioning(existing)
	return nil
}

// DeleteProvisioning removes a provisioning by ID.
func (r *MemProvisioningsRepo) DeleteProvisioning(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.data[id]; !ok {
		return provisionings.ErrProvisioningNotFound
	}
	delete(r.data, id)
	return nil
}

// GetExpiredProvisionings returns provisionings where expires_at <= now()
// and state is not destroyed or destroying. Excludes template_snapshot
// and plan_output since GC only needs metadata.
func (r *MemProvisioningsRepo) GetExpiredProvisionings(
	ctx context.Context, l *logger.Logger,
) ([]provmodels.Provisioning, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	now := timeutil.Now()
	var result []provmodels.Provisioning
	for _, p := range r.data {
		if p.ExpiresAt != nil && !p.ExpiresAt.After(now) &&
			p.State != provmodels.ProvisioningStateDestroyed &&
			p.State != provmodels.ProvisioningStateDestroying {
			c := copyProvisioning(p)
			c.TemplateSnapshot = nil
			c.TemplateSnapshotRef = ""
			c.PlanOutput = nil
			c.PlanOutputRef = ""
			result = append(result, c)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ExpiresAt.Before(*result[j].ExpiresAt)
	})

	return result, nil
}

// copyProvisioning returns a deep copy of a provisioning. Nested maps, slices,
// byte slices, and pointer fields are fully cloned to prevent aliasing.
func copyProvisioning(p provmodels.Provisioning) provmodels.Provisioning {
	p.Variables = deepCopyMap(p.Variables)
	p.Outputs = deepCopyMap(p.Outputs)
	if p.TemplateSnapshot != nil {
		snap := make([]byte, len(p.TemplateSnapshot))
		copy(snap, p.TemplateSnapshot)
		p.TemplateSnapshot = snap
	}
	if p.PlanOutput != nil {
		plan := make([]byte, len(p.PlanOutput))
		copy(plan, p.PlanOutput)
		p.PlanOutput = plan
	}
	if p.ExpiresAt != nil {
		t := *p.ExpiresAt
		p.ExpiresAt = &t
	}
	return p
}

// deepCopyMap recursively clones a map[string]interface{}. The concrete value
// types are those produced by JSON deserialization: string, float64, bool, nil,
// []interface{}, and map[string]interface{}.
func deepCopyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return deepCopyMap(val)
	case []interface{}:
		cp := make([]interface{}, len(val))
		for i, elem := range val {
			cp[i] = deepCopyValue(elem)
		}
		return cp
	default:
		// Primitive types (string, float64, bool, nil) are immutable.
		return v
	}
}
