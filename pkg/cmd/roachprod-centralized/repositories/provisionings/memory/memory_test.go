// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func newTestProvisioning(t *testing.T) provmodels.Provisioning {
	t.Helper()
	id := uuid.MakeV4()
	identifier, err := provmodels.GenerateIdentifier()
	require.NoError(t, err)
	return provmodels.Provisioning{
		ID:               id,
		Name:             "test-prov",
		Environment:      "test-env",
		TemplateType:     "gcs-bucket",
		TemplateChecksum: "abc123",
		TemplateSnapshot: []byte("fake-snapshot-data"),
		State:            provmodels.ProvisioningStateNew,
		Identifier:       identifier,
		Variables:        map[string]interface{}{"region": "us-east1"},
		Outputs:          map[string]interface{}{},
		Owner:            "alice@example.com",
		LastStep:         "",
	}
}

func TestMemProvisioningsRepo(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	t.Run("StoreAndGet", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)

		err := repo.StoreProvisioning(ctx, l, p)
		require.NoError(t, err)

		got, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Equal(t, p.ID, got.ID)
		require.Equal(t, "test-prov", got.Name)
		require.Equal(t, "test-env", got.Environment)
		require.Equal(t, "gcs-bucket", got.TemplateType)
		require.Equal(t, "abc123", got.TemplateChecksum)
		require.Equal(t, []byte("fake-snapshot-data"), got.TemplateSnapshot)
		require.Equal(t, provmodels.ProvisioningStateNew, got.State)
		require.Equal(t, p.Identifier, got.Identifier)
		require.Equal(t, "us-east1", got.Variables["region"])
		require.Equal(t, "alice@example.com", got.Owner)
		require.False(t, got.CreatedAt.IsZero())
		require.False(t, got.UpdatedAt.IsZero())
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		_, err := repo.GetProvisioning(ctx, l, uuid.MakeV4())
		require.ErrorIs(t, err, provisionings.ErrProvisioningNotFound)
	})

	t.Run("StoreDuplicateID", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		// Same ID, different identifier.
		p2 := newTestProvisioning(t)
		p2.ID = p.ID
		err := repo.StoreProvisioning(ctx, l, p2)
		require.ErrorIs(t, err, provisionings.ErrProvisioningAlreadyExists)
	})

	t.Run("StoreDuplicateIdentifier", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		// Different ID, same identifier.
		p2 := newTestProvisioning(t)
		p2.Identifier = p.Identifier
		err := repo.StoreProvisioning(ctx, l, p2)
		require.ErrorIs(t, err, provisionings.ErrProvisioningAlreadyExists)
	})

	t.Run("StoreReturnsDeepCopy", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		// Mutate the original struct — stored copy must not be affected.
		p.Variables["region"] = "MUTATED"
		p.TemplateSnapshot[0] = 0xFF

		got, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Equal(t, "us-east1", got.Variables["region"])
		require.Equal(t, byte('f'), got.TemplateSnapshot[0])
	})

	t.Run("GetReturnsDeepCopy", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		got1, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		got1.Variables["region"] = "MUTATED"

		got2, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Equal(t, "us-east1", got2.Variables["region"])
	})

	t.Run("Update", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		p.State = provmodels.ProvisioningStateProvisioned
		p.Outputs = map[string]interface{}{"bucket_name": "my-bucket"}
		p.LastStep = "done"
		err := repo.UpdateProvisioning(ctx, l, p)
		require.NoError(t, err)

		got, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Equal(t, provmodels.ProvisioningStateProvisioned, got.State)
		require.Equal(t, "my-bucket", got.Outputs["bucket_name"])
		require.Equal(t, "done", got.LastStep)
		// CreatedAt must be preserved.
		require.True(t, got.UpdatedAt.After(got.CreatedAt) || got.UpdatedAt.Equal(got.CreatedAt))
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		err := repo.UpdateProvisioning(ctx, l, p)
		require.ErrorIs(t, err, provisionings.ErrProvisioningNotFound)
	})

	t.Run("Delete", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		require.NoError(t, repo.DeleteProvisioning(ctx, l, p.ID))

		_, err := repo.GetProvisioning(ctx, l, p.ID)
		require.ErrorIs(t, err, provisionings.ErrProvisioningNotFound)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		err := repo.DeleteProvisioning(ctx, l, uuid.MakeV4())
		require.ErrorIs(t, err, provisionings.ErrProvisioningNotFound)
	})

	t.Run("BYTEASnapshotRoundTrip", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		// Use actual binary data that would break if treated as text.
		p.TemplateSnapshot = []byte{0x00, 0x1F, 0x8B, 0xFF, 0xFE}
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		got, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Equal(t, p.TemplateSnapshot, got.TemplateSnapshot)
	})

	t.Run("NullableColumns", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		// Leave nullable fields as zero values.
		p.PlanOutput = nil
		p.ExpiresAt = nil
		p.ClusterName = ""
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		got, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Nil(t, got.PlanOutput)
		require.Nil(t, got.ExpiresAt)
		require.Empty(t, got.ClusterName)

		// Now set them and update.
		now := timeutil.Now()
		p.PlanOutput = json.RawMessage(`{"changes": true}`)
		p.ExpiresAt = &now
		p.ClusterName = "my-cluster"
		require.NoError(t, repo.UpdateProvisioning(ctx, l, p))

		got, err = repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.JSONEq(t, `{"changes": true}`, string(got.PlanOutput))
		require.NotNil(t, got.ExpiresAt)
		require.Equal(t, "my-cluster", got.ClusterName)
	})

	t.Run("JSONBVariablesRoundTrip", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		p.Variables = map[string]interface{}{
			"string_val": "hello",
			"number_val": float64(42),
			"bool_val":   true,
			"nested": map[string]interface{}{
				"inner": "value",
			},
		}
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		got, err := repo.GetProvisioning(ctx, l, p.ID)
		require.NoError(t, err)
		require.Equal(t, "hello", got.Variables["string_val"])
		require.Equal(t, float64(42), got.Variables["number_val"])
		require.Equal(t, true, got.Variables["bool_val"])
		nested, ok := got.Variables["nested"].(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "value", nested["inner"])
	})
}

func TestMemProvisioningsRepoGetAll(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	t.Run("EmptyRepo", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		result, count, err := repo.GetProvisionings(ctx, l, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Empty(t, result)
		require.Equal(t, 0, count)
	})

	t.Run("NoFilter", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		p1 := newTestProvisioning(t)
		p1.Name = "prov-1"
		require.NoError(t, repo.StoreProvisioning(ctx, l, p1))

		p2 := newTestProvisioning(t)
		p2.Name = "prov-2"
		require.NoError(t, repo.StoreProvisioning(ctx, l, p2))

		result, count, err := repo.GetProvisionings(ctx, l, *filters.NewFilterSet())
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, 2, count)
	})

	t.Run("FilterByState", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		p1 := newTestProvisioning(t)
		p1.State = provmodels.ProvisioningStateNew
		require.NoError(t, repo.StoreProvisioning(ctx, l, p1))

		p2 := newTestProvisioning(t)
		p2.State = provmodels.ProvisioningStateProvisioned
		require.NoError(t, repo.StoreProvisioning(ctx, l, p2))

		fs := *filters.NewFilterSet().AddFilter("State", filtertypes.OpEqual, provmodels.ProvisioningStateProvisioned)
		result, count, err := repo.GetProvisionings(ctx, l, fs)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, 1, count)
		require.Equal(t, provmodels.ProvisioningStateProvisioned, result[0].State)
	})

	t.Run("FilterByEnvironment", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		p1 := newTestProvisioning(t)
		p1.Environment = "prod"
		require.NoError(t, repo.StoreProvisioning(ctx, l, p1))

		p2 := newTestProvisioning(t)
		p2.Environment = "staging"
		require.NoError(t, repo.StoreProvisioning(ctx, l, p2))

		fs := *filters.NewFilterSet().AddFilter("Environment", filtertypes.OpEqual, "prod")
		result, count, err := repo.GetProvisionings(ctx, l, fs)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, 1, count)
		require.Equal(t, "prod", result[0].Environment)
	})

	t.Run("FilterByOwner", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		p1 := newTestProvisioning(t)
		p1.Owner = "alice@example.com"
		require.NoError(t, repo.StoreProvisioning(ctx, l, p1))

		p2 := newTestProvisioning(t)
		p2.Owner = "bob@example.com"
		require.NoError(t, repo.StoreProvisioning(ctx, l, p2))

		fs := *filters.NewFilterSet().AddFilter("Owner", filtertypes.OpEqual, "bob@example.com")
		result, count, err := repo.GetProvisionings(ctx, l, fs)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, 1, count)
		require.Equal(t, "bob@example.com", result[0].Owner)
	})
}

func TestMemProvisioningsRepoExpired(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	t.Run("NoExpired", func(t *testing.T) {
		repo := NewProvisioningsRepository()
		p := newTestProvisioning(t)
		p.State = provmodels.ProvisioningStateProvisioned
		// No ExpiresAt set.
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		result, err := repo.GetExpiredProvisionings(ctx, l)
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("ExpiredProvisioned", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		past := timeutil.Now().Add(-1 * time.Hour)
		p := newTestProvisioning(t)
		p.State = provmodels.ProvisioningStateProvisioned
		p.ExpiresAt = &past
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		result, err := repo.GetExpiredProvisionings(ctx, l)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, p.ID, result[0].ID)
	})

	t.Run("ExpiredButExcludedState", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		past := timeutil.Now().Add(-1 * time.Hour)
		// Destroyed and destroying are excluded from GC.
		p1 := newTestProvisioning(t)
		p1.State = provmodels.ProvisioningStateDestroyed
		p1.ExpiresAt = &past
		require.NoError(t, repo.StoreProvisioning(ctx, l, p1))

		p2 := newTestProvisioning(t)
		p2.State = provmodels.ProvisioningStateDestroying
		p2.ExpiresAt = &past
		require.NoError(t, repo.StoreProvisioning(ctx, l, p2))

		result, err := repo.GetExpiredProvisionings(ctx, l)
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("FutureExpiry", func(t *testing.T) {
		repo := NewProvisioningsRepository()

		future := timeutil.Now().Add(1 * time.Hour)
		p := newTestProvisioning(t)
		p.State = provmodels.ProvisioningStateProvisioned
		p.ExpiresAt = &future
		require.NoError(t, repo.StoreProvisioning(ctx, l, p))

		result, err := repo.GetExpiredProvisionings(ctx, l)
		require.NoError(t, err)
		require.Empty(t, result)
	})
}

func TestMemProvisioningsRepoStateTransition(t *testing.T) {
	l := logger.DefaultLogger

	t.Run("ValidTransition", func(t *testing.T) {
		p := newTestProvisioning(t)
		p.State = provmodels.ProvisioningStateNew
		p.SetState(provmodels.ProvisioningStateInitializing, l)
		require.Equal(t, provmodels.ProvisioningStateInitializing, p.State)
	})

	t.Run("InvalidTransitionStillApplied", func(t *testing.T) {
		// State transitions are warning-only, not enforced.
		p := newTestProvisioning(t)
		p.State = provmodels.ProvisioningStateNew
		p.SetState(provmodels.ProvisioningStateDestroyed, l)
		require.Equal(t, provmodels.ProvisioningStateDestroyed, p.State)
	})
}

func TestGenerateIdentifier(t *testing.T) {
	t.Run("Length", func(t *testing.T) {
		id, err := provmodels.GenerateIdentifier()
		require.NoError(t, err)
		require.Len(t, id, provmodels.IdentifierLength)
	})

	t.Run("CharacterSet", func(t *testing.T) {
		id, err := provmodels.GenerateIdentifier()
		require.NoError(t, err)
		for _, c := range id {
			require.True(t,
				(c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'),
				"unexpected character %c in identifier %s", c, id,
			)
		}
	})

	t.Run("Uniqueness", func(t *testing.T) {
		seen := make(map[string]bool)
		for i := 0; i < 100; i++ {
			id, err := provmodels.GenerateIdentifier()
			require.NoError(t, err)
			require.False(t, seen[id], "duplicate identifier %s", id)
			seen[id] = true
		}
	})
}
