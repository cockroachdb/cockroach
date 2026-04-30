// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memory

import (
	"context"
	"testing"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/require"
)

func TestMemEnvironmentsRepo(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	t.Run("StoreAndGet", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		env := envmodels.Environment{
			Name:        "test-env",
			Description: "A test environment",
			Owner:       "alice@example.com",
		}
		err := repo.StoreEnvironment(ctx, l, env)
		require.NoError(t, err)

		got, err := repo.GetEnvironment(ctx, l, "test-env")
		require.NoError(t, err)
		require.Equal(t, "test-env", got.Name)
		require.Equal(t, "A test environment", got.Description)
		require.Equal(t, "alice@example.com", got.Owner)
		require.False(t, got.CreatedAt.IsZero())
		require.False(t, got.UpdatedAt.IsZero())
	})

	t.Run("StoreReturnsDeepCopy", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		env := envmodels.Environment{
			Name:        "copy-env",
			Owner:       "bob@example.com",
			Description: "original",
		}
		err := repo.StoreEnvironment(ctx, l, env)
		require.NoError(t, err)

		// Mutate the original struct — stored copy must not be affected.
		env.Description = "MUTATED"
		got, err := repo.GetEnvironment(ctx, l, "copy-env")
		require.NoError(t, err)
		require.Equal(t, "original", got.Description)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		_, err := repo.GetEnvironment(ctx, l, "nope")
		require.ErrorIs(t, err, environments.ErrEnvironmentNotFound)
	})

	t.Run("StoreDuplicate", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		env := envmodels.Environment{Name: "dup", Owner: "u@x.com"}
		require.NoError(t, repo.StoreEnvironment(ctx, l, env))
		err := repo.StoreEnvironment(ctx, l, env)
		require.ErrorIs(t, err, environments.ErrEnvironmentAlreadyExists)
	})

	t.Run("GetAllEnvironments", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		require.NoError(t, repo.StoreEnvironment(ctx, l, envmodels.Environment{
			Name: "env-a", Owner: "alice@x.com",
		}))
		require.NoError(t, repo.StoreEnvironment(ctx, l, envmodels.Environment{
			Name: "env-b", Owner: "bob@x.com",
		}))

		all, err := repo.GetEnvironments(ctx, l)
		require.NoError(t, err)
		require.Len(t, all, 2)
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		require.NoError(t, repo.StoreEnvironment(ctx, l, envmodels.Environment{
			Name:        "upd-env",
			Description: "original",
			Owner:       "alice@x.com",
		}))

		err := repo.UpdateEnvironment(ctx, l, envmodels.Environment{
			Name:        "upd-env",
			Description: "updated",
		})
		require.NoError(t, err)

		got, err := repo.GetEnvironment(ctx, l, "upd-env")
		require.NoError(t, err)
		require.Equal(t, "updated", got.Description)
		// Owner must be preserved across updates.
		require.Equal(t, "alice@x.com", got.Owner)
		// CreatedAt must be preserved.
		require.True(t, got.UpdatedAt.After(got.CreatedAt) || got.UpdatedAt.Equal(got.CreatedAt))
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		err := repo.UpdateEnvironment(ctx, l, envmodels.Environment{Name: "nope"})
		require.ErrorIs(t, err, environments.ErrEnvironmentNotFound)
	})

	t.Run("Delete", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		require.NoError(t, repo.StoreEnvironment(ctx, l, envmodels.Environment{
			Name: "del-env", Owner: "u@x.com",
		}))

		err := repo.DeleteEnvironment(ctx, l, "del-env")
		require.NoError(t, err)

		_, err = repo.GetEnvironment(ctx, l, "del-env")
		require.ErrorIs(t, err, environments.ErrEnvironmentNotFound)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		err := repo.DeleteEnvironment(ctx, l, "nope")
		require.ErrorIs(t, err, environments.ErrEnvironmentNotFound)
	})
}

func TestMemEnvironmentsRepoVariables(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger

	// storeEnv is a helper that creates an environment in the repository.
	storeEnv := func(t *testing.T, repo *MemEnvironmentsRepo, name string) {
		t.Helper()
		require.NoError(t, repo.StoreEnvironment(ctx, l, envmodels.Environment{
			Name: name, Owner: "owner@x.com",
		}))
	}

	t.Run("StoreAndGetVariable", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		storeEnv(t, repo, "env1")

		v := envmodels.EnvironmentVariable{
			EnvironmentName: "env1",
			Key:             "DB_HOST",
			Value:           "localhost",
			Type:            envmodels.VarTypePlaintext,
		}
		require.NoError(t, repo.StoreVariable(ctx, l, v))

		got, err := repo.GetVariable(ctx, l, "env1", "DB_HOST")
		require.NoError(t, err)
		require.Equal(t, "env1", got.EnvironmentName)
		require.Equal(t, "DB_HOST", got.Key)
		require.Equal(t, "localhost", got.Value)
		require.Equal(t, envmodels.VarTypePlaintext, got.Type)
		require.False(t, got.CreatedAt.IsZero())
		require.False(t, got.UpdatedAt.IsZero())
	})

	t.Run("GetVariables", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		storeEnv(t, repo, "env1")

		require.NoError(t, repo.StoreVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "env1", Key: "B_KEY", Value: "b", Type: envmodels.VarTypePlaintext,
		}))
		require.NoError(t, repo.StoreVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "env1", Key: "A_KEY", Value: "a", Type: envmodels.VarTypeSecret,
		}))

		vars, err := repo.GetVariables(ctx, l, "env1")
		require.NoError(t, err)
		require.Len(t, vars, 2)
		// Should be sorted by key.
		require.Equal(t, "A_KEY", vars[0].Key)
		require.Equal(t, "B_KEY", vars[1].Key)
	})

	t.Run("GetVariablesEmpty", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		vars, err := repo.GetVariables(ctx, l, "no-such-env")
		require.NoError(t, err)
		require.Empty(t, vars)
	})

	t.Run("StoreVariableDuplicate", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		storeEnv(t, repo, "env1")

		v := envmodels.EnvironmentVariable{
			EnvironmentName: "env1", Key: "DUP", Value: "v1", Type: envmodels.VarTypePlaintext,
		}
		require.NoError(t, repo.StoreVariable(ctx, l, v))
		err := repo.StoreVariable(ctx, l, v)
		require.ErrorIs(t, err, environments.ErrVariableAlreadyExists)
	})

	t.Run("StoreVariableNoParentEnv", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		v := envmodels.EnvironmentVariable{
			EnvironmentName: "nonexistent", Key: "K", Value: "V", Type: envmodels.VarTypePlaintext,
		}
		err := repo.StoreVariable(ctx, l, v)
		require.ErrorIs(t, err, environments.ErrEnvironmentNotFound)
	})

	t.Run("GetVariableNotFound", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		_, err := repo.GetVariable(ctx, l, "no-env", "no-key")
		require.ErrorIs(t, err, environments.ErrVariableNotFound)
	})

	t.Run("UpdateVariable", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		storeEnv(t, repo, "env1")

		require.NoError(t, repo.StoreVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "env1", Key: "K", Value: "old", Type: envmodels.VarTypePlaintext,
		}))

		err := repo.UpdateVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "env1", Key: "K", Value: "new", Type: envmodels.VarTypeSecret,
		})
		require.NoError(t, err)

		got, err := repo.GetVariable(ctx, l, "env1", "K")
		require.NoError(t, err)
		require.Equal(t, "new", got.Value)
		require.Equal(t, envmodels.VarTypeSecret, got.Type)
	})

	t.Run("UpdateVariableNotFound", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		err := repo.UpdateVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "no-env", Key: "K", Value: "v", Type: envmodels.VarTypePlaintext,
		})
		require.ErrorIs(t, err, environments.ErrVariableNotFound)
	})

	t.Run("DeleteVariable", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		storeEnv(t, repo, "env1")

		require.NoError(t, repo.StoreVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "env1", Key: "DEL", Value: "v", Type: envmodels.VarTypePlaintext,
		}))

		require.NoError(t, repo.DeleteVariable(ctx, l, "env1", "DEL"))

		_, err := repo.GetVariable(ctx, l, "env1", "DEL")
		require.ErrorIs(t, err, environments.ErrVariableNotFound)
	})

	t.Run("DeleteVariableNotFound", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		err := repo.DeleteVariable(ctx, l, "no-env", "no-key")
		require.ErrorIs(t, err, environments.ErrVariableNotFound)
	})

	t.Run("DeleteEnvironmentCascadesVariables", func(t *testing.T) {
		repo := NewEnvironmentsRepository()
		storeEnv(t, repo, "cascade-env")

		require.NoError(t, repo.StoreVariable(ctx, l, envmodels.EnvironmentVariable{
			EnvironmentName: "cascade-env", Key: "K1", Value: "v1", Type: envmodels.VarTypePlaintext,
		}))

		require.NoError(t, repo.DeleteEnvironment(ctx, l, "cascade-env"))

		vars, err := repo.GetVariables(ctx, l, "cascade-env")
		require.NoError(t, err)
		require.Empty(t, vars)
	})
}
