// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package environments

import (
	"context"
	"fmt"
	"testing"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/environments/memory"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/internal/secrets"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/require"
)

// newPrincipal builds a test principal with the given email and permissions.
func newPrincipal(email string, perms ...string) *pkgauth.Principal {
	permissions := make([]authmodels.Permission, len(perms))
	for i, p := range perms {
		permissions[i] = &authmodels.UserPermission{
			Scope:      "*",
			Permission: p,
		}
	}
	return &pkgauth.Principal{
		User:        &authmodels.User{Email: email},
		Permissions: permissions,
	}
}

// stubSecretResolver is a test double for ISecretResolver.
type stubSecretResolver struct {
	written   map[string]string
	verifyErr error
}

func (r *stubSecretResolver) Resolve(_ context.Context, reference string) (string, error) {
	return "resolved-" + reference, nil
}

func (r *stubSecretResolver) Write(
	_ context.Context, project, secretID, value string,
) (string, error) {
	if r.written == nil {
		r.written = make(map[string]string)
	}
	r.written[secretID] = value
	return fmt.Sprintf("projects/%s/secrets/%s/versions/latest", project, secretID), nil
}

func (r *stubSecretResolver) Verify(_ context.Context, _ string) error {
	return r.verifyErr
}

var _ secrets.ISecretResolver = (*stubSecretResolver)(nil)

func TestService_CreateAndGet(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com", types.PermissionCreate, types.PermissionViewAll)

	env, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{
		Name:        "prod",
		Description: "production",
	})
	require.NoError(t, err)
	require.Equal(t, "prod", env.Name)
	require.Equal(t, "admin@x.com", env.Owner)
	require.Equal(t, "production", env.Description)

	got, err := svc.GetEnvironment(ctx, l, admin, "prod")
	require.NoError(t, err)
	require.Equal(t, "prod", got.Name)
	require.Equal(t, "production", got.Description)
}

func TestService_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})
	admin := newPrincipal("admin@x.com", types.PermissionCreate)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "dup"})
	require.NoError(t, err)

	_, err = svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "dup"})
	require.ErrorIs(t, err, types.ErrEnvironmentAlreadyExists)
}

func TestService_Validation(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})
	admin := newPrincipal("admin@x.com", types.PermissionCreate)

	t.Run("empty name", func(t *testing.T) {
		_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: ""})
		require.ErrorIs(t, err, types.ErrInvalidEnvironmentName)
	})
}

func TestService_OwnerPermissions_ViewAll(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com", types.PermissionCreate, types.PermissionViewAll)
	viewer := newPrincipal("viewer@x.com", types.PermissionViewAll)
	ownViewer := newPrincipal("own@x.com", types.PermissionViewOwn)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	// ViewAll can see it.
	_, err = svc.GetEnvironment(ctx, l, viewer, "env1")
	require.NoError(t, err)

	// ViewOwn cannot see someone else's environment.
	_, err = svc.GetEnvironment(ctx, l, ownViewer, "env1")
	require.ErrorIs(t, err, types.ErrEnvironmentNotFound)
}

func TestService_OwnerPermissions_ViewOwn(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	owner := newPrincipal("alice@x.com",
		types.PermissionCreate, types.PermissionViewOwn)

	_, err := svc.CreateEnvironment(ctx, l, owner, types.InputCreateDTO{Name: "my-env"})
	require.NoError(t, err)

	// Owner with :own can see their environment.
	got, err := svc.GetEnvironment(ctx, l, owner, "my-env")
	require.NoError(t, err)
	require.Equal(t, "my-env", got.Name)
}

func TestService_GetAll_FiltersByOwnership(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	alice := newPrincipal("alice@x.com",
		types.PermissionCreate, types.PermissionViewOwn)
	bob := newPrincipal("bob@x.com",
		types.PermissionCreate, types.PermissionViewOwn)
	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionViewAll)

	_, err := svc.CreateEnvironment(ctx, l, alice, types.InputCreateDTO{Name: "alice-env"})
	require.NoError(t, err)
	_, err = svc.CreateEnvironment(ctx, l, bob, types.InputCreateDTO{Name: "bob-env"})
	require.NoError(t, err)

	// Admin sees all.
	all, err := svc.GetEnvironments(ctx, l, admin)
	require.NoError(t, err)
	require.Len(t, all, 2)

	// Alice sees only her own.
	aliceEnvs, err := svc.GetEnvironments(ctx, l, alice)
	require.NoError(t, err)
	require.Len(t, aliceEnvs, 1)
	require.Equal(t, "alice-env", aliceEnvs[0].Name)

	// Bob sees only his own.
	bobEnvs, err := svc.GetEnvironments(ctx, l, bob)
	require.NoError(t, err)
	require.Len(t, bobEnvs, 1)
	require.Equal(t, "bob-env", bobEnvs[0].Name)
}

func TestService_UpdateOwn(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	owner := newPrincipal("alice@x.com",
		types.PermissionCreate, types.PermissionViewAll,
		types.PermissionUpdateOwn)
	other := newPrincipal("bob@x.com", types.PermissionUpdateOwn)

	_, err := svc.CreateEnvironment(ctx, l, owner, types.InputCreateDTO{
		Name: "upd-env", Description: "original",
	})
	require.NoError(t, err)

	// Owner can update.
	updated, err := svc.UpdateEnvironment(ctx, l, owner, "upd-env", types.InputUpdateDTO{
		Description: "modified",
	})
	require.NoError(t, err)
	require.Equal(t, "modified", updated.Description)
	// Owner must be preserved.
	require.Equal(t, "alice@x.com", updated.Owner)

	// Non-owner with :own cannot update.
	_, err = svc.UpdateEnvironment(ctx, l, other, "upd-env", types.InputUpdateDTO{
		Description: "should-fail",
	})
	require.ErrorIs(t, err, types.ErrEnvironmentNotFound)
}

func TestService_DeleteOwn(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	owner := newPrincipal("alice@x.com",
		types.PermissionCreate, types.PermissionDeleteOwn)
	other := newPrincipal("bob@x.com", types.PermissionDeleteOwn)

	_, err := svc.CreateEnvironment(ctx, l, owner, types.InputCreateDTO{Name: "del-env"})
	require.NoError(t, err)

	// Non-owner cannot delete.
	err = svc.DeleteEnvironment(ctx, l, other, "del-env")
	require.ErrorIs(t, err, types.ErrEnvironmentNotFound)

	// Owner can delete.
	err = svc.DeleteEnvironment(ctx, l, owner, "del-env")
	require.NoError(t, err)
}

func TestService_DeleteAll(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	creator := newPrincipal("alice@x.com", types.PermissionCreate)
	admin := newPrincipal("admin@x.com", types.PermissionDeleteAll)

	_, err := svc.CreateEnvironment(ctx, l, creator, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	// Admin with :all can delete anyone's environment.
	err = svc.DeleteEnvironment(ctx, l, admin, "env1")
	require.NoError(t, err)
}

// -- Variable CRUD tests --

func TestService_CreateVariable_Plaintext(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll, types.PermissionViewAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	v, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key:   "DB_HOST",
		Value: "10.0.0.1",
		Type:  envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)
	require.Equal(t, "DB_HOST", v.Key)
	require.Equal(t, "10.0.0.1", v.Value)
	require.Equal(t, envmodels.VarTypePlaintext, v.Type)
	require.Equal(t, "env1", v.EnvironmentName)
	require.False(t, v.CreatedAt.IsZero())

	// Get it back.
	got, err := svc.GetVariable(ctx, l, admin, "env1", "DB_HOST")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", got.Value)
}

func TestService_CreateVariable_SecretAutoWrite(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	stub := &stubSecretResolver{}
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{
		GCPProject: "my-project",
	})
	svc.RegisterResolver("gcp", stub)

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll, types.PermissionViewAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	// Provide a raw secret value (no prefix) -- should be auto-written.
	v, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key:   "API_KEY",
		Value: "super-secret-value",
		Type:  envmodels.VarTypeSecret,
	})
	require.NoError(t, err)
	require.Equal(t, envmodels.VarTypeSecret, v.Type)
	// Value should be the prefixed reference from the stub.
	require.Equal(t, "gcp:projects/my-project/secrets/env1--API_KEY/versions/latest", v.Value)
	// Stub should have recorded the write.
	require.Equal(t, "super-secret-value", stub.written["env1--API_KEY"])
}

func TestService_CreateVariable_SecretAutoWrite_NoProject(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	// Without GCPProject, raw secret values should fail.
	_, err = svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key:   "API_KEY",
		Value: "raw-secret",
		Type:  envmodels.VarTypeSecret,
	})
	require.ErrorIs(t, err, types.ErrSecretWriteFailed)
}

func TestService_CreateVariable_SecretVerifyReference(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	stub := &stubSecretResolver{}
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})
	svc.RegisterResolver("gcp", stub)

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll, types.PermissionViewAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	// Provide a prefixed reference -- should be verified, not auto-written.
	v, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key:   "API_KEY",
		Value: "gcp:projects/p/secrets/s/versions/1",
		Type:  envmodels.VarTypeSecret,
	})
	require.NoError(t, err)
	// Value preserved as-is (it's a reference).
	require.Equal(t, "gcp:projects/p/secrets/s/versions/1", v.Value)
	// Nothing should have been written.
	require.Empty(t, stub.written)
}

func TestService_CreateVariable_SecretVerifyFails(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	stub := &stubSecretResolver{verifyErr: fmt.Errorf("not readable")}
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})
	svc.RegisterResolver("gcp", stub)

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	_, err = svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key:   "API_KEY",
		Value: "gcp:projects/p/secrets/bad/versions/1",
		Type:  envmodels.VarTypeSecret,
	})
	require.ErrorIs(t, err, types.ErrSecretVerifyFailed)
}

func TestService_UpdateVariable(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll, types.PermissionViewAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	_, err = svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key:   "DB_HOST",
		Value: "10.0.0.1",
		Type:  envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)

	updated, err := svc.UpdateVariable(ctx, l, admin, "env1", "DB_HOST", types.InputUpdateVariableDTO{
		Value: "10.0.0.2",
		Type:  envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2", updated.Value)
	require.Equal(t, "DB_HOST", updated.Key)
}

func TestService_DeleteVariable_OwnershipCheck(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	owner := newPrincipal("alice@x.com",
		types.PermissionCreate, types.PermissionUpdateOwn, types.PermissionViewOwn)
	other := newPrincipal("bob@x.com", types.PermissionUpdateOwn)

	_, err := svc.CreateEnvironment(ctx, l, owner, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	_, err = svc.CreateVariable(ctx, l, owner, "env1", types.InputCreateVariableDTO{
		Key:   "K",
		Value: "V",
		Type:  envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)

	// Non-owner with :own cannot delete.
	err = svc.DeleteVariable(ctx, l, other, "env1", "K")
	require.ErrorIs(t, err, types.ErrEnvironmentNotFound)

	// Owner can delete.
	err = svc.DeleteVariable(ctx, l, owner, "env1", "K")
	require.NoError(t, err)

	// Verify it's gone.
	_, err = svc.GetVariable(ctx, l, owner, "env1", "K")
	require.ErrorIs(t, err, types.ErrVariableNotFound)
}

func TestService_GetVariables(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	alice := newPrincipal("alice@x.com",
		types.PermissionCreate, types.PermissionUpdateOwn, types.PermissionViewOwn)
	bob := newPrincipal("bob@x.com",
		types.PermissionCreate, types.PermissionUpdateOwn, types.PermissionViewOwn)

	_, err := svc.CreateEnvironment(ctx, l, alice, types.InputCreateDTO{Name: "alice-env"})
	require.NoError(t, err)

	_, err = svc.CreateVariable(ctx, l, alice, "alice-env", types.InputCreateVariableDTO{
		Key: "A", Value: "1", Type: envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)
	_, err = svc.CreateVariable(ctx, l, alice, "alice-env", types.InputCreateVariableDTO{
		Key: "B", Value: "2", Type: envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)

	// Owner can list variables.
	vars, err := svc.GetVariables(ctx, l, alice, "alice-env")
	require.NoError(t, err)
	require.Len(t, vars, 2)
	// Memory repo sorts by key.
	require.Equal(t, "A", vars[0].Key)
	require.Equal(t, "B", vars[1].Key)

	// Non-owner with :own cannot list variables.
	_, err = svc.GetVariables(ctx, l, bob, "alice-env")
	require.ErrorIs(t, err, types.ErrEnvironmentNotFound)
}

func TestService_CreateVariable_Validation(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	t.Run("empty key", func(t *testing.T) {
		_, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
			Key:   "",
			Value: "V",
			Type:  envmodels.VarTypePlaintext,
		})
		require.ErrorIs(t, err, types.ErrInvalidVariableKey)
	})

	t.Run("key too long", func(t *testing.T) {
		longKey := make([]byte, types.MaxVariableKeyLen+1)
		for i := range longKey {
			longKey[i] = 'A'
		}
		_, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
			Key:   string(longKey),
			Value: "V",
			Type:  envmodels.VarTypePlaintext,
		})
		require.ErrorIs(t, err, types.ErrInvalidVariableKey)
	})

	t.Run("key at max length", func(t *testing.T) {
		maxKey := make([]byte, types.MaxVariableKeyLen)
		for i := range maxKey {
			maxKey[i] = 'A'
		}
		_, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
			Key:   string(maxKey),
			Value: "V",
			Type:  envmodels.VarTypePlaintext,
		})
		require.NoError(t, err)
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
			Key:   "K",
			Value: "V",
			Type:  "invalid",
		})
		require.ErrorIs(t, err, types.ErrInvalidVariableType)
	})
}

func TestService_CreateVariable_Duplicate(t *testing.T) {
	ctx := context.Background()
	l := logger.DefaultLogger
	svc := NewService(memory.NewEnvironmentsRepository(), types.Options{})

	admin := newPrincipal("admin@x.com",
		types.PermissionCreate, types.PermissionUpdateAll)

	_, err := svc.CreateEnvironment(ctx, l, admin, types.InputCreateDTO{Name: "env1"})
	require.NoError(t, err)

	_, err = svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key: "K", Value: "V", Type: envmodels.VarTypePlaintext,
	})
	require.NoError(t, err)

	_, err = svc.CreateVariable(ctx, l, admin, "env1", types.InputCreateVariableDTO{
		Key: "K", Value: "V2", Type: envmodels.VarTypePlaintext,
	})
	require.ErrorIs(t, err, types.ErrVariableAlreadyExists)
}
