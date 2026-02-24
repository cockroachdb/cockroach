// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package environments

import (
	"context"
	"log/slog"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/internal/secrets"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// Service implements the environments service. It provides CRUD operations on
// environments and their variables, and resolves secret-type variables via a
// prefix-based resolver registry.
//
// Secret variables store a provider-prefixed reference (e.g.
// "gcp:projects/p/secrets/s/versions/1"). During resolution the prefix selects
// the appropriate resolver. References without a recognized prefix are treated
// as literal values.
type Service struct {
	repo            environments.IEnvironmentsRepository
	secretsRegistry *secrets.Registry
}

// NewService creates a new environments service with an empty secrets
// registry. Use RegisterResolver or RegisterGCPResolver to add secret
// providers before processing secret-type variables.
func NewService(repo environments.IEnvironmentsRepository) *Service {
	return &Service{
		repo:            repo,
		secretsRegistry: secrets.NewRegistry(),
	}
}

// RegisterGCPResolver creates a GCP Secret Manager resolver configured for
// the given project and prefix, and registers it under the "gcp" prefix.
// After this call, secret references like
// "gcp:projects/p/secrets/s/versions/latest" will be resolved via GCP,
// and raw secret values will be auto-written to this project.
func (s *Service) RegisterGCPResolver(
	ctx context.Context, project string, secretPrefix string,
) error {
	resolver, err := secrets.NewGCPResolver(ctx, project, secretPrefix)
	if err != nil {
		return errors.Wrap(err, "create GCP secret resolver")
	}
	s.secretsRegistry.Register("gcp", resolver)
	return nil
}

// RegisterResolver registers a secret resolver under the given prefix. This
// is useful for tests and for production wiring of non-GCP resolvers.
func (s *Service) RegisterResolver(prefix string, resolver secrets.ISecretResolver) {
	s.secretsRegistry.Register(prefix, resolver)
}

// -- IService lifecycle methods (no-op for environments) --

// RegisterTasks is a no-op; environments have no background tasks.
func (s *Service) RegisterTasks(_ context.Context) error { return nil }

// StartService is a no-op.
func (s *Service) StartService(_ context.Context, _ *logger.Logger) error { return nil }

// StartBackgroundWork is a no-op.
func (s *Service) StartBackgroundWork(_ context.Context, _ *logger.Logger, _ chan<- error) error {
	return nil
}

// Shutdown releases resources held by the secrets registry (e.g. gRPC
// connections to GCP Secret Manager).
func (s *Service) Shutdown(_ context.Context) error {
	return s.secretsRegistry.Close()
}

// -- Environment CRUD operations --

// GetEnvironment returns an environment by name. Returns
// ErrEnvironmentNotFound if the environment doesn't exist or if the
// principal lacks permission (to avoid leaking existence).
func (s *Service) GetEnvironment(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, name string,
) (envmodels.Environment, error) {
	env, err := s.repo.GetEnvironment(ctx, l, name)
	if err != nil {
		return envmodels.Environment{}, mapRepoError(err)
	}
	if !checkAccess(principal, env, types.PermissionView) {
		return envmodels.Environment{}, types.ErrEnvironmentNotFound
	}
	return env, nil
}

// GetEnvironments returns all environments visible to the principal.
// Principals with :all see everything; principals with :own see only
// their own environments.
func (s *Service) GetEnvironments(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal,
) ([]envmodels.Environment, error) {
	envs, err := s.repo.GetEnvironments(ctx, l)
	if err != nil {
		return nil, mapRepoError(err)
	}

	viewAll := principal.HasPermission(types.PermissionViewAll)
	var result []envmodels.Environment
	for _, env := range envs {
		if viewAll || (isOwner(principal, env) &&
			principal.HasPermission(types.PermissionViewOwn)) {
			result = append(result, env)
		}
	}
	return result, nil
}

// CreateEnvironment persists a new environment after validation. The
// principal's identity is recorded as the owner.
func (s *Service) CreateEnvironment(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, input types.InputCreateDTO,
) (envmodels.Environment, error) {
	if err := validateName(input.Name); err != nil {
		return envmodels.Environment{}, err
	}

	env := envmodels.Environment{
		Name:        input.Name,
		Description: input.Description,
		Owner:       principalIdentity(principal),
	}

	if err := s.repo.StoreEnvironment(ctx, l, env); err != nil {
		return envmodels.Environment{}, mapRepoError(err)
	}

	// Re-read to get timestamps set by the DB.
	stored, err := s.repo.GetEnvironment(ctx, l, env.Name)
	if err != nil {
		return envmodels.Environment{}, mapRepoError(err)
	}
	return stored, nil
}

// UpdateEnvironment updates an existing environment. Requires :all or
// :own (if the principal is the owner).
func (s *Service) UpdateEnvironment(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	name string,
	input types.InputUpdateDTO,
) (envmodels.Environment, error) {
	// Fetch existing to check ownership.
	existing, err := s.repo.GetEnvironment(ctx, l, name)
	if err != nil {
		return envmodels.Environment{}, mapRepoError(err)
	}
	if !checkAccess(principal, existing, types.PermissionUpdate) {
		return envmodels.Environment{}, types.ErrEnvironmentNotFound
	}

	env := envmodels.Environment{
		Name:        name,
		Description: input.Description,
	}

	if err := s.repo.UpdateEnvironment(ctx, l, env); err != nil {
		return envmodels.Environment{}, mapRepoError(err)
	}

	stored, err := s.repo.GetEnvironment(ctx, l, env.Name)
	if err != nil {
		return envmodels.Environment{}, mapRepoError(err)
	}
	return stored, nil
}

// DeleteEnvironment removes an environment. Requires :all or :own.
func (s *Service) DeleteEnvironment(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, name string,
) error {
	existing, err := s.repo.GetEnvironment(ctx, l, name)
	if err != nil {
		return mapRepoError(err)
	}
	if !checkAccess(principal, existing, types.PermissionDelete) {
		return types.ErrEnvironmentNotFound
	}
	return mapRepoError(s.repo.DeleteEnvironment(ctx, l, name))
}

// -- Variable CRUD operations --

// GetVariable returns a single variable for the given environment. The
// principal must have view access to the parent environment.
func (s *Service) GetVariable(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, envName, key string,
) (envmodels.EnvironmentVariable, error) {
	env, err := s.repo.GetEnvironment(ctx, l, envName)
	if err != nil {
		return envmodels.EnvironmentVariable{}, mapRepoError(err)
	}
	if !checkAccess(principal, env, types.PermissionView) {
		return envmodels.EnvironmentVariable{}, types.ErrEnvironmentNotFound
	}

	v, err := s.repo.GetVariable(ctx, l, envName, key)
	if err != nil {
		return envmodels.EnvironmentVariable{}, mapVariableRepoError(err)
	}
	return v, nil
}

// GetVariables returns all variables for the given environment. The
// principal must have view access to the parent environment.
func (s *Service) GetVariables(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, envName string,
) ([]envmodels.EnvironmentVariable, error) {
	env, err := s.repo.GetEnvironment(ctx, l, envName)
	if err != nil {
		return nil, mapRepoError(err)
	}
	if !checkAccess(principal, env, types.PermissionView) {
		return nil, types.ErrEnvironmentNotFound
	}

	vars, err := s.repo.GetVariables(ctx, l, envName)
	if err != nil {
		return nil, mapVariableRepoError(err)
	}
	return vars, nil
}

// CreateVariable creates a new variable on an environment. For secret-type
// variables, if the value matches a registered resolver prefix it is verified;
// otherwise the raw value is auto-written to Secret Manager using the
// configured GCP project.
func (s *Service) CreateVariable(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	envName string,
	input types.InputCreateVariableDTO,
) (envmodels.EnvironmentVariable, error) {
	if err := validateVariable(input.Key, input.Type); err != nil {
		return envmodels.EnvironmentVariable{}, err
	}

	env, err := s.repo.GetEnvironment(ctx, l, envName)
	if err != nil {
		return envmodels.EnvironmentVariable{}, mapRepoError(err)
	}
	if !checkAccess(principal, env, types.PermissionUpdate) {
		return envmodels.EnvironmentVariable{}, types.ErrEnvironmentNotFound
	}

	value := input.Value
	if isSecretType(input.Type) {
		value, err = s.processSecretValue(ctx, envName, input.Key, value)
		if err != nil {
			return envmodels.EnvironmentVariable{}, err
		}
	}

	variable := envmodels.EnvironmentVariable{
		EnvironmentName: envName,
		Key:             input.Key,
		Value:           value,
		Type:            input.Type,
	}
	if err := s.repo.StoreVariable(ctx, l, variable); err != nil {
		return envmodels.EnvironmentVariable{}, mapVariableRepoError(err)
	}

	stored, err := s.repo.GetVariable(ctx, l, envName, input.Key)
	if err != nil {
		return envmodels.EnvironmentVariable{}, mapVariableRepoError(err)
	}
	return stored, nil
}

// UpdateVariable updates an existing variable. For secret-type variables
// (secret or template_secret), if the value matches a registered resolver
// prefix it is verified; otherwise the raw value is auto-written to Secret
// Manager.
func (s *Service) UpdateVariable(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	envName, key string,
	input types.InputUpdateVariableDTO,
) (envmodels.EnvironmentVariable, error) {
	if err := validateVariable(key, input.Type); err != nil {
		return envmodels.EnvironmentVariable{}, err
	}

	env, err := s.repo.GetEnvironment(ctx, l, envName)
	if err != nil {
		return envmodels.EnvironmentVariable{}, mapRepoError(err)
	}
	if !checkAccess(principal, env, types.PermissionUpdate) {
		return envmodels.EnvironmentVariable{}, types.ErrEnvironmentNotFound
	}

	value := input.Value
	if isSecretType(input.Type) {
		value, err = s.processSecretValue(ctx, envName, key, value)
		if err != nil {
			return envmodels.EnvironmentVariable{}, err
		}
	}

	variable := envmodels.EnvironmentVariable{
		EnvironmentName: envName,
		Key:             key,
		Value:           value,
		Type:            input.Type,
	}
	if err := s.repo.UpdateVariable(ctx, l, variable); err != nil {
		return envmodels.EnvironmentVariable{}, mapVariableRepoError(err)
	}

	stored, err := s.repo.GetVariable(ctx, l, envName, key)
	if err != nil {
		return envmodels.EnvironmentVariable{}, mapVariableRepoError(err)
	}
	return stored, nil
}

// DeleteVariable removes a variable from an environment. The principal must
// have update access to the parent environment.
func (s *Service) DeleteVariable(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, envName, key string,
) error {
	env, err := s.repo.GetEnvironment(ctx, l, envName)
	if err != nil {
		return mapRepoError(err)
	}
	if !checkAccess(principal, env, types.PermissionUpdate) {
		return types.ErrEnvironmentNotFound
	}
	return mapVariableRepoError(s.repo.DeleteVariable(ctx, l, envName, key))
}

// -- Internal method for provisioning task handler --

// GetEnvironmentResolved returns the environment with all secret values
// resolved. Plaintext variables are returned as-is. Secret variables are
// dispatched to the resolver matching their provider prefix (e.g. "gcp:...").
// References without a recognized prefix are returned as literal values.
//
// This method performs no authorization check -- it is only called
// internally by the provisioning task handler.
func (s *Service) GetEnvironmentResolved(
	ctx context.Context, l *logger.Logger, name string,
) (types.ResolvedEnvironment, error) {
	if _, err := s.repo.GetEnvironment(ctx, l, name); err != nil {
		return types.ResolvedEnvironment{}, mapRepoError(err)
	}

	vars, err := s.repo.GetVariables(ctx, l, name)
	if err != nil {
		return types.ResolvedEnvironment{}, mapVariableRepoError(err)
	}

	resolved := types.ResolvedEnvironment{
		Name:      name,
		Variables: make([]types.ResolvedVariable, 0, len(vars)),
	}

	for _, v := range vars {
		rv := types.ResolvedVariable{
			Key:  v.Key,
			Type: v.Type,
		}

		if isSecretType(v.Type) {
			val, err := s.secretsRegistry.Resolve(ctx, v.Value)
			if err != nil {
				l.Error("failed to resolve secret",
					slog.String("key", v.Key),
					slog.Any("error", err))
				return types.ResolvedEnvironment{}, errors.Wrapf(err, "resolve secret %s", v.Key)
			}
			rv.Value = val
		} else {
			rv.Value = v.Value
		}
		resolved.Variables = append(resolved.Variables, rv)
	}
	return resolved, nil
}

// -- Secret value processing --

// processSecretValue handles a secret variable's value. If the value matches
// a registered resolver prefix (e.g. "gcp:..."), it is verified against the
// secret manager. Otherwise it is treated as a raw value and auto-written
// using the "gcp" resolver (which owns its project and prefix configuration).
func (s *Service) processSecretValue(
	ctx context.Context, envName, key, value string,
) (string, error) {
	if s.secretsRegistry.HasResolver(value) {
		// Value is already a prefixed reference -- verify it.
		if err := s.secretsRegistry.Verify(ctx, value); err != nil {
			return "", types.ErrSecretVerifyFailed
		}
		return value, nil
	}

	// Raw value -- auto-write via the "gcp" resolver.
	if !s.secretsRegistry.CanWrite("gcp") {
		return "", types.ErrSecretWriteFailed
	}
	secretID := envName + "--" + key
	ref, err := s.secretsRegistry.Write(ctx, "gcp", secretID, value)
	if err != nil {
		return "", types.ErrSecretWriteFailed
	}
	return ref, nil
}

// -- Authorization helpers --

// principalIdentity returns the identity string for the principal. For user
// principals it returns the email; for service accounts it returns the
// delegated user email if present, otherwise the service account name.
func principalIdentity(p *pkgauth.Principal) string {
	if p.User != nil {
		return p.User.Email
	}
	if p.ServiceAccount != nil {
		if p.DelegatedFrom != nil && p.DelegatedFromEmail != "" {
			return p.DelegatedFromEmail
		}
		return p.ServiceAccount.Name
	}
	return ""
}

// isOwner returns true if the principal's identity matches the environment
// owner.
func isOwner(p *pkgauth.Principal, env envmodels.Environment) bool {
	identity := principalIdentity(p)
	return identity != "" && identity == env.Owner
}

// checkAccess returns true if the principal has the ":all" variant of the
// given permission, or the ":own" variant and is the environment owner.
func checkAccess(p *pkgauth.Principal, env envmodels.Environment, basePermission string) bool {
	if p.HasPermission(basePermission + ":all") {
		return true
	}
	if isOwner(p, env) && p.HasPermission(basePermission+":own") {
		return true
	}
	return false
}

// -- Validation helpers --

// validateName checks that the environment name is non-empty, within length
// limits, and contains only allowed characters ([a-zA-Z0-9_-]).
func validateName(name string) error {
	if !types.ValidNameRe.MatchString(name) {
		return types.ErrInvalidEnvironmentName
	}
	return nil
}

// validateVariable checks that a variable key matches the allowed pattern
// ([a-zA-Z0-9_-], 1–190 chars) and the type is valid.
func validateVariable(key string, varType envmodels.EnvironmentVarType) error {
	if !types.ValidVariableKeyRe.MatchString(key) {
		return types.ErrInvalidVariableKey
	}
	switch varType {
	case envmodels.VarTypePlaintext, envmodels.VarTypeSecret, envmodels.VarTypeTemplateSecret:
		return nil
	default:
		return types.ErrInvalidVariableType
	}
}

// isSecretType reports whether the variable type requires secret manager
// resolution (either secret or template_secret).
func isSecretType(t envmodels.EnvironmentVarType) bool {
	return t == envmodels.VarTypeSecret || t == envmodels.VarTypeTemplateSecret
}

// -- Error mapping --

// mapRepoError translates repository sentinel errors to service-level
// public errors.
func mapRepoError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, environments.ErrEnvironmentNotFound):
		return types.ErrEnvironmentNotFound
	case errors.Is(err, environments.ErrEnvironmentAlreadyExists):
		return types.ErrEnvironmentAlreadyExists
	case errors.Is(err, environments.ErrEnvironmentHasProvisionings):
		return types.ErrEnvironmentHasProvisionings
	default:
		return err
	}
}

// mapVariableRepoError translates variable repository sentinel errors to
// service-level public errors.
func mapVariableRepoError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, environments.ErrVariableNotFound):
		return types.ErrVariableNotFound
	case errors.Is(err, environments.ErrVariableAlreadyExists):
		return types.ErrVariableAlreadyExists
	case errors.Is(err, environments.ErrEnvironmentNotFound):
		return types.ErrEnvironmentNotFound
	default:
		return err
	}
}
