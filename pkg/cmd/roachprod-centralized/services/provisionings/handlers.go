// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/vars"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const failurePersistTimeout = 5 * time.Second

// HandleProvision orchestrates the full provisioning lifecycle:
// extract template snapshot, init, plan, apply, and collect outputs.
// Each step is idempotent so the task can be safely retried.
func (s *Service) HandleProvision(
	ctx context.Context, l *logger.Logger, provisioningID uuid.UUID,
) error {
	prov, err := s.repo.GetProvisioning(ctx, l, provisioningID)
	if err != nil {
		return errors.Wrap(err, "load provisioning")
	}

	// Resolve environment variables (including secrets).
	resolvedEnv, err := s.envService.GetEnvironmentResolved(ctx, l, prov.Environment)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "resolve environment"))
	}

	// Prepare the working directory from the template snapshot.
	workingDir := filepath.Join(s.options.WorkingDirBase, prov.ID.String())
	defer func() {
		if err := os.RemoveAll(workingDir); err != nil {
			l.Warn("unable to remove working directory",
				slog.String("working_dir", workingDir),
				slog.Any("error", err),
			)
		}
	}()
	if err := templates.ExtractSnapshot(prov.TemplateSnapshot, workingDir); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "extract snapshot"))
	}

	// Write backend.tf for state storage.
	statePrefix := "provisioning-" + prov.ID.String()
	backendContent := s.backend.GenerateTF(statePrefix)
	if err := templates.WriteBackendTF(workingDir, backendContent); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "write backend.tf"))
	}

	// Parse template variables from the extracted snapshot.
	parsedVars, err := templates.ParseTemplateVariables(workingDir)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "parse template variables"))
	}

	// Build var maps for tofu execution.
	varMap, envVars, err := vars.BuildVarMaps(vars.BuildVarMapsInput{
		ResolvedEnv:    resolvedEnv,
		UserVars:       prov.Variables,
		TemplateVars:   parsedVars,
		Identifier:     prov.Identifier,
		TemplateType:   prov.TemplateType,
		Environment:    prov.Environment,
		Owner:          prov.Owner,
		BackendEnvVars: s.backend.EnvVars(),
	})
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "build var maps"))
	}

	// Step: Init
	prov.SetState(provmodels.ProvisioningStateInitializing, l)
	prov.LastStep = "init"
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update state to initializing"))
	}
	if err := s.executor.Init(ctx, l, workingDir, envVars); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu init"))
	}

	// Step: Plan
	prov.SetState(provmodels.ProvisioningStatePlanning, l)
	prov.LastStep = "plan"
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update state to planning"))
	}
	_, planJSON, err := s.executor.Plan(ctx, l, workingDir, varMap, envVars)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu plan"))
	}
	prov.PlanOutput = planJSON
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "store plan output"))
	}

	// Step: Apply
	prov.SetState(provmodels.ProvisioningStateProvisioning, l)
	prov.LastStep = "apply"
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update state to provisioning"))
	}

	// Check if plan file still exists (idempotency: re-plan if missing).
	planFile := filepath.Join(workingDir, "plan.tfplan")
	if _, statErr := os.Stat(planFile); os.IsNotExist(statErr) {
		l.Info("plan file missing, re-running plan for idempotency",
			slog.String("provisioning_id", prov.ID.String()),
		)
		if _, planJSON, err = s.executor.Plan(ctx, l, workingDir, varMap, envVars); err != nil {
			return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu re-plan"))
		}
		prov.PlanOutput = planJSON
	}

	if err := s.executor.Apply(ctx, l, workingDir, varMap, envVars); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu apply"))
	}

	// Step: Output
	prov.LastStep = "output"
	prov.UpdatedAt = time.Now().UTC()
	outputs, err := s.executor.Output(ctx, l, workingDir, envVars)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu output"))
	}
	prov.Outputs = outputs

	// Done
	prov.SetState(provmodels.ProvisioningStateProvisioned, l)
	prov.LastStep = "done"
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return errors.Wrap(err, "update state to provisioned")
	}

	l.Info("provisioning completed successfully",
		slog.String("provisioning_id", prov.ID.String()),
		slog.String("identifier", prov.Identifier),
	)
	return nil
}

// HandleDestroy orchestrates the full destroy lifecycle:
// extract template snapshot, init, destroy, and clean up.
func (s *Service) HandleDestroy(
	ctx context.Context, l *logger.Logger, provisioningID uuid.UUID,
) error {
	prov, err := s.repo.GetProvisioning(ctx, l, provisioningID)
	if err != nil {
		return errors.Wrap(err, "load provisioning")
	}

	// Resolve environment variables (same template version as original apply).
	resolvedEnv, err := s.envService.GetEnvironmentResolved(ctx, l, prov.Environment)
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "resolve environment"))
	}

	// Prepare working directory using the same template snapshot.
	workingDir := filepath.Join(s.options.WorkingDirBase, prov.ID.String())
	defer func() {
		if err := os.RemoveAll(workingDir); err != nil {
			l.Warn("unable to remove working directory",
				slog.String("working_dir", workingDir),
				slog.Any("error", err),
			)
		}
	}()
	if err := templates.ExtractSnapshot(prov.TemplateSnapshot, workingDir); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "extract snapshot"))
	}

	// Write backend.tf.
	statePrefix := "provisioning-" + prov.ID.String()
	backendContent := s.backend.GenerateTF(statePrefix)
	if err := templates.WriteBackendTF(workingDir, backendContent); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "write backend.tf"))
	}

	// Parse template variables from extracted snapshot.
	parsedVars, err := templates.ParseTemplateVariables(workingDir)
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "parse template variables"))
	}

	// Build var maps.
	varMap, envVars, err := vars.BuildVarMaps(vars.BuildVarMapsInput{
		ResolvedEnv:    resolvedEnv,
		UserVars:       prov.Variables,
		TemplateVars:   parsedVars,
		Identifier:     prov.Identifier,
		TemplateType:   prov.TemplateType,
		Environment:    prov.Environment,
		Owner:          prov.Owner,
		BackendEnvVars: s.backend.EnvVars(),
	})
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "build var maps"))
	}

	// Step: Init
	prov.LastStep = "init"
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "update last_step to init"))
	}
	if err := s.executor.Init(ctx, l, workingDir, envVars); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "tofu init"))
	}

	// Step: Destroy
	prov.LastStep = "destroy"
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "update last_step to destroy"))
	}
	if err := s.executor.Destroy(ctx, l, workingDir, varMap, envVars); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "tofu destroy"))
	}

	// Best-effort backend state cleanup.
	if err := s.backend.CleanupState(ctx, l, statePrefix); err != nil {
		l.Warn("failed to clean up backend state",
			slog.String("prefix", statePrefix), slog.Any("error", err))
	}

	// Done
	prov.SetState(provmodels.ProvisioningStateDestroyed, l)
	prov.LastStep = "done"
	prov.PlanOutput = nil
	prov.Outputs = make(map[string]interface{}) // stores as {} in JSONB
	prov.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return errors.Wrap(err, "update state to destroyed")
	}

	l.Info("provisioning destroyed successfully",
		slog.String("provisioning_id", prov.ID.String()),
		slog.String("identifier", prov.Identifier),
	)
	return nil
}

// failProvision marks the provisioning as failed and persists the error.
func (s *Service) failProvision(
	ctx context.Context, l *logger.Logger, prov *provmodels.Provisioning, provErr error,
) error {
	prov.Error = provErr.Error()
	prov.SetState(provmodels.ProvisioningStateFailed, l)
	prov.UpdatedAt = time.Now().UTC()
	repoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failurePersistTimeout)
	defer cancel()
	if updateErr := s.repo.UpdateProvisioning(repoCtx, l, *prov); updateErr != nil {
		l.Error("failed to update provisioning state to failed",
			slog.String("provisioning_id", prov.ID.String()),
			slog.Any("update_error", updateErr),
			slog.Any("original_error", provErr),
		)
	}
	return provErr
}

// failDestroy marks the provisioning as destroy_failed and persists the error.
func (s *Service) failDestroy(
	ctx context.Context, l *logger.Logger, prov *provmodels.Provisioning, provErr error,
) error {
	prov.Error = provErr.Error()
	prov.SetState(provmodels.ProvisioningStateDestroyFailed, l)
	prov.UpdatedAt = time.Now().UTC()
	repoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failurePersistTimeout)
	defer cancel()
	if updateErr := s.repo.UpdateProvisioning(repoCtx, l, *prov); updateErr != nil {
		l.Error("failed to update provisioning state to destroy_failed",
			slog.String("provisioning_id", prov.ID.String()),
			slog.Any("update_error", updateErr),
			slog.Any("original_error", provErr),
		)
	}
	return provErr
}
