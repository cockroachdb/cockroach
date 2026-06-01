// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/vars"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

const failurePersistTimeout = 5 * time.Second

// HandleProvision orchestrates the full provisioning lifecycle:
// extract template snapshot, init, plan, apply, and collect outputs.
// Each step is idempotent so the task can be safely retried.
func (s *Service) HandleProvision(
	ctx context.Context, l *logger.Logger, provisioningID uuid.UUID,
) error {
	prov, err := s.repo.GetProvisioningForExecution(ctx, l, provisioningID)
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
	archiveReader, err := s.openTemplateArchive(ctx, prov)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "open template snapshot"))
	}
	defer archiveReader.Close()
	if err := templates.ExtractSnapshotFromReader(archiveReader, workingDir); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "extract snapshot"))
	}
	// Release snapshot bytes now that they are extracted to disk. Safe
	// because UpdateProvisioningProgress does not write this field.
	prov.TemplateSnapshot = nil

	// Write backend.tf for state storage.
	statePrefix := "provisionings/" + prov.ID.String() + "/state"
	backendContent, err := s.backend.GenerateTF(ctx, statePrefix)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "generate backend.tf"))
	}
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
		ResolvedEnv:  resolvedEnv,
		UserVars:     prov.Variables,
		TemplateVars: parsedVars,
		Identifier:   prov.Identifier,
		TemplateType: prov.TemplateType,
		Environment:  prov.Environment,
		Owner:        prov.Owner,
	})
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "build var maps"))
	}

	// Inject auto-computed variables for roachprod cluster templates.
	s.injectAutoVars(l, &prov, parsedVars, varMap, envVars)

	// Write secret_file variables to disk and replace env var values with
	// file paths. Required for env vars like GOOGLE_APPLICATION_CREDENTIALS
	// that expect a file path, not the credential content.
	envVars, cleanupCreds, err := vars.PrepareCredentialFiles(workingDir, envVars, resolvedEnv)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "prepare credential files"))
	}
	defer func() {
		if cleanErr := cleanupCreds(); cleanErr != nil {
			l.Warn("failed to clean up credential files",
				slog.String("working_dir", workingDir),
				slog.Any("error", cleanErr),
			)
		}
	}()

	// Apply backend-specific env overrides AFTER all user/environment
	// variables to prevent user override of backend auth credentials.
	for k, v := range s.backend.EnvOverrides() {
		envVars[k] = v
	}

	// Step: Init
	prov.SetState(provmodels.ProvisioningStateInitializing, l)
	prov.LastStep = "init"
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update state to initializing"))
	}
	if err := s.executor.Init(ctx, l, workingDir, envVars); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu init"))
	}

	// Step: Plan
	prov.SetState(provmodels.ProvisioningStatePlanning, l)
	prov.LastStep = "plan"
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update state to planning"))
	}
	currentPlanRef := prov.PlanOutputRef
	storePlanData := func(oldPlanRef string) (string, error) {
		if s.usesExternalArtifacts() {
			newPlanRef, err := s.storePlanArtifact(ctx, l, prov.ID, workingDir, envVars)
			if err != nil {
				return oldPlanRef, err
			}
			if err := s.repo.StorePlanData(ctx, l, prov.ID, nil, newPlanRef); err != nil {
				if cleanupErr := s.deleteArtifactIfPresent(context.WithoutCancel(ctx), newPlanRef); cleanupErr != nil {
					l.Warn("failed to clean up uploaded plan artifact after repository error",
						slog.String("provisioning_id", prov.ID.String()),
						slog.String("plan_output_ref", newPlanRef),
						slog.Any("error", cleanupErr),
					)
				}
				return oldPlanRef, err
			}
			if oldPlanRef != "" && oldPlanRef != newPlanRef && s.artifactStore != nil {
				if err := s.deleteArtifactIfPresent(context.WithoutCancel(ctx), oldPlanRef); err != nil {
					l.Warn("failed to delete replaced plan artifact",
						slog.String("provisioning_id", prov.ID.String()),
						slog.String("plan_output_ref", oldPlanRef),
						slog.Any("error", err),
					)
				}
			}
			return newPlanRef, nil
		}

		var planJSON bytes.Buffer
		if err := s.executor.WritePlanJSON(ctx, l, workingDir, envVars, &planJSON); err != nil {
			return oldPlanRef, err
		}
		if err := s.repo.StorePlanData(
			ctx, l, prov.ID, json.RawMessage(bytes.Clone(planJSON.Bytes())), "",
		); err != nil {
			return oldPlanRef, err
		}
		if oldPlanRef != "" && s.artifactStore != nil {
			if err := s.deleteArtifactIfPresent(context.WithoutCancel(ctx), oldPlanRef); err != nil {
				l.Warn("failed to delete replaced plan artifact after inline plan write",
					slog.String("provisioning_id", prov.ID.String()),
					slog.String("plan_output_ref", oldPlanRef),
					slog.Any("error", err),
				)
			}
		}
		return "", nil
	}
	_, err = s.executor.Plan(ctx, l, workingDir, varMap, envVars)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu plan"))
	}
	currentPlanRef, err = storePlanData(currentPlanRef)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "store plan output"))
	}
	prov.PlanOutputRef = currentPlanRef

	// Step: Apply
	prov.SetState(provmodels.ProvisioningStateProvisioning, l)
	prov.LastStep = "apply"
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update state to provisioning"))
	}

	// Check if plan file still exists (idempotency: re-plan if missing).
	planFile := filepath.Join(workingDir, "plan.tfplan")
	if _, statErr := os.Stat(planFile); oserror.IsNotExist(statErr) {
		l.Info("plan file missing, re-running plan for idempotency",
			slog.String("provisioning_id", prov.ID.String()),
		)
		if _, err = s.executor.Plan(ctx, l, workingDir, varMap, envVars); err != nil {
			return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu re-plan"))
		}
		currentPlanRef, err = storePlanData(currentPlanRef)
		if err != nil {
			return s.failProvision(ctx, l, &prov, errors.Wrap(err, "store re-plan output"))
		}
		prov.PlanOutputRef = currentPlanRef
	}

	if err := s.executor.Apply(ctx, l, workingDir, varMap, envVars); err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu apply"))
	}

	// Step: Output
	prov.LastStep = "output"
	prov.UpdatedAt = timeutil.Now().UTC()
	outputs, err := s.executor.Output(ctx, l, workingDir, envVars)
	if err != nil {
		return s.failProvision(ctx, l, &prov, errors.Wrap(err, "tofu output"))
	}
	prov.Outputs = outputs

	// Step: Post-apply hooks
	if s.hookOrchestrator != nil {
		prov.LastStep = "hooks"
		prov.UpdatedAt = timeutil.Now().UTC()
		if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
			return s.failProvision(ctx, l, &prov, errors.Wrap(err, "update last_step to hooks"))
		}
		if err := s.hookOrchestrator.RunPostApply(ctx, l, prov, workingDir, resolvedEnv); err != nil {
			return s.failProvision(ctx, l, &prov, errors.Wrap(err, "post-apply hooks"))
		}
	}

	// Done
	prov.SetState(provmodels.ProvisioningStateProvisioned, l)
	prov.LastStep = "done"
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
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
	prov, err := s.repo.GetProvisioningForExecution(ctx, l, provisioningID)
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
	archiveReader, err := s.openTemplateArchive(ctx, prov)
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "open template snapshot"))
	}
	defer archiveReader.Close()
	if err := templates.ExtractSnapshotFromReader(archiveReader, workingDir); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "extract snapshot"))
	}
	prov.TemplateSnapshot = nil

	// Write backend.tf.
	statePrefix := "provisionings/" + prov.ID.String() + "/state"
	backendContent, err := s.backend.GenerateTF(ctx, statePrefix)
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "generate backend.tf"))
	}
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
		ResolvedEnv:  resolvedEnv,
		UserVars:     prov.Variables,
		TemplateVars: parsedVars,
		Identifier:   prov.Identifier,
		TemplateType: prov.TemplateType,
		Environment:  prov.Environment,
		Owner:        prov.Owner,
	})
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "build var maps"))
	}

	// Inject auto-computed variables for roachprod cluster templates.
	s.injectAutoVars(l, &prov, parsedVars, varMap, envVars)

	// Write secret_file variables to disk and replace env var values with
	// file paths.
	envVars, cleanupCreds, err := vars.PrepareCredentialFiles(workingDir, envVars, resolvedEnv)
	if err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "prepare credential files"))
	}
	defer func() {
		if cleanErr := cleanupCreds(); cleanErr != nil {
			l.Warn("failed to clean up credential files",
				slog.String("working_dir", workingDir),
				slog.Any("error", cleanErr),
			)
		}
	}()

	// Apply backend-specific env overrides AFTER all user/environment
	// variables to prevent user override of backend auth credentials.
	for k, v := range s.backend.EnvOverrides() {
		envVars[k] = v
	}

	// Step: Init
	prov.LastStep = "init"
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "update last_step to init"))
	}
	if err := s.executor.Init(ctx, l, workingDir, envVars); err != nil {
		return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "tofu init"))
	}

	// Step: Pre-destroy hooks
	if s.hookOrchestrator != nil {
		prov.LastStep = "pre_destroy_hooks"
		prov.UpdatedAt = timeutil.Now().UTC()
		if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
			return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "update last_step to pre_destroy_hooks"))
		}
		if err := s.hookOrchestrator.RunPreDestroy(ctx, l, prov, workingDir, resolvedEnv); err != nil {
			return s.failDestroy(ctx, l, &prov, errors.Wrap(err, "pre-destroy hooks"))
		}
	}

	// Step: Destroy
	prov.LastStep = "destroy"
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
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

	// Done — clear plan output in DB and mark destroyed.
	if prov.PlanOutputRef != "" {
		if err := s.deleteArtifactIfPresent(context.WithoutCancel(ctx), prov.PlanOutputRef); err != nil {
			l.Warn("failed to delete plan artifact after destroy",
				slog.String("provisioning_id", prov.ID.String()),
				slog.String("plan_output_ref", prov.PlanOutputRef),
				slog.Any("error", err),
			)
		}
	}
	if err := s.repo.StorePlanData(ctx, l, prov.ID, nil, ""); err != nil {
		return errors.Wrap(err, "clear plan output")
	}
	prov.SetState(provmodels.ProvisioningStateDestroyed, l)
	prov.LastStep = "done"
	prov.Outputs = make(map[string]interface{}) // stores as {} in JSONB
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioningProgress(ctx, l, prov); err != nil {
		return errors.Wrap(err, "update state to destroyed")
	}

	l.Info("provisioning destroyed successfully",
		slog.String("provisioning_id", prov.ID.String()),
		slog.String("identifier", prov.Identifier),
	)
	return nil
}

// injectAutoVars conditionally injects auto-computed TF variables for
// roachprod cluster templates. Variables are only injected if the template
// declares them (checked via parsedVars). This allows non-roachprod
// templates to remain unaffected.
//
// Injected variables:
//   - gce_roachprod_cluster_startup_script: precomputed GCE startup script
//     with SSHPublicKeyPlaceholder replaced by the resolved ssh_public_key.
//   - aws_roachprod_cluster_startup_script: precomputed AWS startup script.
//   - roachprod_vm_labels: JSON-serialized label map for roachprod VMs.
func (s *Service) injectAutoVars(
	l *logger.Logger,
	prov *provmodels.Provisioning,
	parsedVars map[string]provmodels.TemplateOption,
	varMap map[string]string,
	envVars map[string]string,
) {
	if s.autoStartupScripts == nil {
		return
	}

	// Inject GCE startup script if declared by the template.
	if _, declared := parsedVars["gce_roachprod_cluster_startup_script"]; declared {
		script := s.autoStartupScripts.gce
		// Replace the SSH public key placeholder with the resolved value.
		// User-provided and environment ssh_public_key values are both
		// delivered via TF_VAR_* env vars.
		if sshPubKey, ok := envVars["TF_VAR_ssh_public_key"]; ok && sshPubKey != "" {
			script = strings.ReplaceAll(script, vm.SSHPublicKeyPlaceholder, sshPubKey)
		}
		envVars["TF_VAR_gce_roachprod_cluster_startup_script"] = script
	}

	// Inject AWS startup script if declared by the template.
	if _, declared := parsedVars["aws_roachprod_cluster_startup_script"]; declared {
		envVars["TF_VAR_aws_roachprod_cluster_startup_script"] = s.autoStartupScripts.aws
	}

	// Inject roachprod VM labels if declared by the template.
	if _, declared := parsedVars["roachprod_vm_labels"]; declared {
		// Use the precomputed ClusterName from CreateProvisioning which
		// already applies clusterNameFromOwner (email local part + provName).
		labelMap := vm.GetDefaultLabelMap(vm.CreateOpts{
			ClusterName: prov.ClusterName,
			Lifetime:    prov.Lifetime,
		})
		labelMap[vm.TagProvisioningIdentifier] = prov.Identifier

		labelJSON, err := json.Marshal(labelMap)
		if err != nil {
			l.Warn("failed to marshal roachprod_vm_labels",
				slog.Any("error", err),
			)
			return
		}
		envVars["TF_VAR_roachprod_vm_labels"] = string(labelJSON)
	}
}

// failProvision marks the provisioning as failed and persists the error.
func (s *Service) failProvision(
	ctx context.Context, l *logger.Logger, prov *provmodels.Provisioning, provErr error,
) error {
	prov.Error = provErr.Error()
	prov.SetState(provmodels.ProvisioningStateFailed, l)
	prov.UpdatedAt = timeutil.Now().UTC()
	repoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failurePersistTimeout)
	defer cancel()
	if updateErr := s.repo.UpdateProvisioningProgress(repoCtx, l, *prov); updateErr != nil {
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
	prov.UpdatedAt = timeutil.Now().UTC()
	repoCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failurePersistTimeout)
	defer cancel()
	if updateErr := s.repo.UpdateProvisioningProgress(repoCtx, l, *prov); updateErr != nil {
		l.Error("failed to update provisioning state to destroy_failed",
			slog.String("provisioning_id", prov.ID.String()),
			slog.Any("update_error", updateErr),
			slog.Any("original_error", provErr),
		)
	}
	return provErr
}
