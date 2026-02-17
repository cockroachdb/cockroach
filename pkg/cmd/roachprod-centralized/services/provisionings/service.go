// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	taskmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	provrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	ptasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/tofu"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/vars"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Service implements the provisionings service interface.
type Service struct {
	repo        provrepo.IProvisioningsRepository
	envService  envtypes.IService
	taskService stasks.IService
	executor    tofu.IExecutor
	templateMgr *templates.Manager
	options     types.Options

	backgroundJobsCancelFunc context.CancelFunc
	backgroundJobsWg         *sync.WaitGroup
}

// NewService creates a new provisionings service.
func NewService(
	repo provrepo.IProvisioningsRepository,
	envService envtypes.IService,
	taskService stasks.IService,
	opts types.Options,
) *Service {
	return &Service{
		repo:        repo,
		envService:  envService,
		taskService: taskService,
		executor:    tofu.NewExecutor(opts.TofuBinary),
		templateMgr: templates.NewManager(opts.TemplatesDir),
		options:     opts,
	}
}

// RegisterTasks registers the provisioning tasks with the tasks service.
func (s *Service) RegisterTasks(ctx context.Context) error {
	if s.taskService != nil {
		s.taskService.RegisterTasksService(s)
	}
	return nil
}

// StartService initializes the service for operation.
func (s *Service) StartService(ctx context.Context, l *logger.Logger) error {
	s.backgroundJobsWg = &sync.WaitGroup{}
	return nil
}

// StartBackgroundWork starts background goroutines for the service.
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {
	if !s.options.WorkersEnabled {
		l.Info("provisionings: skipping background work (workers disabled)")
		return nil
	}
	ctx, s.backgroundJobsCancelFunc = context.WithCancel(context.WithoutCancel(ctx))
	// Future: start TTL watcher here.
	return nil
}

// Shutdown gracefully stops the service and waits for background work.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.backgroundJobsWg.Wait()
		close(done)
	}()
	if s.backgroundJobsCancelFunc != nil {
		s.backgroundJobsCancelFunc()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// GetTemplates returns all available templates.
func (s *Service) GetTemplates(
	ctx context.Context, l *logger.Logger,
) ([]provmodels.Template, error) {
	return s.templateMgr.ListTemplates()
}

// GetTemplate returns a single template by name.
func (s *Service) GetTemplate(
	ctx context.Context, l *logger.Logger, name string,
) (provmodels.Template, error) {
	tmpl, err := s.templateMgr.GetTemplate(name)
	if err != nil {
		return provmodels.Template{}, types.ErrTemplateNotFound
	}
	return tmpl, nil
}

// GetProvisioning returns a provisioning by ID, subject to access control.
// PlanOutput and Outputs are stripped from the response — use the dedicated
// GetProvisioningPlan and GetProvisioningOutputs endpoints instead.
func (s *Service) GetProvisioning(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) (provmodels.Provisioning, error) {
	prov, err := s.getProvisioningWithAccess(ctx, l, principal, id, types.PermissionViewAll, types.PermissionViewOwn)
	if err != nil {
		return provmodels.Provisioning{}, err
	}
	prov.PlanOutput = nil
	prov.Outputs = nil
	return prov, nil
}

// GetProvisionings returns provisionings matching the given filters.
// Principals with :all see everything; principals with :own see only
// their own provisionings. PlanOutput and Outputs are stripped.
func (s *Service) GetProvisionings(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, input types.InputGetAllDTO,
) ([]provmodels.Provisioning, int, error) {
	provs, _, err := s.repo.GetProvisionings(ctx, l, input.Filters)
	if err != nil {
		return nil, 0, err
	}
	viewAll := principal.HasPermission(types.PermissionViewAll)
	var result []provmodels.Provisioning
	for _, p := range provs {
		if viewAll || (principal.HasPermission(types.PermissionViewOwn) && s.isOwner(principal, p)) {
			p.PlanOutput = nil
			p.Outputs = nil
			result = append(result, p)
		}
	}
	return result, len(result), nil
}

// GetProvisioningPlan returns the plan output for a provisioning.
func (s *Service) GetProvisioningPlan(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) (json.RawMessage, error) {
	prov, err := s.getProvisioningWithAccess(ctx, l, principal, id, types.PermissionViewAll, types.PermissionViewOwn)
	if err != nil {
		return nil, err
	}
	return prov.PlanOutput, nil
}

// GetProvisioningOutputs returns the outputs for a provisioning.
func (s *Service) GetProvisioningOutputs(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) (map[string]interface{}, error) {
	prov, err := s.getProvisioningWithAccess(ctx, l, principal, id, types.PermissionViewAll, types.PermissionViewOwn)
	if err != nil {
		return nil, err
	}
	return prov.Outputs, nil
}

// getProvisioningWithAccess loads a provisioning from the repo and checks
// access. Returns ErrProvisioningNotFound if the provisioning doesn't exist
// or if the principal lacks the required permissions.
func (s *Service) getProvisioningWithAccess(
	ctx context.Context,
	l *logger.Logger,
	principal *auth.Principal,
	id uuid.UUID,
	allPerm, ownPerm string,
) (provmodels.Provisioning, error) {
	prov, err := s.repo.GetProvisioning(ctx, l, id)
	if err != nil {
		if errors.Is(err, provrepo.ErrProvisioningNotFound) {
			return provmodels.Provisioning{}, types.ErrProvisioningNotFound
		}
		return provmodels.Provisioning{}, err
	}
	if !s.checkAccess(principal, prov, allPerm, ownPerm) {
		return provmodels.Provisioning{}, types.ErrProvisioningNotFound
	}
	return prov, nil
}

// CreateProvisioning creates a new provisioning and schedules a provision task.
// Returns the provisioning, the scheduled task ID, and any error.
func (s *Service) CreateProvisioning(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, input types.InputCreateDTO,
) (provmodels.Provisioning, *uuid.UUID, error) {
	// Validate environment exists.
	_, err := s.envService.GetEnvironment(ctx, l, principal, input.Environment)
	if err != nil {
		if errors.Is(err, envtypes.ErrEnvironmentNotFound) {
			return provmodels.Provisioning{}, nil, types.ErrEnvironmentNotFound
		}
		return provmodels.Provisioning{}, nil, err
	}

	// Validate template exists.
	tmpl, err := s.templateMgr.GetTemplate(input.TemplateType)
	if err != nil {
		return provmodels.Provisioning{}, nil, types.ErrTemplateNotFound
	}

	// Resolve environment to validate variables before storing.
	resolvedEnv, err := s.envService.GetEnvironmentResolved(ctx, l, input.Environment)
	if err != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(err, "resolve environment")
	}

	// Validate user-provided variables against the template schema and
	// resolved environment. Uses a placeholder identifier since it hasn't
	// been generated yet (identifier is always auto-injected so it won't
	// trigger a missing-required error).
	if _, _, err := vars.BuildVarMaps(vars.BuildVarMapsInput{
		ResolvedEnv:    resolvedEnv,
		UserVars:       input.Variables,
		TemplateVars:   tmpl.Variables,
		Identifier:     "validation",
		TemplateType:   input.TemplateType,
		Environment:    input.Environment,
		Owner:          getOwnerEmail(principal),
		BackendEnvVars: templates.GCSBackendEnvVars(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")),
	}); err != nil {
		return provmodels.Provisioning{}, nil, utils.NewPublicError(err)
	}

	// Snapshot the template.
	archive, checksum, err := s.templateMgr.SnapshotTemplate(input.TemplateType)
	if err != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(err, "snapshot template")
	}

	// Parse lifetime if provided.
	var lifetime time.Duration
	if input.Lifetime != "" {
		lifetime, err = time.ParseDuration(input.Lifetime)
		if err != nil {
			return provmodels.Provisioning{}, nil, types.ErrInvalidLifetime
		}
	}

	now := timeutil.Now().UTC()
	prov := provmodels.Provisioning{
		ID:               uuid.MakeV4(),
		Environment:      input.Environment,
		TemplateType:     input.TemplateType,
		TemplateChecksum: checksum,
		TemplateSnapshot: archive,
		State:            provmodels.ProvisioningStateNew,
		Variables:        input.Variables,
		Owner:            getOwnerEmail(principal),
		Lifetime:         lifetime,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	if lifetime > 0 {
		expiresAt := now.Add(lifetime)
		prov.ExpiresAt = &expiresAt
	}

	// Generate identifier and store provisioning (retry on collision).
	for attempt := range types.MaxIdentifierRetries {

		// Generate a unique identifier for the provisioning.
		identifier, genErr := provmodels.GenerateIdentifier()
		if genErr != nil {
			return provmodels.Provisioning{}, nil, errors.Wrap(genErr, "generate identifier")
		}
		prov.Identifier = identifier
		prov.Name = fmt.Sprintf("%s-%s", input.TemplateType, identifier)

		storeErr := s.repo.StoreProvisioning(ctx, l, prov)
		if storeErr == nil {
			break
		}
		if !errors.Is(storeErr, provrepo.ErrProvisioningAlreadyExists) {
			return provmodels.Provisioning{}, nil, storeErr
		}
		if attempt == types.MaxIdentifierRetries-1 {
			return provmodels.Provisioning{}, nil, types.ErrIdentifierCollision
		}
		l.Warn("identifier collision, retrying",
			slog.Int("attempt", attempt+1),
			slog.String("identifier", identifier),
		)
	}

	// Store succeeded — schedule the provision task.
	task, taskErr := ptasks.NewTaskProvision(prov.ID)
	if taskErr != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(taskErr, "create provision task")
	}
	if _, taskErr = s.taskService.CreateTask(ctx, l, task); taskErr != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(taskErr, "schedule provision task")
	}

	taskID := task.GetID()
	return prov, &taskID, nil
}

// DestroyProvisioning handles destroy requests based on the provisioning's
// current state:
//   - new: no infra exists, mark destroyed immediately
//   - destroyed: no-op, return current state
//   - in-flight (initializing/planning/provisioning/destroying): block if a
//     task is active, otherwise schedule tofu destroy
//   - idle/terminal (provisioned/failed/destroy_failed): schedule tofu destroy
func (s *Service) DestroyProvisioning(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) (provmodels.Provisioning, *uuid.UUID, error) {
	prov, err := s.getProvisioningWithAccess(ctx, l, principal, id, types.PermissionDestroyAll, types.PermissionDestroyOwn)
	if err != nil {
		return provmodels.Provisioning{}, nil, err
	}

	// Clear plan output and outputs because if successful, we will return the provisioning,
	// and we have dedicated paths for these fields that should be used instead of overloading
	// the main provisioning object.
	resp := prov
	resp.PlanOutput = nil
	resp.Outputs = nil

	switch prov.State {
	case provmodels.ProvisioningStateNew:
		// No infra was created — mark destroyed immediately.
		prov.SetState(provmodels.ProvisioningStateDestroyed, l)
		prov.UpdatedAt = timeutil.Now().UTC()
		if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
			return provmodels.Provisioning{}, nil, err
		}
		return resp, nil, nil

	case provmodels.ProvisioningStateDestroyed:
		// Already destroyed — no-op.
		prov.PlanOutput = nil
		prov.Outputs = nil
		return resp, nil, nil

	case provmodels.ProvisioningStateInitializing,
		provmodels.ProvisioningStatePlanning,
		provmodels.ProvisioningStateProvisioning,
		provmodels.ProvisioningStateDestroying:
		// In-flight — block if a task is currently active.
		active, err := s.hasActiveTask(ctx, l, prov.ID)
		if err != nil {
			return provmodels.Provisioning{}, nil, err
		}
		if active {
			return provmodels.Provisioning{}, nil, types.ErrTaskInProgress
		}
		// No active task — state is stale, proceed with destroy.
		fallthrough

	case provmodels.ProvisioningStateProvisioned,
		provmodels.ProvisioningStateFailed,
		provmodels.ProvisioningStateDestroyFailed:
		// Schedule tofu destroy.
		prov.SetState(provmodels.ProvisioningStateDestroying, l)
		prov.UpdatedAt = timeutil.Now().UTC()
		if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
			return provmodels.Provisioning{}, nil, err
		}
		task, err := ptasks.NewTaskDestroy(prov.ID)
		if err != nil {
			return provmodels.Provisioning{}, nil, errors.Wrap(err, "create destroy task")
		}
		if _, err = s.taskService.CreateTask(ctx, l, task); err != nil {
			return provmodels.Provisioning{}, nil, errors.Wrap(err, "schedule destroy task")
		}
		taskID := task.GetID()
		return resp, &taskID, nil

	default:
		return provmodels.Provisioning{}, nil, types.ErrInvalidState
	}
}

// DeleteProvisioning removes the provisioning record from the database.
// Only allowed when the provisioning is in state new or destroyed.
func (s *Service) DeleteProvisioning(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) error {
	prov, err := s.getProvisioningWithAccess(ctx, l, principal, id, types.PermissionDestroyAll, types.PermissionDestroyOwn)
	if err != nil {
		return err
	}

	if prov.State != provmodels.ProvisioningStateNew &&
		prov.State != provmodels.ProvisioningStateDestroyed {
		return types.ErrInvalidState
	}

	// TODO: clean up GCS state prefix for "provisioning-<id>".
	return s.repo.DeleteProvisioning(ctx, l, prov.ID)
}

// hasActiveTask checks whether a pending or running task exists for the
// given provisioning by querying the task service using the reference field.
func (s *Service) hasActiveTask(
	ctx context.Context, l *logger.Logger, provID uuid.UUID,
) (bool, error) {
	ref := ptasks.ReferencePrefix + provID.String()
	fs := filters.NewFilterSet().
		AddFilter("Reference", filtertypes.OpEqual, ref).
		AddFilter("State", filtertypes.OpIn, []string{
			string(taskmodels.TaskStatePending),
			string(taskmodels.TaskStateRunning),
		})
	result, _, err := s.taskService.GetTasks(ctx, l, nil, stasks.InputGetAllTasksDTO{Filters: *fs})
	if err != nil {
		return false, err
	}
	return len(result) > 0, nil
}

// checkAccess returns true if the principal has the allPerm permission, or
// the ownPerm permission and is the provisioning owner.
func (s *Service) checkAccess(
	principal *auth.Principal, prov provmodels.Provisioning, allPerm, ownPerm string,
) bool {
	if principal.HasPermission(allPerm) {
		return true
	}
	if principal.HasPermission(ownPerm) && s.isOwner(principal, prov) {
		return true
	}
	return false
}

// isOwner checks whether the principal owns the provisioning.
func (s *Service) isOwner(principal *auth.Principal, prov provmodels.Provisioning) bool {
	if principal.User != nil {
		return principal.User.Email == prov.Owner
	}
	if principal.Claims != nil {
		if email, ok := principal.Claims["email"].(string); ok {
			return email == prov.Owner
		}
	}
	return false
}

// getOwnerEmail extracts the email from the principal.
func getOwnerEmail(principal *auth.Principal) string {
	if principal.User != nil {
		return principal.User.Email
	}
	if principal.Claims != nil {
		if email, ok := principal.Claims["email"].(string); ok {
			return email
		}
	}
	return ""
}

// GetRepo returns the provisioning repository (used by task handlers).
func (s *Service) GetRepo() provrepo.IProvisioningsRepository {
	return s.repo
}

// GetEnvService returns the environments service (used by task handlers).
func (s *Service) GetEnvService() envtypes.IService {
	return s.envService
}

// GetExecutor returns the tofu executor (used by task handlers).
func (s *Service) GetExecutor() tofu.IExecutor {
	return s.executor
}

// GetTemplateMgr returns the template manager (used by task handlers).
func (s *Service) GetTemplateMgr() *templates.Manager {
	return s.templateMgr
}

// GetOptions returns the service options (used by task handlers).
func (s *Service) GetOptions() types.Options {
	return s.options
}
