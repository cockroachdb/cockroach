// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	taskmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	provrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/provisionings"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/hooks"
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
	repo             provrepo.IProvisioningsRepository
	envService       envtypes.IService
	taskService      stasks.IService
	executor         tofu.IExecutor
	templateMgr      *templates.Manager
	options          types.Options
	backend          templates.Backend // GCS or local state backend
	hookOrchestrator hooks.IOrchestrator

	backgroundJobsCancelFunc context.CancelFunc
	backgroundJobsWg         *sync.WaitGroup
}

// NewService creates a new provisionings service.
func NewService(
	repo provrepo.IProvisioningsRepository,
	envService envtypes.IService,
	taskService stasks.IService,
	opts types.Options,
	backend templates.Backend,
	hookOrchestrator hooks.IOrchestrator,
) *Service {
	return &Service{
		repo:             repo,
		envService:       envService,
		taskService:      taskService,
		executor:         tofu.NewExecutor(opts.TofuBinary),
		templateMgr:      templates.NewManager(opts.TemplatesDir),
		options:          opts,
		backend:          backend,
		hookOrchestrator: hookOrchestrator,
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

	// GC scheduler: periodically creates a GC task that scans for expired
	// provisionings. Using the task system ensures only one instance runs
	// the scan at a time, preventing duplicate destroy task creation.
	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()
		l.Info("GC scheduler started",
			slog.Duration("interval", s.options.GCWatcherInterval))

		ticker := time.NewTicker(s.options.GCWatcherInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.Debug("GC scheduler stopped")
				return
			case <-ticker.C:
				if err := s.scheduleGCTaskIfNeeded(ctx, l); err != nil {
					l.Warn("GC scheduler error", slog.Any("error", err))
				}
			}
		}
	}()

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

// scheduleGCTaskIfNeeded creates a GC task if no recent one exists.
// This prevents duplicate GC scans when multiple instances are running.
func (s *Service) scheduleGCTaskIfNeeded(ctx context.Context, l *logger.Logger) error {
	if s.taskService == nil {
		return nil
	}
	task, err := s.taskService.CreateTaskIfNotRecentlyScheduled(
		ctx, l,
		ptasks.NewTaskGC(),
		s.options.GCWatcherInterval,
	)
	if err != nil {
		return err
	}
	if task != nil {
		l.Info("GC task scheduled", slog.String("task_id", task.GetID().String()))
	}
	return nil
}

// HandleGC queries expired provisionings and either marks them destroyed
// directly (if no infra was created) or schedules destroy tasks.
// Runs as a task without auth context (system operation).
//
// The repository query already excludes destroyed and destroying states.
// For state=new, no infra exists so we mark destroyed immediately. For
// all other states, we delegate to gcScheduleDestroy which checks for
// active tasks before scheduling (handles both in-flight and idle states).
func (s *Service) HandleGC(ctx context.Context, l *logger.Logger) error {
	expired, err := s.repo.GetExpiredProvisionings(ctx, l)
	if err != nil {
		return errors.Wrap(err, "query expired provisionings")
	}

	for _, prov := range expired {
		switch prov.State {
		case provmodels.ProvisioningStateNew:
			// No infra created — mark destroyed immediately.
			prov.SetState(provmodels.ProvisioningStateDestroyed, l)
			prov.UpdatedAt = timeutil.Now().UTC()
			if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
				l.Warn("GC: failed to mark new provisioning as destroyed",
					slog.String("id", prov.ID.String()), slog.Any("error", err))
			}

		default:
			// All other states (initializing, planning, provisioning,
			// provisioned, failed, destroy_failed): schedule destroy.
			// gcScheduleDestroy checks hasActiveTask internally and skips
			// if a task is already pending/running.
			s.gcScheduleDestroy(ctx, l, &prov)
		}
	}
	return nil
}

// gcScheduleDestroy transitions a provisioning to destroying and creates a
// destroy task. Checks for an already-scheduled task to avoid duplicates.
// Logs warnings on errors instead of returning them (best-effort GC).
func (s *Service) gcScheduleDestroy(
	ctx context.Context, l *logger.Logger, prov *provmodels.Provisioning,
) {
	// Check if a destroy task is already pending or running for this
	// provisioning to avoid creating duplicates when GC runs faster than
	// task processing.
	active, err := s.hasActiveTask(ctx, l, prov.ID)
	if err != nil {
		l.Warn("GC: failed to check active tasks",
			slog.String("id", prov.ID.String()), slog.Any("error", err))
		return
	}
	if active {
		return
	}

	prov.SetState(provmodels.ProvisioningStateDestroying, l)
	prov.UpdatedAt = timeutil.Now().UTC()
	if err := s.repo.UpdateProvisioning(ctx, l, *prov); err != nil {
		l.Warn("GC: failed to update state to destroying",
			slog.String("id", prov.ID.String()), slog.Any("error", err))
		return
	}
	task, err := ptasks.NewTaskDestroy(prov.ID)
	if err != nil {
		l.Warn("GC: failed to create destroy task",
			slog.String("id", prov.ID.String()), slog.Any("error", err))
		return
	}
	if _, err := s.taskService.CreateTask(ctx, l, task); err != nil {
		l.Warn("GC: failed to schedule destroy task",
			slog.String("id", prov.ID.String()), slog.Any("error", err))
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

// ExtendLifetime extends the provisioning's expiration by the configured
// extension duration. Cannot extend destroyed provisionings.
func (s *Service) ExtendLifetime(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) (provmodels.Provisioning, error) {
	prov, err := s.getProvisioningWithAccess(ctx, l, principal, id,
		types.PermissionUpdateAll, types.PermissionUpdateOwn)
	if err != nil {
		return provmodels.Provisioning{}, err
	}

	if prov.State == provmodels.ProvisioningStateDestroyed {
		return provmodels.Provisioning{}, types.ErrInvalidState
	}

	now := timeutil.Now().UTC()
	if prov.ExpiresAt != nil {
		extended := prov.ExpiresAt.Add(s.options.LifetimeExtension)
		prov.ExpiresAt = &extended
	} else {
		extended := now.Add(s.options.LifetimeExtension)
		prov.ExpiresAt = &extended
	}
	prov.UpdatedAt = now

	if err := s.repo.UpdateProvisioning(ctx, l, prov); err != nil {
		return provmodels.Provisioning{}, err
	}

	prov.PlanOutput = nil
	prov.Outputs = nil
	return prov, nil
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
		ResolvedEnv:  resolvedEnv,
		UserVars:     input.Variables,
		TemplateVars: tmpl.Variables,
		Identifier:   "validation",
		TemplateType: input.TemplateType,
		Environment:  input.Environment,
		Owner:        getOwnerEmail(principal),
	}); err != nil {
		return provmodels.Provisioning{}, nil, utils.NewPublicError(err)
	}

	// Validate that hook environment references are resolvable. This catches
	// missing SSH key vars and hook env mappings before infra is created.
	if err := hooks.ValidateHookEnvironment(
		tmpl.TemplateMetadata, resolvedEnv,
	); err != nil {
		return provmodels.Provisioning{}, nil, utils.NewPublicError(err)
	}

	// Snapshot the template.
	archive, checksum, err := s.templateMgr.SnapshotTemplate(input.TemplateType)
	if err != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(err, "snapshot template")
	}

	// Determine lifetime: user input > template default > global default.
	lifetime := s.options.DefaultLifetime
	if input.Lifetime != "" {
		lifetime, err = time.ParseDuration(input.Lifetime)
		if err != nil {
			return provmodels.Provisioning{}, nil, types.ErrInvalidLifetime
		}
	} else if tmpl.DefaultLifetime != "" {
		parsed, parseErr := time.ParseDuration(tmpl.DefaultLifetime)
		if parseErr != nil {
			l.Warn("invalid default_lifetime in template metadata, using global default",
				slog.String("template", input.TemplateType),
				slog.String("default_lifetime", tmpl.DefaultLifetime),
				slog.Any("error", parseErr),
			)
		} else {
			lifetime = parsed
		}
	}

	now := timeutil.Now().UTC()
	expiresAt := now.Add(lifetime)
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
		ExpiresAt:        &expiresAt,
		CreatedAt:        now,
		UpdatedAt:        now,
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

	// Best-effort backend state cleanup.
	statePrefix := "provisioning-" + prov.ID.String()
	if err := s.backend.CleanupState(ctx, l, statePrefix); err != nil {
		l.Warn("failed to clean up backend state during delete",
			slog.String("prefix", statePrefix), slog.Any("error", err))
	}
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

// SetupSSHKeys creates a task to run ssh-keys-setup on a provisioning.
// The provisioning must be in 'provisioned' state.
func (s *Service) SetupSSHKeys(
	ctx context.Context, l *logger.Logger, principal *auth.Principal, id uuid.UUID,
) (provmodels.Provisioning, *uuid.UUID, error) {
	prov, err := s.getProvisioningWithAccess(
		ctx, l, principal, id,
		types.PermissionUpdateAll, types.PermissionUpdateOwn,
	)
	if err != nil {
		return provmodels.Provisioning{}, nil, err
	}

	if prov.State != provmodels.ProvisioningStateProvisioned {
		return prov, nil, types.ErrInvalidState
	}

	task, err := ptasks.NewTaskSSHKeysSetup(prov.ID)
	if err != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(
			err, "create ssh-keys-setup task",
		)
	}

	if _, err = s.taskService.CreateTask(ctx, l, task); err != nil {
		return provmodels.Provisioning{}, nil, errors.Wrap(
			err, "schedule ssh-keys-setup task",
		)
	}

	taskID := task.GetID()
	prov.PlanOutput = nil
	prov.Outputs = nil
	return prov, &taskID, nil
}

// HandleSetupSSHKeys runs the ssh-keys-setup hook for a provisioning.
// Called by the task handler, not directly by API callers.
func (s *Service) HandleSetupSSHKeys(
	ctx context.Context, l *logger.Logger, provisioningID uuid.UUID,
) error {
	prov, err := s.repo.GetProvisioning(ctx, l, provisioningID)
	if err != nil {
		return errors.Wrap(err, "get provisioning")
	}

	resolvedEnv, err := s.envService.GetEnvironmentResolved(
		ctx, l, prov.Environment,
	)
	if err != nil {
		return errors.Wrap(err, "resolve environment")
	}

	meta, err := templates.ParseMetadataFromSnapshot(prov.TemplateSnapshot)
	if err != nil {
		return errors.Wrap(err, "parse template metadata from snapshot")
	}

	return s.hookOrchestrator.RunByType(
		ctx, l, prov, meta, "ssh-keys-setup", resolvedEnv,
	)
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

// GetBackend returns the backend (used by task handlers).
func (s *Service) GetBackend() templates.Backend {
	return s.backend
}
