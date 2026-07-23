// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/internal/scheduler"
	sclustertasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	healthtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/types"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/ibm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Service implements the clusters service interface and manages cluster discovery and synchronization
// across multiple cloud providers. It coordinates with cloud APIs to discover roachprod clusters
// and maintains an up-to-date view of the distributed cluster infrastructure.
type Service struct {
	options Options

	backgroundJobsCancelFunc context.CancelFunc
	backgroundJobsWg         *sync.WaitGroup
	roachprodCloud           *cloud.Cloud

	// providerEnvironments maps providerID (e.g., "gce-my-project") to the
	// configured environment name (e.g., "gcp-engineering"). Used by
	// authorization checks to resolve cluster cloud providers to permission
	// scopes.
	providerEnvironments map[string]string

	_taskService   stasks.IService
	_store         clusters.IClustersRepository
	_healthService healthtypes.IHealthService
}

// Options contains configuration parameters for the clusters service.
type Options struct {
	// CloudProviders lists the cloud providers to monitor for clusters.
	CloudProviders []configtypes.CloudProvider
	// DNSProviders maps DNS domain to DNS provider instances for cloud provider injection.
	// This allows cloud providers to manage DNS records for their clusters.
	DNSProviders map[string]vm.DNSProvider
	// PeriodicRefreshEnabled controls whether background cluster synchronization is enabled.
	PeriodicRefreshEnabled bool
	// PeriodicRefreshInterval specifies how often to sync clusters from cloud providers.
	PeriodicRefreshInterval time.Duration
	// NoInitialSync skips the initial cluster synchronization on service startup.
	NoInitialSync bool
	// WorkersEnabled indicates whether task workers are running
	WorkersEnabled bool
}

// NewService creates a new clusters service.
func NewService(
	store clusters.IClustersRepository,
	tasksService stasks.IService,
	healthService healthtypes.IHealthService,
	options Options,
) (*Service, error) {

	service := &Service{
		backgroundJobsWg: &sync.WaitGroup{},
		_taskService:     tasksService,
		_store:           store,
		_healthService:   healthService,
		options:          options,
	}

	// Build the environment mapping from config. This is used by both
	// worker and API-only modes for authorization scope resolution.
	service.providerEnvironments = buildProviderEnvironments(
		slog.Default(), options.CloudProviders,
	)

	// If instance is API only, skip cloud provider initialization
	// because no background tasks will be scheduled.
	// This allows running the API in lightweight environments
	// without cloud credentials.
	// This means API only mode cannot perform Cloud operations, and will only
	// serve the state and schedule tasks.
	if service.options.WorkersEnabled {
		cloudProviders := []vm.Provider{}
		for _, cp := range options.CloudProviders {

			var dnsProvider vm.DNSProvider
			if len(options.DNSProviders) > 0 && cp.DNSPublicZone != "" {
				if dp, found := options.DNSProviders[cp.DNSPublicZone]; found {
					dnsProvider = dp
				}
			}

			var provider vm.Provider
			var err error
			switch strings.ToLower(cp.Type) {
			case gce.ProviderName:
				opts := cp.GCE.ToOptions()

				// Inject DNS provider from registry if available.
				if dnsProvider != nil {
					opts = append(opts, gce.WithDNSProvider(dnsProvider))
				}

				provider, err = gce.NewProvider(opts...)
				if err != nil {
					return nil, errors.Wrapf(err, "unable to create GCE cloud provider")
				}

			case aws.ProviderName:
				opts := cp.AWS.ToOptions()

				// Inject DNS provider from registry if available.
				if dnsProvider != nil {
					opts = append(opts, aws.WithDNSProvider(dnsProvider))
				}

				provider, err = aws.NewProvider(opts...)
				if err != nil {
					return nil, errors.Wrapf(err, "unable to create AWS cloud provider")
				}

			case azure.ProviderName:
				opts := cp.Azure.ToOptions()

				// Inject DNS provider from registry if available.
				if dnsProvider != nil {
					opts = append(opts, azure.WithDNSProvider(dnsProvider))
				}

				provider, err = azure.NewProvider(opts...)
				if err != nil {
					return nil, errors.Wrapf(err, "unable to create Azure cloud provider")
				}

			case ibm.ProviderName:
				opts := cp.IBM.ToOptions()

				// Inject DNS provider from registry if available.
				if dnsProvider != nil {
					opts = append(opts, ibm.WithDNSProvider(dnsProvider))
				}

				provider, err = ibm.NewProvider(opts...)
				if err != nil {
					return nil, errors.Wrapf(err, "unable to create IBM cloud provider")
				}

			default:
				return nil, errors.Newf("unknown cloud provider type: %s", cp.Type)
			}

			cloudProviders = append(cloudProviders, provider)
		}

		// Initialize roachprod Cloud with the desired providers.
		service.roachprodCloud = cloud.NewCloud(
			cloud.WithProviders(cloudProviders),
		)

		// Validate that the derived environment mapping matches the real
		// provider identities. Mismatches indicate a config problem (e.g.,
		// the AccountID in config doesn't match the credentials).
		validateProviderEnvironments(
			slog.Default(), service.providerEnvironments, cloudProviders,
		)
	}

	// If the periodic refresh interval is not set, use the default.
	if service.options.PeriodicRefreshInterval == 0 {
		service.options.PeriodicRefreshInterval = types.DefaultPeriodicRefreshInterval
	}

	return service, nil
}

// RegisterTasks registers the cluster tasks with the tasks service.
func (s *Service) RegisterTasks(ctx context.Context) error {
	// Register the cluster tasks with the tasks service.
	if s._taskService != nil {
		s._taskService.RegisterTasksService(s)
	}
	return nil
}

// GetTaskServiceName returns the name of the task service.
func (s *Service) GetTaskServiceName() string {
	return types.TaskServiceName
}

func (s *Service) StartService(ctx context.Context, l *logger.Logger) error {
	// Initialize the background jobs wait group.
	s.backgroundJobsWg = &sync.WaitGroup{}

	// Check if we should perform initial sync
	if s.shouldPerformInitialSync(ctx, l) {
		// Schedule sync task
		task, err := s._taskService.CreateTaskIfNotAlreadyPlanned(ctx, l, sclustertasks.NewTaskSync())
		if err != nil {
			return errors.Wrap(err, "failed to schedule initial sync task")
		}

		// Wait for task completion (blocks until sync is done)
		l.Info("waiting for initial sync task to complete", slog.String("task_id", task.GetID().String()))
		err = s._taskService.WaitForTaskCompletion(ctx, l, task.GetID(), 5*time.Minute)
		if err != nil {
			return errors.Wrap(err, "initial sync failed")
		}
		l.Info("initial sync completed successfully")
	}

	return nil
}

// StartBackgroundWork initializes the service by making the initial clusters sync
// and starting the periodic refresh if enabled.
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {

	// Only start background work if workers are enabled (API-only mode doesn't schedule tasks)
	if !s.options.WorkersEnabled {
		l.Info("clusters service: skipping background work (workers disabled)")
		return nil
	}

	// Create a new context without the parent cancel because we prefer
	// to properly handle the context cancellation in the Shutdown method
	// that is called by our parent when the app is stopping.
	ctx, s.backgroundJobsCancelFunc = context.WithCancel(context.WithoutCancel(ctx))

	// If there is a task service and the periodic refresh is enabled,
	// start the periodic refresh.
	if s._taskService != nil && s.options.PeriodicRefreshEnabled {
		s.backgroundJobsWg.Add(1)
		scheduler.StartPeriodicRefresh(
			ctx, l, errChan,
			s.options.PeriodicRefreshInterval,
			s, // implements scheduler.PeriodicRefreshScheduler
			s.backgroundJobsWg.Done,
		)
	}
	return nil
}

// Shutdown contains the logic to shutdown the service.
// It waits for the background jobs to finish and cancels the periodic refresh
// context.
func (s *Service) Shutdown(ctx context.Context) error {

	done := make(chan struct{})

	// Start a goroutine to wait for the workers to finish
	go func() {
		s.backgroundJobsWg.Wait()
		close(done)
	}()

	// Cancel the background context to stop the periodic refresh.
	if s.backgroundJobsCancelFunc != nil {
		s.backgroundJobsCancelFunc()
	}

	// Wait for either the workers to finish or the context to be cancelled
	select {
	case <-ctx.Done():
		// If context is done (e.g., due to timeout), return the error
		return types.ErrShutdownTimeout
	case <-done:
		// If all workers finish successfully, return nil
		return nil
	}

}

func (s *Service) shouldPerformInitialSync(ctx context.Context, l *logger.Logger) bool {
	if s.options.NoInitialSync {
		l.Debug("skipping initial sync (NoInitialSync=true)")
		return false
	}

	// Get most recent completed sync task
	lastTask, err := s._taskService.GetMostRecentCompletedTaskOfType(
		ctx, l, string(sclustertasks.ClustersTaskSync))

	if err != nil || lastTask == nil {
		l.Info("no recent sync found, will perform initial sync")
		return true
	}

	timeSinceLastSync := timeutil.Since(lastTask.GetUpdateDatetime())

	if timeSinceLastSync >= s.options.PeriodicRefreshInterval {
		l.Info(
			"last sync is stale, will perform initial sync",
			slog.String("time_since_last_sync", timeSinceLastSync.String()),
			slog.String("periodic_refresh_interval", s.options.PeriodicRefreshInterval.String()),
		)
		return true
	}

	// Check for healthy worker and API instances
	// IMPORTANT: Exclude the current instance because:
	// - This is called during StartService(), BEFORE the API server starts
	// - The current instance's API cannot yet receive requests
	// - Only OTHER instances count for coverage
	instances, err := s._healthService.GetHealthyInstances(ctx, l)
	if err != nil {
		l.Warn("failed to check healthy instances, will perform initial sync", slog.Any("error", err))
		return true
	}

	currentInstanceID := s._healthService.GetInstanceID()
	hasWorkers := false
	hasAPI := false
	for _, inst := range instances {
		// Skip the current instance - it's not operational yet
		if inst.InstanceID == currentInstanceID {
			continue
		}

		if inst.Mode == health.WorkersOnly || inst.Mode == health.APIWithWorkers {
			hasWorkers = true
		}
		if inst.Mode == health.APIOnly || inst.Mode == health.APIWithWorkers {
			hasAPI = true
		}
	}

	// If we have a recent sync and workers exist, we can skip ONLY if:
	// - There are currently healthy API instances, OR
	// - There's been no gap in API coverage since the last sync
	//
	// Since we can't easily track "continuous API coverage", we use a simpler heuristic:
	// Skip sync if BOTH workers AND API instances are currently healthy
	if hasWorkers && hasAPI {
		l.Info(
			"skipping initial sync (recent sync with both worker and API coverage)",
			slog.String("time_since_last_sync", timeSinceLastSync.String()),
		)
		return false
	}

	// If only workers exist (no API), we MUST sync because:
	// - No API means no cluster operations could have been received
	// - The only source of truth is a fresh sync from cloud providers
	if hasWorkers && !hasAPI {
		l.Info(
			"performing initial sync (workers exist but no API instances - cannot receive cluster operations)",
			slog.String("time_since_last_sync", timeSinceLastSync.String()),
		)
		return true
	}

	// No workers means no periodic refresh will happen
	l.Info(
		"performing initial sync (no worker coverage)",
		slog.String("time_since_last_sync", timeSinceLastSync.String()),
	)
	return true
}
