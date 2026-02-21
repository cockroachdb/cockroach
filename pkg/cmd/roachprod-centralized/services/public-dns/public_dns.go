// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package publicdns

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	sclustermodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	dnsregistry "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/dns/registry"
	dnstasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/types"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	logger "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	rplogger "github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	ROACHPROD_DIR = ".roachprod"
)

// Service is the implementation of the public DNS service.
type Service struct {
	options types.Options

	// dnsRegistry provides access to DNS providers created by the registry.
	// This replaces the previous infraProviders and dnsProviders maps.
	dnsRegistry dnsregistry.Registry

	_taskService     stasks.IService
	_clustersService sclustermodels.IService
	_syncing         bool
}

// NewService creates a new public DNS service.
// The dnsRegistry parameter provides access to DNS providers that have been
// created by the DNS registry, avoiding the circular dependency with the clusters service.
func NewService(
	clustersService sclustermodels.IService,
	tasksService stasks.IService,
	dnsRegistry *dnsregistry.Registry,
	options types.Options,
) (*Service, error) {
	service := &Service{
		_taskService:     tasksService,
		_clustersService: clustersService,
		dnsRegistry:      *dnsRegistry,
		options:          options,
	}

	return service, nil
}

// RegisterTasks registers the DNS tasks with the tasks service.
func (s *Service) RegisterTasks(ctx context.Context) error {
	// Register the DNS tasks with the tasks service.
	if s._taskService != nil {
		s._taskService.RegisterTasksService(s)
	}
	return nil
}

// StartService initializes the service and registers it with the tasks service.
func (s *Service) StartService(ctx context.Context, l *logger.Logger) error {

	// SyncDNS writes the DNS records to the .roachprod directory in the user's
	// home directory. We make sure the directory exists before starting the service.
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return errors.Wrapf(err, "failed to get the user's home directory")
	}

	err = os.MkdirAll(fmt.Sprintf("%s/%s", homeDir, ROACHPROD_DIR), 0755)
	if err != nil {
		return errors.Wrapf(err, "failed to create service directory %s", ROACHPROD_DIR)
	}
	return nil
}

// StartBackgroundWork is a nil-op for this service as it does not require background work.
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *logger.Logger, errChan chan<- error,
) error {
	return nil
}

// Shutdown is a nil-op as the service does not require shutdown logic.
func (s *Service) Shutdown(ctx context.Context) error {
	return nil
}

// GetTaskServiceName returns the name of the task service.
func (s *Service) GetTaskServiceName() string {
	return types.TaskServiceName
}

// GetHandledTasks returns a map of task types to task implementations that are
// handled by this service.
func (s *Service) GetHandledTasks() map[string]stasks.ITask {
	return map[string]stasks.ITask{
		string(dnstasks.PublicDNSTaskSync): &dnstasks.TaskSync{
			Service: s,
		},
		string(dnstasks.PublicDNSTaskManageRecords): &dnstasks.TaskManageRecords{
			Service: s,
		},
	}
}

// CreateTaskInstance creates a new instance of a task of the given type.
func (s *Service) CreateTaskInstance(taskType string) (tasks.ITask, error) {
	switch taskType {
	case string(dnstasks.PublicDNSTaskSync):
		return &dnstasks.TaskSync{
			Task:    tasks.Task{Type: taskType},
			Service: s,
		}, nil
	case string(dnstasks.PublicDNSTaskManageRecords):
		task := &dnstasks.TaskManageRecords{Service: s}
		task.Type = taskType
		// Options will be deserialized from payload by the task service
		return task, nil
	default:
		return nil, stasks.ErrUnknownTaskType
	}
}

// SyncDNS creates a task to sync the DNS.
func (s *Service) SyncDNS(ctx context.Context, l *logger.Logger) (tasks.ITask, error) {

	// Create a task to sync the clouds.
	task := &dnstasks.TaskSync{}
	task.Type = string(dnstasks.PublicDNSTaskSync)

	// Save the task.
	return s._taskService.CreateTaskIfNotAlreadyPlanned(ctx, l, task)
}

// sync contains the logic to sync the clusters and store them
// in the repository.
func (s *Service) Sync(ctx context.Context, l *logger.Logger) error {

	s._syncing = true
	defer func(s *Service) {
		l.Info("syncing DNS across all providers is over, releasing syncing flag")
		s._syncing = false
	}(s)

	crlLogger, err := (&rplogger.Config{Stdout: l, Stderr: l}).NewLogger("")
	if err != nil {
		return err
	}

	l.Info("syncing DNS across all providers")

	for dnsZone, dnsProvider := range s.dnsRegistry.GetAllDNSProviders() {

		vms, err := s._clustersService.GetAllDNSZoneVMs(ctx, l, dnsZone)
		if err != nil {
			l.Error("failed to get DNS zone VMs",
				slog.Any("error", err),
				slog.String("dns_zone", dnsZone))
			return err
		}

		l.Info("syncing DNS for zone",
			slog.String("zone", dnsZone),
			slog.Int("vms", len(vms)))

		if err := dnsProvider.SyncDNS(crlLogger, vms); err != nil {
			// Check if error is due to 404 (external deletion race)
			// This can happen when records are deleted externally between the time
			// gcloud lists existing records and attempts to delete them
			if is404Error(err) {
				l.Warn("sync hit 404 error, retrying once (external deletion detected)",
					slog.String("dns_zone", dnsZone),
					slog.Any("error", err))

				// Single immediate retry - by the time we retry, the problematic
				// records are already gone and won't appear in the delete list
				if retryErr := dnsProvider.SyncDNS(crlLogger, vms); retryErr != nil {
					l.Error("sync failed after retry",
						slog.Any("error", retryErr),
						slog.String("dns_zone", dnsZone),
						slog.Int("vm_count", len(vms)))
					return err // Return original error for consistency
				}
				l.Info("sync succeeded on retry after 404",
					slog.String("dns_zone", dnsZone))
			} else {
				l.Error("failed to sync DNS",
					slog.Any("error", err),
					slog.String("dns_zone", dnsZone),
					slog.Int("vm_count", len(vms)))
				return err
			}
		}
	}
	return nil
}

func (s *Service) ManageRecords(
	ctx context.Context, l *logger.Logger, input types.ManageRecordsDTO,
) error {

	if input.Zone == "" {
		l.Info("DNS zone is empty, assuming no records to manage")
		return nil
	}

	dnsProvider, ok := s.dnsRegistry.GetDNSProvider(input.Zone)
	if !ok {
		return errors.Newf("no DNS provider found for zone: %s", input.Zone)
	}

	// Run DeleteRecords first to avoid conflicts.
	if len(input.DeleteRecords) > 0 {
		l.Info("deleting DNS records",
			slog.String("zone", input.Zone),
			slog.Int("records_count", len(input.DeleteRecords)),
			slog.String("cluster_name", input.ClusterName),
		)

		recs := make([]string, 0, len(input.DeleteRecords))
		for a := range input.DeleteRecords {
			recs = append(recs, a)
		}

		// Run DeleteRecords for each VM entry to delete on the provider
		err := dnsProvider.DeletePublicRecordsByName(ctx, recs...)
		if err != nil {
			l.Error("failed to delete DNS records",
				slog.Any("error", err),
				slog.String("zone", input.Zone),
				slog.Int("records_count", len(input.DeleteRecords)),
				slog.String("cluster_name", input.ClusterName),
			)
			return err
		}
	}

	// Run CreateRecords for each VM on the provider
	if len(input.CreateRecords) > 0 {
		l.Info("creating DNS records",
			slog.String("zone", input.Zone),
			slog.Int("records_count", len(input.CreateRecords)),
			slog.String("cluster_name", input.ClusterName),
		)

		recs := make([]vm.DNSRecord, 0, len(input.CreateRecords))
		for a, ip := range input.CreateRecords {
			rec := vm.CreateDNSRecord(a, vm.A, ip, 60)
			rec.Public = true
			recs = append(recs, rec)
		}

		err := dnsProvider.CreateRecords(ctx, recs...)
		if err != nil {
			l.Error("failed to create DNS records",
				slog.Any("error", err),
				slog.String("zone", input.Zone),
				slog.Int("records_count", len(input.CreateRecords)),
				slog.String("cluster_name", input.ClusterName),
			)
			return err
		}
	}

	return nil
}

// is404Error checks if an error indicates a 404/not found response from gcloud.
// This typically occurs during sync operations when records are deleted externally
// between the time gcloud lists existing records and attempts to delete them.
func is404Error(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "HTTPError 404") ||
		strings.Contains(errStr, "does not exist") ||
		strings.Contains(errStr, "was not found") ||
		strings.Contains(errStr, "NOT_FOUND")
}
