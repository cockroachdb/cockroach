// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/health"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	ccrdbstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/cockroachdb"
	cmemstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/memory"
	rhealth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/health"
	hcrdbstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/health/cockroachdb"
	hmemstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/health/memory"
	rtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks"
	tcrdbstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/cockroachdb"
	tmemstore "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/tasks/memory"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services"
	sclusters "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters"
	dnsregistry "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/dns/registry"
	shealth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health"
	spublicdns "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"
	crdbmigrator "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database/cockroachdb"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Services holds all the application services
type Services struct {
	Task     *stasks.Service
	Health   *shealth.Service
	Clusters *sclusters.Service
	DNS      *spublicdns.Service
}

// NewServicesFromConfig creates and initializes all services from configuration
func NewServicesFromConfig(
	cfg *configtypes.Config, l *logger.Logger, mode health.Mode,
) (*Services, error) {
	appCtx := context.Background()

	// Generate instance ID to be used by both task and health services
	instanceID := shealth.GenerateInstanceID()

	// Initialize repositories based on configuration.
	var clustersRepository clusters.IClustersRepository
	var tasksRepository rtasks.ITasksRepository
	var healthRepository rhealth.IHealthRepository

	switch strings.ToLower(cfg.Database.Type) {
	case "cockroachdb":
		// Create database connection
		db, err := database.NewConnection(appCtx, database.ConnectionConfig{
			URL:         cfg.Database.URL,
			MaxConns:    cfg.Database.MaxConns,
			MaxIdleTime: cfg.Database.MaxIdleTime,
		})
		if err != nil {
			l.Error("failed to connect to database",
				slog.Any("error", err),
				slog.String("database_type", cfg.Database.Type),
				slog.Int("max_conns", cfg.Database.MaxConns),
			)
			return nil, errors.Wrap(err, "error connecting to database")
		}

		// Run database migrations for tasks repository
		if err := database.RunMigrationsForRepository(appCtx, l, db, "tasks", tcrdbstore.GetTasksMigrations(), crdbmigrator.NewMigrator()); err != nil {
			l.Error("failed to run tasks migrations",
				slog.Any("error", err),
				slog.String("repository", "tasks"),
			)
			return nil, errors.Wrap(err, "error running tasks migrations")
		}

		// Run database migrations for health repository
		if err := database.RunMigrationsForRepository(appCtx, l, db, "health", hcrdbstore.GetHealthMigrations(), crdbmigrator.NewMigrator()); err != nil {
			l.Error("failed to run health migrations",
				slog.Any("error", err),
				slog.String("repository", "health"),
			)
			return nil, errors.Wrap(err, "error running health migrations")
		}

		// Run database migrations for clusters repository
		if err := database.RunMigrationsForRepository(appCtx, l, db, "clusters", ccrdbstore.GetClustersMigrations(), crdbmigrator.NewMigrator()); err != nil {
			l.Error("failed to run clusters migrations",
				slog.Any("error", err),
				slog.String("repository", "clusters"),
			)
			return nil, errors.Wrap(err, "error running clusters migrations")
		}

		healthRepository = hcrdbstore.NewHealthRepository(db)
		clustersRepository = ccrdbstore.NewClustersRepository(db)

		tasksRepository = tcrdbstore.NewTasksRepository(db, tcrdbstore.Options{
			HealthTimeout: time.Duration(cfg.InstanceHealthTimeoutSeconds) * time.Second,
		})

		// Register database connection pool metrics if enabled
		if cfg.Api.Metrics.Enabled {
			// Extract database name from connection URL
			dbName := "unknown"
			if parsedURL, err := url.Parse(cfg.Database.URL); err == nil && parsedURL.Path != "" {
				// Remove leading slash from path
				dbName = strings.TrimPrefix(parsedURL.Path, "/")
			}

			dbStatsCollector := collectors.NewDBStatsCollector(db, dbName)
			err = prometheus.Register(dbStatsCollector)
			if err != nil {
				l.Error("failed to register database metrics collector",
					slog.Any("error", err),
					slog.String("db_name", dbName),
					slog.String("database_type", cfg.Database.Type),
				)
				return nil, errors.Wrap(err, "error registering database metrics collector")
			}

			l.Info(
				"registered database metrics collector",
				slog.String("db_name", dbName),
				slog.String("database_type", cfg.Database.Type),
			)
		}

	case "memory", "":
		tasksRepository = tmemstore.NewTasksRepository()
		healthRepository = hmemstore.NewHealthRepository()
		clustersRepository = cmemstore.NewClustersRepository()

	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
	}

	// Create the DNS provider registry FIRST to break the circular dependency.
	// The registry creates DNS provider instances that will be shared between
	// the clusters service (for cloud provider DNS injection) and the public-dns
	// service (for DNS record management).
	dnsRegistry, err := dnsregistry.NewRegistry(l, cfg.DNSProviders)
	if err != nil {
		l.Error("failed to create DNS provider registry",
			slog.Any("error", err),
			slog.Int("dns_providers", len(cfg.DNSProviders)),
		)
		return nil, errors.Wrap(err, "error creating DNS provider registry")
	}

	// Create the task service.
	// This service is responsible for managing tasks, which are used to perform
	// operations like syncing clusters or DNS.
	// The service is used by other services to schedule and perform background tasks.
	taskService := stasks.NewService(
		tasksRepository,
		instanceID,
		stasks.Options{
			Workers:        cfg.Tasks.Workers,
			WorkersEnabled: cfg.Tasks.Workers > 0,
			CollectMetrics: cfg.Api.Metrics.Enabled,
		},
	)

	// Create the health service.
	// This service is responsible for tracking instance health and cleanup.
	healthService, err := shealth.NewService(
		healthRepository,
		taskService,
		instanceID,
		shealth.Options{
			HeartbeatInterval: time.Second,
			InstanceTimeout:   time.Duration(cfg.InstanceHealthTimeoutSeconds) * time.Second,
			CleanupInterval:   time.Hour,
			CleanupRetention:  time.Hour * 24,
			WorkersEnabled:    cfg.Tasks.Workers > 0,
			Mode:              mode,
		},
	)
	if err != nil {
		l.Error("failed to create health service",
			slog.Any("error", err),
			slog.String("database_type", cfg.Database.Type),
		)
		return nil, errors.Wrap(err, "error creating health service")
	}

	// Create the clusters service WITH DNS providers from the registry.
	// This allows cloud providers to manage DNS records for their clusters.
	clustersService, err := sclusters.NewService(
		clustersRepository,
		taskService,
		healthService,
		sclusters.Options{
			PeriodicRefreshEnabled: true,
			CloudProviders:         cfg.CloudProviders,
			DNSProviders:           dnsRegistry.GetAllDNSProviders(),
			WorkersEnabled:         cfg.Tasks.Workers > 0,
		},
	)
	if err != nil {
		l.Error("failed to create clusters service",
			slog.Any("error", err),
			slog.Int("cloud_providers", len(cfg.CloudProviders)),
			slog.String("database_type", cfg.Database.Type),
		)
		return nil, errors.Wrap(err, "error creating clusters service")
	}

	// Create the DNS service WITH the registry.
	// Now that clusters service is available, we can create public-dns service.
	// The registry provides DNS providers without recreating them.
	dnsService, err := spublicdns.NewService(
		clustersService,
		taskService,
		dnsRegistry,
		spublicdns.Options{
			WorkersEnabled: cfg.Tasks.Workers > 0,
		},
	)
	if err != nil {
		l.Error("failed to create DNS service",
			slog.Any("error", err),
			slog.Int("dns_providers", len(cfg.DNSProviders)),
			slog.String("database_type", cfg.Database.Type),
		)
		return nil, errors.Wrap(err, "error creating DNS service")
	}

	return &Services{
		Task:     taskService,
		Health:   healthService,
		Clusters: clustersService,
		DNS:      dnsService,
	}, nil
}

func (s *Services) ToSlice() []services.IService {
	return []services.IService{
		s.Task,
		s.Health,
		s.Clusters,
		s.DNS,
	}
}
