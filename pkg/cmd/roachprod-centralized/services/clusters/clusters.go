// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"container/list"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultPeriodicRefreshInterval is the default interval at which the clusters are refreshed.
	DefaultPeriodicRefreshInterval = 10 * time.Minute
)

var (
	once sync.Once

	// ErrClusterNotFound is the error returned when a cluster is not found.
	ErrClusterNotFound = utils.NewPublicError(fmt.Errorf("cluster not found"))
	// ErrClusterAlreadyExists is the error returned when a cluster already
	// exists.
	ErrClusterAlreadyExists = utils.NewPublicError(fmt.Errorf("cluster already exists"))
	// ErrShutdownTimeout is the error returned when the service shutdown times out.
	ErrShutdownTimeout = fmt.Errorf("service shutdown timeout")
)

// IService is the interface for the clusters service.
type IService interface {
	SyncClouds(context.Context, *utils.Logger) (tasks.ITask, error)
	GetAllClusters(context.Context, *utils.Logger, InputGetAllClustersDTO) (cloud.Clusters, error)
	GetCluster(context.Context, *utils.Logger, InputGetClusterDTO) (*cloud.Cluster, error)
	CreateCluster(context.Context, *utils.Logger, InputCreateClusterDTO) (*cloud.Cluster, error)
	UpdateCluster(context.Context, *utils.Logger, InputUpdateClusterDTO) (*cloud.Cluster, error)
	DeleteCluster(context.Context, *utils.Logger, InputDeleteClusterDTO) error
}

// Service is the implementation of the clusters service.
type Service struct {
	options Options

	backgroundJobsCancelFunc context.CancelFunc
	backgroundJobsWg         *sync.WaitGroup

	_taskService        stasks.IService
	_store              clusters.IClustersRepository
	_operationsStackMux syncutil.Mutex
	_operationsStack    *list.List
	_syncing            bool
}

// Options contains the options for the clusters service.
type Options struct {
	PeriodicRefreshEnabled  bool
	PeriodicRefreshInterval time.Duration
	NoInitialSync           bool
}

// NewService creates a new clusters service.
func NewService(
	store clusters.IClustersRepository, tasksService stasks.IService, options Options,
) *Service {

	// Initialize the roachprod providers
	// only during the first instantiation of the service.
	once.Do(func() {
		_ = roachprod.InitProviders()
	})

	service := &Service{
		backgroundJobsWg: &sync.WaitGroup{},
		_taskService:     tasksService,
		_store:           store,
		_operationsStack: list.New(),
		options:          options,
	}

	return service
}

// Init initializes the service by making the initial clusters sync
// and starting the periodic refresh if enabled.
func (s *Service) StartBackgroundWork(
	ctx context.Context, l *utils.Logger, errChan chan<- error,
) error {

	// Create a new context without cancel because we prefer to properly
	// handle the cancellation of the context in the Shutdown method.
	ctx, s.backgroundJobsCancelFunc = context.WithCancel(context.WithoutCancel(ctx))

	// Initial sync
	if !s.options.NoInitialSync {
		_, err := s.sync(ctx, l)
		if err != nil {
			return err
		}
	}

	// If the service is registered with a tasks service, register the tasks.
	if s._taskService != nil {
		s._taskService.RegisterTasksService(s)

		// If the periodic refresh is enabled, start the periodic refresh.
		if s.options.PeriodicRefreshEnabled {
			l.Debug("periodic refresh is enabled")
			if s.options.PeriodicRefreshInterval == 0 {
				l.Debug(
					"using default periodic refresh interval",
					slog.Duration("interval", DefaultPeriodicRefreshInterval),
				)
				s.options.PeriodicRefreshInterval = DefaultPeriodicRefreshInterval
			}

			// Periodically create a sync task to sync the clouds.
			s.periodicRefresh(ctx, l, errChan)
		}
	}
	return nil
}

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
		return ErrShutdownTimeout
	case <-done:
		// If all workers finish successfully, return nil
		return nil
	}

}

// periodicRefresh is a routine that periodically refreshes the clusters.
func (s *Service) periodicRefresh(ctx context.Context, l *utils.Logger, errChan chan<- error) {

	logger := &utils.Logger{
		Logger: l.With("service", "clusters", "method", "periodicRefresh"),
	}

	logger.Info(
		"starting periodic refresh routine",
		slog.Duration("interval", s.options.PeriodicRefreshInterval),
	)

	s.backgroundJobsWg.Add(1)
	go func() {
		defer s.backgroundJobsWg.Done()
		for {
			select {
			case <-ctx.Done():
				logger.Debug("Stopping periodic refresh routine")
				return

			case <-time.After(s.options.PeriodicRefreshInterval):

				logger.Debug("Periodic refresh routine triggered")

				task, err := s.SyncClouds(ctx, logger)
				if err != nil {
					errChan <- errors.Wrap(err, "error in periodic refresh routine")
					continue
				}
				logger.Info(
					"Task created from periodic refresh routine",
					slog.String("task_id", task.GetID().String()),
				)

			}
		}
	}()
}

// SyncClouds creates a task to sync the clouds.
func (s *Service) SyncClouds(ctx context.Context, l *utils.Logger) (tasks.ITask, error) {

	// Create a task to sync the clouds.
	task := &TaskSync{}
	task.Type = string(ClustersTaskSync)

	// Save the task.
	return s._taskService.CreateTaskIfNotAlreadyPlanned(ctx, l, task)
}

// InputGetAllDTO is the data transfer object to get all clusters.
type InputGetAllClustersDTO struct {
	Username string `json:"username" binding:"omitempty,alphanum"`
}

// GetAll returns all clusters.
func (s *Service) GetAllClusters(
	ctx context.Context, l *utils.Logger, input InputGetAllClustersDTO,
) (cloud.Clusters, error) {

	c, err := s._store.GetClusters(ctx)
	if err != nil {
		return nil, err
	}

	if input.Username != "" {
		regexp, err := regexp.Compile(fmt.Sprintf("^%s-.*", input.Username))
		if err != nil {
			return nil, err
		}
		c = c.FilterByName(regexp)
	}

	return c, nil
}

// InputGetClusterDTO is the data transfer object to get a cluster.
type InputGetClusterDTO struct {
	Name string `json:"name" binding:"required"`
}

// GetCluster returns a cluster.
func (s *Service) GetCluster(
	ctx context.Context, l *utils.Logger, input InputGetClusterDTO,
) (*cloud.Cluster, error) {

	c, err := s._store.GetCluster(ctx, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, ErrClusterNotFound
		}
		return nil, err
	}

	return &c, nil
}

// InputCreateClusterDTO is the data transfer object to create a new cluster.
type InputCreateClusterDTO struct {
	cloud.Cluster
}

// CreateCluster creates a cluster.
func (s *Service) CreateCluster(
	ctx context.Context, l *utils.Logger, input InputCreateClusterDTO,
) (*cloud.Cluster, error) {

	// Check that the cluster does not already exist.
	_, err := s._store.GetCluster(ctx, input.Name)
	if err == nil {
		return nil, ErrClusterAlreadyExists
	}

	// Create the operation.
	op := OperationCreate{
		Cluster: input.Cluster,
	}

	// Apply the operation on the current repository.
	err = op.applyOnRepository(ctx, s._store)
	if err != nil {
		return nil, err
	}

	// If we are syncing, we need to keep track of the operation
	// to be able to replay it at the end of the sync.
	if s._syncing {
		s.maybeEnqueueOperation(op)
	}

	// Return the created cluster.
	return &input.Cluster, nil
}

// InputUpdateClusterDTO is the data transfer object to update a cluster.
type InputUpdateClusterDTO struct {
	cloud.Cluster
}

// UpdateCluster updates a cluster.
func (s *Service) UpdateCluster(
	ctx context.Context, l *utils.Logger, input InputUpdateClusterDTO,
) (*cloud.Cluster, error) {

	// Check that the cluster exists.
	_, err := s._store.GetCluster(ctx, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return nil, ErrClusterNotFound
		}
		return nil, err
	}

	// Create the operation.
	op := OperationUpdate{
		Cluster: input.Cluster,
	}

	// Apply the operation on the current repository.
	err = op.applyOnRepository(ctx, s._store)
	if err != nil {
		return nil, err
	}

	s.maybeEnqueueOperation(OperationUpdate{
		Cluster: input.Cluster,
	})

	return &input.Cluster, nil
}

// InputDeleteClusterDTO is the data transfer object to delete a cluster.
type InputDeleteClusterDTO struct {
	Name string `json:"name" binding:"required"`
}

// DeleteCluster deletes a cluster.
func (s *Service) DeleteCluster(
	ctx context.Context, l *utils.Logger, input InputDeleteClusterDTO,
) error {

	// Check that the cluster exists.
	c, err := s._store.GetCluster(ctx, input.Name)
	if err != nil {
		if errors.Is(err, clusters.ErrClusterNotFound) {
			return ErrClusterNotFound
		}
		return err
	}

	// Create the operation.
	op := OperationDelete{
		Cluster: c,
	}

	// Apply the operation on the current repository.
	err = op.applyOnRepository(ctx, s._store)
	if err != nil {
		return err
	}

	s.maybeEnqueueOperation(OperationDelete{
		Cluster: c,
	})

	return nil
}

// sync contains the logic to sync the clusters and store them
// in the repository.
func (s *Service) sync(ctx context.Context, l *utils.Logger) (cloud.Clusters, error) {

	s._syncing = true
	defer func(s *Service) {
		l.Info("syncing clusters is over, releasing syncing flag")
		s._syncing = false
	}(s)

	crlLogger, err := (&logger.Config{Stdout: l, Stderr: l}).NewLogger("")
	if err != nil {
		return nil, err
	}

	l.Info("syncing clusters")
	c, err := cloud.ListCloud(crlLogger, vm.ListOptions{
		IncludeVolumes:       true,
		ComputeEstimatedCost: true,
		IncludeProviders:     []string{"aws", "gce", "azure"},
	})
	if err != nil {
		return nil, err
	}

	// Replay the operations stack
	l.Debug(
		"cloud listing done, replaying operations stack",
		slog.Int("operations", s._operationsStack.Len()),
	)
	for el := s._operationsStack.Front(); el != nil; el = el.Next() {

		// Perform the operation on the new refreshed clusters
		op := el.Value.(IOperation)
		err := op.applyOnStagingClusters(ctx, c.Clusters)
		if err != nil {
			l.Error("unable to replay operation", "operation", op, "error", err)
		}

		s.deleteStackedOperation(el)
	}

	l.Debug("storing new set of clusters in the repository")
	err = s._store.StoreClusters(ctx, c.Clusters)
	if err != nil {
		return nil, err
	}

	return c.Clusters, nil
}

// enqueueOperation enqueues an operation in the stack.
func (s *Service) maybeEnqueueOperation(op IOperation) {

	// If we are not syncing, we do not need to keep track of the operation
	// to replay it on the new set of data.
	if !s._syncing {
		return
	}

	s._operationsStackMux.Lock()
	defer s._operationsStackMux.Unlock()
	s._operationsStack.PushBack(op)
}

// deleteStackedOperation deletes an operation from the stack.
func (s *Service) deleteStackedOperation(el *list.Element) {
	s._operationsStackMux.Lock()
	defer s._operationsStackMux.Unlock()
	s._operationsStack.Remove(el)
}
