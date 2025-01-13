// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"testing"
	"time"

	clustersrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/mocks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks"
	tasksmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestService_GetAllClusters(t *testing.T) {
	tests := []struct {
		name     string
		input    InputGetAllClustersDTO
		mockFunc func(*clustersrepomock.IClustersRepository)
		want     cloud.Clusters
		wantErr  bool
	}{
		{
			name: "success - no filter",
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetClusters", mock.Anything).Return(cloud.Clusters{
					"test-1": &cloud.Cluster{Name: "test-1"},
					"test-2": &cloud.Cluster{Name: "test-2"},
				}, nil)
			},
			want: cloud.Clusters{
				"test-1": &cloud.Cluster{Name: "test-1"},
				"test-2": &cloud.Cluster{Name: "test-2"},
			},
		},
		{
			name: "success - with username filter",
			input: InputGetAllClustersDTO{
				Username: "user1",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetClusters", mock.Anything).Return(cloud.Clusters{
					"user1-test": &cloud.Cluster{Name: "user1-test"},
					"user2-test": &cloud.Cluster{Name: "user2-test"},
				}, nil)
			},
			want: cloud.Clusters{
				"user1-test": &cloud.Cluster{Name: "user1-test"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(store)
			}

			s := NewService(store, nil, Options{})
			got, err := s.GetAllClusters(context.Background(), &utils.Logger{}, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// Assert the mock service calls and reset
			store.AssertExpectations(t)
			store.ExpectedCalls = nil
		})
	}
}

func TestService_GetCluster(t *testing.T) {
	tests := []struct {
		name     string
		input    InputGetClusterDTO
		mockFunc func(*clustersrepomock.IClustersRepository)
		want     *cloud.Cluster
		wantErr  error
	}{
		{
			name: "success",
			input: InputGetClusterDTO{
				Name: "test-1",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, "test-1").Return(
					cloud.Cluster{Name: "test-1"}, nil,
				)
			},
			want: &cloud.Cluster{Name: "test-1"},
		},
		{
			name: "not found",
			input: InputGetClusterDTO{
				Name: "nonexistent",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, "nonexistent").Return(
					cloud.Cluster{}, ErrClusterNotFound,
				)
			},
			wantErr: ErrClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(store)
			}

			s := NewService(store, nil, Options{})
			got, err := s.GetCluster(context.Background(), &utils.Logger{}, tt.input)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)

			store.AssertExpectations(t)
			store.ExpectedCalls = nil
		})
	}
}

func TestService_CreateCluster(t *testing.T) {
	tests := []struct {
		name     string
		input    InputCreateClusterDTO
		mockFunc func(*clustersrepomock.IClustersRepository)
		want     *cloud.Cluster
		wantErr  error
	}{
		{
			name: "success",
			input: InputCreateClusterDTO{
				Cluster: cloud.Cluster{Name: "new-cluster"},
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, "new-cluster").Return(
					cloud.Cluster{}, ErrClusterNotFound,
				)
				repo.On("StoreCluster", mock.Anything, mock.MatchedBy(func(c cloud.Cluster) bool {
					return c.Name == "new-cluster"
				})).Return(nil)
			},
			want: &cloud.Cluster{Name: "new-cluster"},
		},
		{
			name: "already exists",
			input: InputCreateClusterDTO{
				Cluster: cloud.Cluster{Name: "existing-cluster"},
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, "existing-cluster").Return(
					cloud.Cluster{Name: "existing-cluster"}, nil,
				)
			},
			wantErr: ErrClusterAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(store)
			}

			s := NewService(store, nil, Options{})
			got, err := s.CreateCluster(context.Background(), &utils.Logger{}, tt.input)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)

			store.AssertExpectations(t)
			store.ExpectedCalls = nil
		})
	}
}

func TestService_Shutdown(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func(*Service)
		ctxTimeout  time.Duration
		wantErr     error
		wantTimeout bool
	}{
		{
			name:       "clean shutdown",
			ctxTimeout: 1 * time.Second,
		},
		{
			name: "shutdown timeout",
			setupFunc: func(s *Service) {
				s.backgroundJobsWg.Add(1)
				go func() {
					time.Sleep(2 * time.Second)
					s.backgroundJobsWg.Done()
				}()
			},
			ctxTimeout: 100 * time.Millisecond,
			wantErr:    ErrShutdownTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			s := NewService(store, nil, Options{})

			if tt.setupFunc != nil {
				tt.setupFunc(s)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.ctxTimeout)
			defer cancel()

			err := s.Shutdown(ctx)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)

			store.AssertExpectations(t)
			store.ExpectedCalls = nil
		})
	}
}

func TestService_SyncClouds(t *testing.T) {
	taskService := tasksmock.NewIService(t)
	taskService.On("CreateTaskIfNotAlreadyPlanned", mock.Anything, mock.Anything, mock.MatchedBy(func(task stasks.ITask) bool {
		return task.(*TaskSync).Type == string(ClustersTaskSync)
	})).Return(&TaskSync{}, nil)

	store := clustersrepomock.NewIClustersRepository(t)
	service := NewService(store, taskService, Options{})

	task, err := service.SyncClouds(context.Background(), &utils.Logger{})
	require.NoError(t, err)
	assert.NotNil(t, task)

	store.AssertExpectations(t)
	store.ExpectedCalls = nil
}

func TestService_StartBackgroundWork(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr bool
	}{
		{
			name: "success with periodic refresh enabled",
			options: Options{
				NoInitialSync:           true,
				PeriodicRefreshEnabled:  true,
				PeriodicRefreshInterval: 50 * time.Millisecond,
			},
		},
		{
			name: "success with periodic refresh disabled",
			options: Options{
				NoInitialSync:          true,
				PeriodicRefreshEnabled: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if !tt.options.NoInitialSync {
				store.On("StoreClusters", mock.Anything, mock.Anything).Return(nil)
			}

			taskService := tasksmock.NewIService(t)
			taskService.On("RegisterTasksService", mock.Anything).Return()

			service := NewService(store, taskService, tt.options)

			errChan := make(chan error, 1)
			err := service.StartBackgroundWork(context.Background(), utils.DefaultLogger, errChan)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			store.AssertExpectations(t)
			store.ExpectedCalls = nil

			taskService.AssertExpectations(t)
			taskService.ExpectedCalls = nil

			// Cleanup
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_ = service.Shutdown(ctx)
		})
	}
}
