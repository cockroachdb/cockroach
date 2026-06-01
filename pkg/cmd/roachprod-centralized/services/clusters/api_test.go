// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"fmt"
	"testing"
	"time"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	clustersrepo "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	clustersrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/types"
	healthmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/mocks"
	tasksmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	stasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Tests for API operations (CRUD methods)

func TestService_GetAllClusters(t *testing.T) {
	tests := []struct {
		name     string
		input    types.InputGetAllClustersDTO
		mockFunc func(*clustersrepomock.IClustersRepository)
		want     cloudcluster.Clusters
		wantErr  bool
	}{
		{
			name:  "success - no filter",
			input: types.NewInputGetAllClustersDTO(),
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetClusters", mock.Anything, mock.Anything, *filters.NewFilterSet()).Return(cloudcluster.Clusters{
					"test-1": &cloudcluster.Cluster{Name: "test-1"},
					"test-2": &cloudcluster.Cluster{Name: "test-2"},
				}, nil)
			},
			want: cloudcluster.Clusters{
				"test-1": &cloudcluster.Cluster{Name: "test-1"},
				"test-2": &cloudcluster.Cluster{Name: "test-2"},
			},
		},
		{
			name: "success - with username filter",
			input: types.InputGetAllClustersDTO{
				Filters: *filters.NewFilterSet().AddFilter("Name", filtertypes.OpLike, "user1"),
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetClusters",
					mock.Anything,
					mock.Anything,
					*filters.NewFilterSet().AddFilter("Name", filtertypes.OpLike, "user1"),
				).Return(cloudcluster.Clusters{
					"user1-test": &cloudcluster.Cluster{Name: "user1-test"},
				}, nil)
			},
			want: cloudcluster.Clusters{
				"user1-test": &cloudcluster.Cluster{Name: "user1-test"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(store)
			}

			healthService := healthmock.NewIHealthService(t)
			s, err := NewService(store, nil, healthService, Options{})
			assert.NoError(t, err)
			got, err := s.GetAllClusters(context.Background(), &logger.Logger{}, makeTestPrincipal(), tt.input)

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
		input    types.InputGetClusterDTO
		mockFunc func(*clustersrepomock.IClustersRepository)
		want     *cloudcluster.Cluster
		wantErr  error
	}{
		{
			name: "success",
			input: types.InputGetClusterDTO{
				Name: "test-1",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, mock.Anything, "test-1").Return(
					cloudcluster.Cluster{Name: "test-1"}, nil,
				)
			},
			want: &cloudcluster.Cluster{Name: "test-1"},
		},
		{
			name: "not found",
			input: types.InputGetClusterDTO{
				Name: "nonexistent",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, mock.Anything, "nonexistent").Return(
					cloudcluster.Cluster{}, types.ErrClusterNotFound,
				)
			},
			wantErr: types.ErrClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(store)
			}

			healthService := healthmock.NewIHealthService(t)
			s, err := NewService(store, nil, healthService, Options{})
			assert.NoError(t, err)

			got, err := s.GetCluster(context.Background(), &logger.Logger{}, makeTestPrincipal(), tt.input)

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
		input    types.InputRegisterClusterDTO
		mockFunc func(*clustersrepomock.IClustersRepository)
		want     *cloudcluster.Cluster
		wantErr  error
	}{
		{
			name: "success",
			input: types.InputRegisterClusterDTO{
				Cluster: cloudcluster.Cluster{Name: "new-cluster"},
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, mock.Anything, "new-cluster").Return(
					cloudcluster.Cluster{}, types.ErrClusterNotFound,
				)
				repo.On("StoreCluster", mock.Anything, mock.Anything, mock.MatchedBy(func(c cloudcluster.Cluster) bool {
					return c.Name == "new-cluster"
				})).Return(nil)
				// Mock the atomic conditional enqueue (returns false = no sync in progress)
				repo.On("ConditionalEnqueueOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil)
			},
			want: &cloudcluster.Cluster{Name: "new-cluster", User: "test@example.com"},
		},
		{
			name: "already exists",
			input: types.InputRegisterClusterDTO{
				Cluster: cloudcluster.Cluster{Name: "existing-cluster"},
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("GetCluster", mock.Anything, mock.Anything, "existing-cluster").Return(
					cloudcluster.Cluster{Name: "existing-cluster"}, nil,
				)
			},
			wantErr: types.ErrClusterAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(store)
			}

			healthService := healthmock.NewIHealthService(t)
			// Mock GetInstanceTimeout (called for conditional enqueue in success path)
			if tt.wantErr == nil {
				healthService.On("GetInstanceTimeout").Return(3 * time.Second)
			}

			s, err := NewService(store, nil, healthService, Options{})
			assert.NoError(t, err)

			got, err := s.RegisterCluster(context.Background(), logger.DefaultLogger, makeTestPrincipal(), tt.input)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)

			store.AssertExpectations(t)
			healthService.AssertExpectations(t)
			store.ExpectedCalls = nil
		})
	}
}

func TestService_SyncClouds(t *testing.T) {
	taskService := tasksmock.NewIService(t)
	taskService.On("CreateTaskIfNotAlreadyPlanned", mock.Anything, mock.Anything, mock.MatchedBy(func(task stasks.ITask) bool {
		return task.(*tasks.TaskSync).Type == string(tasks.ClustersTaskSync)
	})).Return(&tasks.TaskSync{}, nil)

	store := clustersrepomock.NewIClustersRepository(t)
	healthService := healthmock.NewIHealthService(t)
	service, err := NewService(store, taskService, healthService, Options{})
	require.NoError(t, err)

	task, err := service.SyncClouds(context.Background(), logger.DefaultLogger, nil)
	require.NoError(t, err)
	assert.NotNil(t, task)

	store.AssertExpectations(t)
	store.ExpectedCalls = nil
}

// makeTestPrincipal creates a principal with wildcard permissions for testing.
// This simulates a super-admin user that bypasses all authorization checks.
func makeTestPrincipal() *pkgauth.Principal {
	userID := uuid.MakeV4()
	return &pkgauth.Principal{
		Token: pkgauth.TokenInfo{
			ID:   uuid.MakeV4(),
			Type: authmodels.TokenTypeUser,
		},
		UserID: &userID,
		User: &authmodels.User{
			ID:    userID,
			Email: "test@example.com",
		},
		Permissions: []authmodels.Permission{
			&authmodels.UserPermission{
				Scope:      "*",
				Permission: "*", // Wildcard grants access to everything
			},
		},
	}
}

func TestService_RegisterClusterInternal(t *testing.T) {
	t.Run("idempotent skip when cluster exists", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		store.On("GetCluster", mock.Anything, mock.Anything, "existing-cluster").Return(
			cloudcluster.Cluster{Name: "existing-cluster"}, nil,
		)

		s, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = s.RegisterClusterInternal(
			context.Background(), logger.DefaultLogger,
			cloudcluster.Cluster{Name: "existing-cluster"},
		)
		assert.NoError(t, err)

		// StoreCluster should NOT have been called.
		store.AssertNotCalled(t, "StoreCluster", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("store lookup error propagates", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		store.On("GetCluster", mock.Anything, mock.Anything, "some-cluster").Return(
			cloudcluster.Cluster{}, fmt.Errorf("db connection error"),
		)

		s, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = s.RegisterClusterInternal(
			context.Background(), logger.DefaultLogger,
			cloudcluster.Cluster{Name: "some-cluster"},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db connection error")
		assert.Contains(t, err.Error(), "check cluster existence")
	})
}

func TestService_UnregisterClusterInternal(t *testing.T) {
	t.Run("not found skips silently", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		store.On("GetCluster", mock.Anything, mock.Anything, "gone-cluster").Return(
			cloudcluster.Cluster{}, clustersrepo.ErrClusterNotFound,
		)

		s, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = s.UnregisterClusterInternal(
			context.Background(), logger.DefaultLogger,
			"gone-cluster", "owner@example.com", "prov-id",
		)
		assert.NoError(t, err)

		store.AssertNotCalled(t, "DeleteCluster", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("owner mismatch returns error", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		store.On("GetCluster", mock.Anything, mock.Anything, "my-cluster").Return(
			cloudcluster.Cluster{
				Name: "my-cluster",
				User: "alice@example.com",
				VMs: vm.List{{
					Name:   "vm-1",
					Labels: map[string]string{vm.TagProvisioningIdentifier: "prov-id"},
				}},
			}, nil,
		)

		s, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = s.UnregisterClusterInternal(
			context.Background(), logger.DefaultLogger,
			"my-cluster", "bob@example.com", "prov-id",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "owner mismatch")

		store.AssertNotCalled(t, "DeleteCluster", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("VM label mismatch returns error", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		store.On("GetCluster", mock.Anything, mock.Anything, "my-cluster").Return(
			cloudcluster.Cluster{
				Name: "my-cluster",
				User: "owner@example.com",
				VMs: vm.List{{
					Name:   "vm-1",
					Labels: map[string]string{vm.TagProvisioningIdentifier: "other-prov"},
				}},
			}, nil,
		)

		s, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = s.UnregisterClusterInternal(
			context.Background(), logger.DefaultLogger,
			"my-cluster", "owner@example.com", "my-prov",
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "owner label mismatch")

		store.AssertNotCalled(t, "DeleteCluster", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("owner and labels match applies delete", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		cluster := cloudcluster.Cluster{
			Name: "my-cluster",
			User: "owner@example.com",
			VMs: vm.List{{
				Name:   "vm-1",
				Labels: map[string]string{vm.TagProvisioningIdentifier: "prov-id"},
			}},
		}
		store.On("GetCluster", mock.Anything, mock.Anything, "my-cluster").Return(cluster, nil)
		store.On("DeleteCluster", mock.Anything, mock.Anything, mock.MatchedBy(func(c cloudcluster.Cluster) bool {
			return c.Name == "my-cluster"
		})).Return(nil)
		store.On("ConditionalEnqueueOperation", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(false, nil)
		healthService.On("GetInstanceTimeout").Return(3 * time.Second)

		s, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = s.UnregisterClusterInternal(
			context.Background(), logger.DefaultLogger,
			"my-cluster", "owner@example.com", "prov-id",
		)
		assert.NoError(t, err)

		store.AssertCalled(t, "DeleteCluster", mock.Anything, mock.Anything, mock.Anything)
	})
}

func TestService_RegisterClusterDelete_ManagedCluster(t *testing.T) {
	store := clustersrepomock.NewIClustersRepository(t)
	healthService := healthmock.NewIHealthService(t)

	managedCluster := cloudcluster.Cluster{
		Name:                  "managed-cluster",
		ManagedByProvisioning: true,
		VMs: vm.List{{
			Name:   "vm-1",
			Labels: map[string]string{vm.TagProvisioningIdentifier: "prov-abc"},
		}},
	}
	store.On("GetCluster", mock.Anything, mock.Anything, "managed-cluster").Return(managedCluster, nil)

	s, err := NewService(store, nil, healthService, Options{})
	require.NoError(t, err)

	err = s.RegisterClusterDelete(
		context.Background(), logger.DefaultLogger, makeTestPrincipal(),
		types.InputRegisterClusterDeleteDTO{Name: "managed-cluster"},
	)
	assert.ErrorIs(t, err, types.ErrClusterManagedByProvisioning)

	store.AssertNotCalled(t, "DeleteCluster", mock.Anything, mock.Anything, mock.Anything)
}

func TestService_RegisterClusterUpdate_ManagedCluster(t *testing.T) {
	store := clustersrepomock.NewIClustersRepository(t)
	healthService := healthmock.NewIHealthService(t)

	managedCluster := cloudcluster.Cluster{
		Name:                  "managed-cluster",
		User:                  "test@example.com",
		ManagedByProvisioning: true,
		VMs: vm.List{{
			Name:   "vm-1",
			Labels: map[string]string{vm.TagProvisioningIdentifier: "prov-abc"},
		}},
	}
	store.On("GetCluster", mock.Anything, mock.Anything, "managed-cluster").Return(managedCluster, nil)

	s, err := NewService(store, nil, healthService, Options{})
	require.NoError(t, err)

	_, err = s.RegisterClusterUpdate(
		context.Background(), logger.DefaultLogger, makeTestPrincipal(),
		types.InputRegisterClusterUpdateDTO{Cluster: cloudcluster.Cluster{Name: "managed-cluster"}},
	)
	assert.ErrorIs(t, err, types.ErrClusterManagedByProvisioning)

	store.AssertNotCalled(t, "StoreCluster", mock.Anything, mock.Anything, mock.Anything)
}
