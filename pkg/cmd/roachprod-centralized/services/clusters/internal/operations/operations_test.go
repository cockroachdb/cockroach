// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"testing"

	clustersrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestOperationCreate_ApplyOnRepository(t *testing.T) {
	tests := []struct {
		name     string
		cluster  cloudcluster.Cluster
		mockFunc func(*clustersrepomock.IClustersRepository)
		wantErr  bool
	}{
		{
			name: "success",
			cluster: cloudcluster.Cluster{
				Name: "test-cluster",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("StoreCluster", mock.Anything, mock.Anything, mock.MatchedBy(func(c cloudcluster.Cluster) bool {
					return c.Name == "test-cluster"
				})).Return(nil)
			},
		},
		{
			name: "repository error",
			cluster: cloudcluster.Cluster{
				Name: "failing-cluster",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("StoreCluster", mock.Anything, mock.Anything, mock.Anything).Return(
					errors.New("database error"),
				)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(repo)
			}

			op := OperationCreate{Cluster: tt.cluster}
			err := op.ApplyOnRepository(context.Background(), &logger.Logger{}, repo)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			repo.AssertExpectations(t)
		})
	}
}

func TestOperationCreate_ApplyOnStagingClusters(t *testing.T) {
	cluster := cloudcluster.Cluster{Name: "new-cluster"}
	staging := cloudcluster.Clusters{}

	op := OperationCreate{Cluster: cluster}
	err := op.ApplyOnStagingClusters(context.Background(), &logger.Logger{}, staging)

	assert.NoError(t, err)
	assert.Contains(t, staging, "new-cluster")
}

func TestOperationUpdate_ApplyOnRepository(t *testing.T) {
	tests := []struct {
		name     string
		cluster  cloudcluster.Cluster
		mockFunc func(*clustersrepomock.IClustersRepository)
		wantErr  bool
	}{
		{
			name: "success",
			cluster: cloudcluster.Cluster{
				Name: "existing-cluster",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("StoreCluster", mock.Anything, mock.Anything, mock.MatchedBy(func(c cloudcluster.Cluster) bool {
					return c.Name == "existing-cluster"
				})).Return(nil)
			},
		},
		{
			name: "repository error",
			cluster: cloudcluster.Cluster{
				Name: "failing-cluster",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("StoreCluster", mock.Anything, mock.Anything, mock.Anything).Return(
					errors.New("update failed"),
				)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(repo)
			}

			op := OperationUpdate{Cluster: tt.cluster}
			err := op.ApplyOnRepository(context.Background(), &logger.Logger{}, repo)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			repo.AssertExpectations(t)
		})
	}
}

func TestOperationUpdate_ApplyOnStagingClusters(t *testing.T) {
	oldCluster := &cloudcluster.Cluster{Name: "test-cluster"}
	staging := cloudcluster.Clusters{"test-cluster": oldCluster}

	updatedCluster := cloudcluster.Cluster{Name: "test-cluster"}
	op := OperationUpdate{Cluster: updatedCluster}
	err := op.ApplyOnStagingClusters(context.Background(), &logger.Logger{}, staging)

	assert.NoError(t, err)
	assert.Contains(t, staging, "test-cluster")
}

func TestOperationDelete_ApplyOnRepository(t *testing.T) {
	tests := []struct {
		name     string
		cluster  cloudcluster.Cluster
		mockFunc func(*clustersrepomock.IClustersRepository)
		wantErr  bool
	}{
		{
			name: "success",
			cluster: cloudcluster.Cluster{
				Name: "cluster-to-delete",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("DeleteCluster", mock.Anything, mock.Anything, mock.MatchedBy(func(c cloudcluster.Cluster) bool {
					return c.Name == "cluster-to-delete"
				})).Return(nil)
			},
		},
		{
			name: "repository error",
			cluster: cloudcluster.Cluster{
				Name: "failing-cluster",
			},
			mockFunc: func(repo *clustersrepomock.IClustersRepository) {
				repo.On("DeleteCluster", mock.Anything, mock.Anything, mock.Anything).Return(
					errors.New("delete failed"),
				)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := clustersrepomock.NewIClustersRepository(t)
			if tt.mockFunc != nil {
				tt.mockFunc(repo)
			}

			op := OperationDelete{Cluster: tt.cluster}
			err := op.ApplyOnRepository(context.Background(), &logger.Logger{}, repo)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			repo.AssertExpectations(t)
		})
	}
}

func TestOperationDelete_ApplyOnStagingClusters(t *testing.T) {
	cluster := &cloudcluster.Cluster{Name: "cluster-to-delete"}
	staging := cloudcluster.Clusters{"cluster-to-delete": cluster, "other-cluster": {}}

	op := OperationDelete{Cluster: *cluster}
	err := op.ApplyOnStagingClusters(context.Background(), &logger.Logger{}, staging)

	assert.NoError(t, err)
	assert.NotContains(t, staging, "cluster-to-delete")
	assert.Contains(t, staging, "other-cluster")
}

// Test interface compliance
func TestOperationTypes_ImplementIOperation(t *testing.T) {
	var _ IOperation = OperationCreate{}
	var _ IOperation = OperationUpdate{}
	var _ IOperation = OperationDelete{}
}

func TestOperations_WithNilClusters(t *testing.T) {
	// Test that operations handle nil staging clusters gracefully
	t.Run("Create with nil map initializes it", func(t *testing.T) {
		cluster := cloudcluster.Cluster{Name: "test"}
		staging := cloudcluster.Clusters{}

		op := OperationCreate{Cluster: cluster}
		err := op.ApplyOnStagingClusters(context.Background(), &logger.Logger{}, staging)

		assert.NoError(t, err)
		assert.Contains(t, staging, "test")
	})

	t.Run("Update on empty map adds cluster", func(t *testing.T) {
		cluster := cloudcluster.Cluster{Name: "test"}
		staging := cloudcluster.Clusters{}

		op := OperationUpdate{Cluster: cluster}
		err := op.ApplyOnStagingClusters(context.Background(), &logger.Logger{}, staging)

		assert.NoError(t, err)
		assert.Contains(t, staging, "test")
	})

	t.Run("Delete on empty map is safe", func(t *testing.T) {
		cluster := cloudcluster.Cluster{Name: "nonexistent"}
		staging := cloudcluster.Clusters{}

		op := OperationDelete{Cluster: cluster}
		err := op.ApplyOnStagingClusters(context.Background(), &logger.Logger{}, staging)

		assert.NoError(t, err)
		assert.NotContains(t, staging, "nonexistent")
	})
}
