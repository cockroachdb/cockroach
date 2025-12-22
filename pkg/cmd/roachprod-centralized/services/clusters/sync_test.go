// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"testing"
	"time"

	clustersrepmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/mocks"
	healthmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestServiceSyncReturnsExistingClustersWhenLockHeld(t *testing.T) {
	repo := clustersrepmock.NewIClustersRepository(t)
	health := healthmock.NewIHealthService(t)

	instanceID := "instance-1"
	healthTimeout := 5 * time.Second
	expectedClusters := cloudcluster.Clusters{
		"cluster-a": {Name: "cluster-a"},
	}

	health.On("GetInstanceID").Return(instanceID)
	health.On("GetInstanceTimeout").Return(healthTimeout)
	repo.On(
		"AcquireSyncLockWithHealthCheck",
		mock.Anything,
		logger.DefaultLogger,
		instanceID,
		healthTimeout,
	).Return(false, nil).Once()
	repo.On(
		"GetClusters",
		mock.Anything,
		logger.DefaultLogger,
		mock.Anything,
	).Return(expectedClusters, nil).Once()

	service := &Service{
		_store:         repo,
		_healthService: health,
	}

	got, err := service.Sync(context.Background(), logger.DefaultLogger)
	require.NoError(t, err)
	require.Equal(t, expectedClusters, got)
}
