// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	clustersrepomock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/internal/operations"
	healthmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/health/mocks"
	dnstasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/tasks"
	tasksmock "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/tasks/mocks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Tests for operation conversion methods

func TestService_operationToOperationData(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	tests := []struct {
		name     string
		op       operations.IOperation
		wantType clusters.OperationType
		wantName string
	}{
		{
			name: "OperationCreate",
			op: operations.OperationCreate{
				Cluster: cloudcluster.Cluster{Name: "test-cluster"},
			},
			wantType: clusters.OperationTypeCreate,
			wantName: "test-cluster",
		},
		{
			name: "OperationUpdate",
			op: operations.OperationUpdate{
				Cluster: cloudcluster.Cluster{Name: "update-cluster"},
			},
			wantType: clusters.OperationTypeUpdate,
			wantName: "update-cluster",
		},
		{
			name: "OperationDelete",
			op: operations.OperationDelete{
				Cluster: cloudcluster.Cluster{Name: "delete-cluster"},
			},
			wantType: clusters.OperationTypeDelete,
			wantName: "delete-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opData, err := service.operationToOperationData(tt.op)

			require.NoError(t, err)
			assert.Equal(t, tt.wantType, opData.Type)
			assert.Equal(t, tt.wantName, opData.ClusterName)
			assert.NotEmpty(t, opData.ClusterData)
			assert.False(t, opData.Timestamp.IsZero())

			// Verify cluster data can be unmarshaled
			var cluster cloudcluster.Cluster
			err = json.Unmarshal(opData.ClusterData, &cluster)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, cluster.Name)
		})
	}

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_operationDataToOperation(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	cluster := cloudcluster.Cluster{Name: "test-cluster"}
	clusterData, err := json.Marshal(cluster)
	require.NoError(t, err)

	tests := []struct {
		name     string
		opData   clusters.OperationData
		wantType string // Type name for assertion
	}{
		{
			name: "OperationTypeCreate",
			opData: clusters.OperationData{
				Type:        clusters.OperationTypeCreate,
				ClusterName: "test-cluster",
				ClusterData: clusterData,
			},
			wantType: "operations.OperationCreate",
		},
		{
			name: "OperationTypeUpdate",
			opData: clusters.OperationData{
				Type:        clusters.OperationTypeUpdate,
				ClusterName: "test-cluster",
				ClusterData: clusterData,
			},
			wantType: "operations.OperationUpdate",
		},
		{
			name: "OperationTypeDelete",
			opData: clusters.OperationData{
				Type:        clusters.OperationTypeDelete,
				ClusterName: "test-cluster",
				ClusterData: clusterData,
			},
			wantType: "operations.OperationDelete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := service.operationDataToOperation(tt.opData)

			require.NoError(t, err)
			assert.NotNil(t, op)

			// Verify the operation type
			switch tt.opData.Type {
			case clusters.OperationTypeCreate:
				_, ok := op.(operations.OperationCreate)
				assert.True(t, ok, "expected OperationCreate")
			case clusters.OperationTypeUpdate:
				_, ok := op.(operations.OperationUpdate)
				assert.True(t, ok, "expected OperationUpdate")
			case clusters.OperationTypeDelete:
				_, ok := op.(operations.OperationDelete)
				assert.True(t, ok, "expected OperationDelete")
			}
		})
	}

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_operationDataToOperation_InvalidJSON(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	opData := clusters.OperationData{
		Type:        clusters.OperationTypeCreate,
		ClusterName: "test",
		ClusterData: []byte("invalid json"),
	}

	_, err := service.operationDataToOperation(opData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal cluster data")

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_operationDataToOperation_UnknownType(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	cluster := cloudcluster.Cluster{Name: "test"}
	clusterData, _ := json.Marshal(cluster)

	opData := clusters.OperationData{
		Type:        clusters.OperationType("unknown"),
		ClusterName: "test",
		ClusterData: clusterData,
	}

	_, err := service.operationDataToOperation(opData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation type")

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

// Tests for DNS sync task enqueueing

func TestService_maybeEnqueuePublicDNSSyncTaskService(t *testing.T) {
	t.Run("success with task service", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		taskService.On("CreateTaskIfNotAlreadyPlanned", mock.Anything, mock.Anything, mock.MatchedBy(func(task interface{}) bool {
			dnsTask, ok := task.(*dnstasks.TaskSync)
			return ok && dnsTask.Type == string(dnstasks.PublicDNSTaskSync)
		})).Return(&dnstasks.TaskSync{}, nil)

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.maybeEnqueuePublicDNSSyncTaskService(context.Background(), &logger.Logger{})
		assert.NoError(t, err)

		taskService.AssertExpectations(t)
	})

	t.Run("nil task service", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		service, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = service.maybeEnqueuePublicDNSSyncTaskService(context.Background(), &logger.Logger{})
		assert.NoError(t, err) // Should not error when task service is nil
	})

	t.Run("task creation error", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		taskService.On("CreateTaskIfNotAlreadyPlanned", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("task creation failed"))

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.maybeEnqueuePublicDNSSyncTaskService(context.Background(), &logger.Logger{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task creation failed")

		taskService.AssertExpectations(t)
	})
}

// Tests for conditional operation enqueueing

func TestService_conditionalEnqueueOperationWithHealthCheck(t *testing.T) {
	cluster := cloudcluster.Cluster{Name: "test"}
	clusterData, _ := json.Marshal(cluster)
	operation := clusters.OperationData{
		Type:        clusters.OperationTypeCreate,
		ClusterName: "test",
		ClusterData: clusterData,
	}
	healthTimeout := 3 * time.Second

	t.Run("no sync in progress", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		// Mock GetInstanceTimeout (called in the atomic operation)
		healthService.On("GetInstanceTimeout").Return(healthTimeout)
		// Atomic operation returns false (no sync in progress)
		store.On("ConditionalEnqueueOperation", mock.Anything, mock.Anything, operation, healthTimeout).
			Return(false, nil)

		service, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		enqueued, err := service.conditionalEnqueueOperationWithHealthCheck(
			context.Background(), &logger.Logger{}, operation,
		)

		assert.NoError(t, err)
		assert.False(t, enqueued)
		store.AssertExpectations(t)
		healthService.AssertExpectations(t)
	})

	t.Run("sync in progress, instance healthy, operation enqueued", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		// Mock GetInstanceTimeout (called in the atomic operation)
		healthService.On("GetInstanceTimeout").Return(healthTimeout)
		// Atomic operation returns true (sync in progress AND instance healthy)
		store.On("ConditionalEnqueueOperation", mock.Anything, mock.Anything, operation, healthTimeout).
			Return(true, nil)

		service, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		enqueued, err := service.conditionalEnqueueOperationWithHealthCheck(
			context.Background(), &logger.Logger{}, operation,
		)

		assert.NoError(t, err)
		assert.True(t, enqueued)
		store.AssertExpectations(t)
		healthService.AssertExpectations(t)
	})

	t.Run("sync in progress, instance unhealthy", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		// Mock GetInstanceTimeout (called in the atomic operation)
		healthService.On("GetInstanceTimeout").Return(healthTimeout)
		// Atomic operation returns false (instance unhealthy - health check done in DB)
		store.On("ConditionalEnqueueOperation", mock.Anything, mock.Anything, operation, healthTimeout).
			Return(false, nil)

		service, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		enqueued, err := service.conditionalEnqueueOperationWithHealthCheck(
			context.Background(), &logger.Logger{}, operation,
		)

		assert.NoError(t, err)
		assert.False(t, enqueued)
		store.AssertExpectations(t)
		healthService.AssertExpectations(t)
	})

	t.Run("database error", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		// Mock GetInstanceTimeout (called in the atomic operation)
		healthService.On("GetInstanceTimeout").Return(healthTimeout)
		// Atomic operation returns error
		store.On("ConditionalEnqueueOperation", mock.Anything, mock.Anything, operation, healthTimeout).
			Return(false, errors.New("database error"))

		service, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		_, err = service.conditionalEnqueueOperationWithHealthCheck(
			context.Background(), &logger.Logger{}, operation,
		)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database error")
		store.AssertExpectations(t)
		healthService.AssertExpectations(t)
	})
}

// Tests for DNS record change computation

func TestService_computeDNSRecordChanges(t *testing.T) {
	service, store, _, healthService := createTestService(t, Options{})

	t.Run("empty clusters - no changes", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{Name: "test"}
		newCluster := cloudcluster.Cluster{Name: "test"}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Empty(t, dnsZone)
		assert.Empty(t, createRecords)
		assert.Empty(t, deleteRecords)
	})

	t.Run("initial cluster creation - all creates", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{Name: "test"}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Len(t, createRecords, 3)
		assert.Equal(t, "1.2.3.4", createRecords["vm-0001.example.com"])
		assert.Equal(t, "5.6.7.8", createRecords["vm-0002.example.com"])
		assert.Equal(t, "9.10.11.12", createRecords["vm-0003.example.com"])
		assert.Empty(t, deleteRecords)
	})

	t.Run("cluster deletion - all deletes", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{Name: "test"}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Empty(t, createRecords)
		assert.Len(t, deleteRecords, 3)
		assert.Equal(t, "1.2.3.4", deleteRecords["vm-0001.example.com"])
		assert.Equal(t, "5.6.7.8", deleteRecords["vm-0002.example.com"])
		assert.Equal(t, "9.10.11.12", deleteRecords["vm-0003.example.com"])
	})

	t.Run("no IP changes - no operations (optimization)", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Empty(t, createRecords, "should skip creates when IPs unchanged")
		assert.Empty(t, deleteRecords)
	})

	t.Run("IP change for existing VM - update via create", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "13.14.15.16", PublicDNSZone: "example.com"}, // IP changed
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Len(t, createRecords, 1, "only changed IP should be in createRecords")
		assert.Equal(t, "13.14.15.16", createRecords["vm-0002.example.com"])
		assert.Empty(t, deleteRecords)
	})

	t.Run("cluster grow - new VM added", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"}, // new VM
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Len(t, createRecords, 1, "only new VM should be in createRecords")
		assert.Equal(t, "9.10.11.12", createRecords["vm-0003.example.com"])
		assert.Empty(t, deleteRecords)
	})

	t.Run("cluster shrink - VM removed", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Empty(t, createRecords)
		assert.Len(t, deleteRecords, 1, "removed VM should be in deleteRecords")
		assert.Equal(t, "9.10.11.12", deleteRecords["vm-0003.example.com"])
	})

	t.Run("VM replacement - different DNS name", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0004.example.com", PublicIP: "17.18.19.20", PublicDNSZone: "example.com"}, // replaced vm-0002
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Len(t, createRecords, 1)
		assert.Equal(t, "17.18.19.20", createRecords["vm-0004.example.com"])
		assert.Len(t, deleteRecords, 1)
		assert.Equal(t, "5.6.7.8", deleteRecords["vm-0002.example.com"])
	})

	t.Run("complex scenario - grow, shrink, IP change, and replacement", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0003.example.com", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"},
				{PublicDNS: "vm-0004.example.com", PublicIP: "13.14.15.16", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},     // unchanged
				{PublicDNS: "vm-0002.example.com", PublicIP: "21.22.23.24", PublicDNSZone: "example.com"}, // IP changed
				{PublicDNS: "vm-0005.example.com", PublicIP: "25.26.27.28", PublicDNSZone: "example.com"}, // replaced vm-0003
				{PublicDNS: "vm-0006.example.com", PublicIP: "29.30.31.32", PublicDNSZone: "example.com"}, // new VM (grow)
				{PublicDNS: "vm-0007.example.com", PublicIP: "33.34.35.36", PublicDNSZone: "example.com"}, // new VM (grow)
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)

		// Creates: IP change (vm-0002) + replacement (vm-0005) + 2 new VMs (vm-0006, vm-0007)
		assert.Len(t, createRecords, 4)
		assert.Equal(t, "21.22.23.24", createRecords["vm-0002.example.com"])
		assert.Equal(t, "25.26.27.28", createRecords["vm-0005.example.com"])
		assert.Equal(t, "29.30.31.32", createRecords["vm-0006.example.com"])
		assert.Equal(t, "33.34.35.36", createRecords["vm-0007.example.com"])

		// Deletes: replaced VM (vm-0003) + shrink (vm-0004)
		assert.Len(t, deleteRecords, 2)
		assert.Equal(t, "9.10.11.12", deleteRecords["vm-0003.example.com"])
		assert.Equal(t, "13.14.15.16", deleteRecords["vm-0004.example.com"])
	})

	t.Run("VMs without public IP are ignored", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "", PublicDNSZone: "example.com"}, // no IP
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"}, // now has IP
				{PublicDNS: "vm-0002.example.com", PublicIP: "", PublicDNSZone: "example.com"},        // IP removed
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Len(t, createRecords, 1)
		assert.Equal(t, "1.2.3.4", createRecords["vm-0001.example.com"])
		assert.Len(t, deleteRecords, 1)
		assert.Equal(t, "5.6.7.8", deleteRecords["vm-0002.example.com"])
	})

	t.Run("DNS zone fallback to old cluster when new is empty", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{Name: "test"}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone, "should get DNS zone from old cluster")
		assert.Empty(t, createRecords)
		assert.Len(t, deleteRecords, 1)
	})

	t.Run("empty DNS names are filtered out", func(t *testing.T) {
		oldCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"}, // empty DNS name
				{PublicDNS: "vm-0002.example.com", PublicIP: "5.6.7.8", PublicDNSZone: "example.com"},
			},
		}
		newCluster := cloudcluster.Cluster{
			Name: "test",
			VMs: vm.List{
				{PublicDNS: "vm-0001.example.com", PublicIP: "1.2.3.4", PublicDNSZone: "example.com"},
				{PublicDNS: "", PublicIP: "9.10.11.12", PublicDNSZone: "example.com"}, // empty DNS name
			},
		}

		dnsZone, createRecords, deleteRecords := service.computeDNSRecordChanges(
			context.Background(), logger.DefaultLogger, oldCluster, newCluster,
		)

		assert.Equal(t, "example.com", dnsZone)
		assert.Len(t, createRecords, 1, "empty DNS names should be filtered out")
		assert.Equal(t, "1.2.3.4", createRecords["vm-0001.example.com"])
		assert.Len(t, deleteRecords, 1, "empty DNS names should be filtered out")
		assert.Equal(t, "5.6.7.8", deleteRecords["vm-0002.example.com"])
	})

	store.AssertExpectations(t)
	healthService.AssertExpectations(t)
}

func TestService_enqueueManageDNSRecordTask(t *testing.T) {
	t.Run("success with changes", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		createRecords := map[string]string{"vm-0001.example.com": "1.2.3.4"}
		deleteRecords := map[string]string{"vm-0002.example.com": "5.6.7.8"}

		taskService.On("CreateTask", mock.Anything, mock.Anything, mock.MatchedBy(func(task interface{}) bool {
			dnsTask, ok := task.(*dnstasks.TaskManageRecords)
			return ok && dnsTask.Options.ClusterName == "test-cluster"
		})).Return(&dnstasks.TaskManageRecords{}, nil)

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "example.com",
			createRecords, deleteRecords,
		)

		assert.NoError(t, err)
		taskService.AssertExpectations(t)
	})

	t.Run("skip when no changes (optimization)", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		// Empty create and delete records - should skip task creation
		createRecords := map[string]string{}
		deleteRecords := map[string]string{}

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "example.com",
			createRecords, deleteRecords,
		)

		assert.NoError(t, err)
		// Verify CreateTask was NOT called
		taskService.AssertNotCalled(t, "CreateTask")
	})

	t.Run("nil task service", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		healthService := healthmock.NewIHealthService(t)

		createRecords := map[string]string{"vm-0001.example.com": "1.2.3.4"}
		deleteRecords := map[string]string{}

		service, err := NewService(store, nil, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "example.com",
			createRecords, deleteRecords,
		)

		assert.NoError(t, err) // Should not error when task service is nil
	})

	t.Run("task creation error", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		createRecords := map[string]string{"vm-0001.example.com": "1.2.3.4"}
		deleteRecords := map[string]string{}

		taskService.On("CreateTask", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("task creation failed"))

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "example.com",
			createRecords, deleteRecords,
		)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "task creation failed")
		taskService.AssertExpectations(t)
	})

	t.Run("empty DNS zone validation", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		createRecords := map[string]string{"vm-0001.example.com": "1.2.3.4"}
		deleteRecords := map[string]string{}

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "", // empty DNS zone
			createRecords, deleteRecords,
		)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DNS zone is required")
		taskService.AssertNotCalled(t, "CreateTask")
	})

	t.Run("empty DNS name in createRecords validation", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		createRecords := map[string]string{"": "1.2.3.4"} // empty DNS name
		deleteRecords := map[string]string{}

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "example.com",
			createRecords, deleteRecords,
		)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DNS names cannot be empty")
		taskService.AssertNotCalled(t, "CreateTask")
	})

	t.Run("empty DNS name in deleteRecords validation", func(t *testing.T) {
		store := clustersrepomock.NewIClustersRepository(t)
		taskService := tasksmock.NewIService(t)
		healthService := healthmock.NewIHealthService(t)

		createRecords := map[string]string{}
		deleteRecords := map[string]string{"": "1.2.3.4"} // empty DNS name

		service, err := NewService(store, taskService, healthService, Options{})
		require.NoError(t, err)

		err = service.enqueueManageDNSRecordTask(
			context.Background(), logger.DefaultLogger,
			"test-cluster", "example.com",
			createRecords, deleteRecords,
		)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DNS names cannot be empty")
		taskService.AssertNotCalled(t, "CreateTask")
	})
}
