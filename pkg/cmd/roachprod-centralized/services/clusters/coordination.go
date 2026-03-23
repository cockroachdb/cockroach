// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/clusters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/clusters/internal/operations"
	dnstasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// operationToOperationData converts an IOperation to OperationData for storage.
func (s *Service) operationToOperationData(
	op operations.IOperation,
) (clusters.OperationData, error) {
	var opType clusters.OperationType
	var cluster cloudcluster.Cluster

	switch o := op.(type) {
	case operations.OperationCreate:
		opType = clusters.OperationTypeCreate
		cluster = o.Cluster
	case operations.OperationUpdate:
		opType = clusters.OperationTypeUpdate
		cluster = o.Cluster
	case operations.OperationDelete:
		opType = clusters.OperationTypeDelete
		cluster = o.Cluster
	default:
		return clusters.OperationData{}, fmt.Errorf("unknown operation type: %T", op)
	}

	clusterData, err := json.Marshal(cluster)
	if err != nil {
		return clusters.OperationData{}, errors.Wrap(err, "failed to marshal cluster data")
	}

	return clusters.OperationData{
		Type:        opType,
		ClusterName: cluster.Name,
		ClusterData: clusterData,
		Timestamp:   timeutil.Now(),
	}, nil
}

// operationDataToOperation converts OperationData back to an IOperation.
func (s *Service) operationDataToOperation(
	opData clusters.OperationData,
) (operations.IOperation, error) {
	var cluster cloudcluster.Cluster
	if err := json.Unmarshal(opData.ClusterData, &cluster); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal cluster data")
	}

	switch opData.Type {
	case clusters.OperationTypeCreate:
		return operations.OperationCreate{Cluster: cluster}, nil
	case clusters.OperationTypeUpdate:
		return operations.OperationUpdate{Cluster: cluster}, nil
	case clusters.OperationTypeDelete:
		return operations.OperationDelete{Cluster: cluster}, nil
	default:
		return nil, fmt.Errorf("unknown operation type: %s", opData.Type)
	}
}

func (s *Service) maybeEnqueuePublicDNSSyncTaskService(
	ctx context.Context, l *logger.Logger,
) error {
	if s._taskService == nil {
		return nil
	}

	// Create a task to sync the public DNS.
	_, err := s._taskService.CreateTaskIfNotAlreadyPlanned(ctx, l, dnstasks.NewTaskSync())
	if err != nil {
		return err
	}
	return nil
}

// conditionalEnqueueOperationWithHealthCheck enqueues operation with health verification.
// The sync lock and health check are performed atomically at the repository level
// to avoid race conditions.
func (s *Service) conditionalEnqueueOperationWithHealthCheck(
	ctx context.Context, l *logger.Logger, operation clusters.OperationData,
) (bool, error) {
	return s._store.ConditionalEnqueueOperation(ctx, l, operation, s._healthService.GetInstanceTimeout())
}

func (s *Service) enqueueManageDNSRecordTask(
	ctx context.Context,
	l *logger.Logger,
	clusterName, dnsZone string,
	createRecords, deleteRecords map[string]string,
) error {

	if s._taskService == nil {
		l.Info("no task service configured, skipping public DNS records management task enqueue")
		return nil
	}

	// Skip task creation if no DNS changes are needed.
	// This reduces task system pressure for operations that don't affect DNS (e.g., tag updates).
	if len(createRecords) == 0 && len(deleteRecords) == 0 {
		l.Info(
			"no DNS record changes needed, skipping task creation",
			slog.String("cluster", clusterName),
		)
		return nil
	}

	// Validate DNS zone is set
	if dnsZone == "" {
		l.Warn(
			"DNS zone is empty, cannot create DNS management task",
			slog.String("cluster", clusterName),
		)
		return errors.New("DNS zone is required for DNS management tasks")
	}

	// Validate all DNS names are non-empty
	for dns := range createRecords {
		if dns == "" {
			l.Warn(
				"empty DNS name in createRecords, skipping task creation",
				slog.String("cluster", clusterName),
			)
			return errors.New("DNS names cannot be empty")
		}
	}
	for dns := range deleteRecords {
		if dns == "" {
			l.Warn(
				"empty DNS name in deleteRecords, skipping task creation",
				slog.String("cluster", clusterName),
			)
			return errors.New("DNS names cannot be empty")
		}
	}

	task, err := dnstasks.NewTaskManageRecords(
		clusterName, dnsZone,
		createRecords, deleteRecords,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create public DNS records creation task")
	}

	// We enqueue the task if not already planned.
	_, err = s._taskService.CreateTask(ctx, l, task)
	if err != nil {
		return errors.Wrap(err, "failed to enqueue public DNS records creation task")
	}

	return nil
}

func (s *Service) computeDNSRecordChanges(
	ctx context.Context, l *logger.Logger, oldCluster, newCluster cloudcluster.Cluster,
) (dnsZone string, createRecords, deleteRecords map[string]string) {

	dnsZone = ""
	createRecords = make(map[string]string)
	deleteRecords = make(map[string]string)

	// Build maps for efficient comparison.
	oldVMsByDNS := make(map[string]string)
	newVMsByDNS := make(map[string]string)

	for _, v := range oldCluster.VMs {
		if v.PublicIP != "" && v.PublicDNS != "" {
			oldVMsByDNS[v.PublicDNS] = v.PublicIP
			if dnsZone == "" {
				dnsZone = v.PublicDNSZone
			}
		}
	}

	for _, v := range newCluster.VMs {
		if v.PublicIP != "" && v.PublicDNS != "" {
			newVMsByDNS[v.PublicDNS] = v.PublicIP
			if dnsZone == "" {
				dnsZone = v.PublicDNSZone
			}
		}
	}

	// Only add to createRecords if the IP actually changed or DNS name is new.
	// This avoids unnecessary DNS operations for node restarts, tag updates, etc.
	for dns, newIP := range newVMsByDNS {
		if oldIP, exists := oldVMsByDNS[dns]; !exists || oldIP != newIP {
			createRecords[dns] = newIP
		}
	}

	// Delete DNS records that are no longer present in the new cluster.
	for dns, ip := range oldVMsByDNS {
		if _, exists := newVMsByDNS[dns]; !exists {
			deleteRecords[dns] = ip
		}
	}

	return
}

func (s *Service) GetAllDNSZoneVMs(
	ctx context.Context, l *logger.Logger, dnsZone string,
) (vm.List, error) {
	c, err := s._store.GetClusters(ctx, l, *filters.NewFilterSet())
	if err != nil {
		return nil, err
	}

	var vms vm.List
	for _, cluster := range c {
		for _, vm := range cluster.VMs {
			if vm.PublicDNSZone != dnsZone {
				continue
			}
			vms = append(vms, vm)
		}
	}

	return vms, nil
}
