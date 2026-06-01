// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"time"

	mtasks "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/public-dns/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

const (
	// PublicDNSTaskManageRecords is the task type for managing a cluster's
	// records in the public DNS.
	PublicDNSTaskManageRecords types.PublicDNSTaskType = types.TaskServiceName + "_manage_records"
)

// TaskRecordOptions contains configuration for create and delete records tasks.
type TaskRecordOptions struct {
	ClusterName   string
	DNSZone       string
	CreateRecords map[string]string // map of A record to public IP to create
	DeleteRecords map[string]string // map of A record to public IP to delete
}

// TaskManageRecords is the task that manages a cluster's records in the DNS.
type TaskManageRecords struct {
	mtasks.TaskWithOptions[TaskRecordOptions]
	Service types.IService
}

// NewTaskManageRecords creates a new TaskManageRecords instance.
func NewTaskManageRecords(
	clusterName, dnsZone string, createRecords, deleteRecords map[string]string,
) (*TaskManageRecords, error) {
	task := &TaskManageRecords{}
	task.Type = string(PublicDNSTaskManageRecords)
	if err := task.SetOptions(TaskRecordOptions{
		ClusterName:   clusterName,
		DNSZone:       dnsZone,
		CreateRecords: createRecords,
		DeleteRecords: deleteRecords,
	}); err != nil {
		return nil, err
	}
	return task, nil
}

// Process will create a cluster's records in the DNS.
func (t *TaskManageRecords) Process(ctx context.Context, l *logger.Logger) error {
	dto := types.ManageRecordsDTO{
		ClusterName:   t.GetOptions().ClusterName,
		Zone:          t.GetOptions().DNSZone,
		CreateRecords: t.GetOptions().CreateRecords,
		DeleteRecords: t.GetOptions().DeleteRecords,
	}

	return t.Service.ManageRecords(ctx, l, dto)
}

// ResolveConcurrencyKey disables default per-type locking for manage-records tasks
// so multiple record batches can run concurrently.
func (t *TaskManageRecords) ResolveConcurrencyKey() string {
	return ""
}

func (t *TaskManageRecords) GetTimeout() time.Duration {
	return time.Second * 90
}
