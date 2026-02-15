// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
)

const (
	TaskServiceName = "public-dns"
)

// PublicDNSTaskType is the type of the task.
type PublicDNSTaskType string

const (
	PermissionSync = TaskServiceName + ":sync"
)

// IService is the interface for the clusters service.
type IService interface {
	SyncDNS(context.Context, *logger.Logger) (tasks.ITask, error)
	Sync(ctx context.Context, l *logger.Logger) error
	ManageRecords(context.Context, *logger.Logger, ManageRecordsDTO) error
}

// Options contains the options for the public DNS service.
type Options struct {
	WorkersEnabled bool // Whether task workers are running
}

type ManageRecordsDTO struct {
	ClusterName   string
	Zone          string
	CreateRecords map[string]string // map of A record to public IP to create
	DeleteRecords map[string]string // map of A record to public IP to delete
}
