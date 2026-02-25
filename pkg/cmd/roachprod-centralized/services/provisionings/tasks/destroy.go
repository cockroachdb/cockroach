// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/tasks"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TaskDestroy is the task that destroys provisioned infrastructure via OpenTofu.
type TaskDestroy struct {
	tasks.Task
	Service types.IProvisioningTaskHandler
	Options TaskDestroyOptions
}

// TaskDestroyOptions contains the options for a destroy task.
type TaskDestroyOptions struct {
	ProvisioningID uuid.UUID `json:"provisioning_id"`
}

// NewTaskDestroy creates a new TaskDestroy instance with the given
// provisioning ID serialized as the task payload.
func NewTaskDestroy(provisioningID uuid.UUID) (*TaskDestroy, error) {
	opts := TaskDestroyOptions{ProvisioningID: provisioningID}
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "marshal destroy task options")
	}
	return &TaskDestroy{
		Task: tasks.Task{
			Type:      string(ProvisioningsTaskDestroy),
			Payload:   payload,
			Reference: ReferencePrefix + provisioningID.String(),
		},
		Options: opts,
	}, nil
}

// SetPayload overrides base Task.SetPayload to also deserialize Options.
func (t *TaskDestroy) SetPayload(data []byte) error {
	t.Payload = data
	if len(data) > 0 {
		if err := json.Unmarshal(data, &t.Options); err != nil {
			return errors.Wrap(err, "unmarshal destroy task options")
		}
	}
	return nil
}

// GetTimeout returns the maximum duration for a destroy task.
func (t *TaskDestroy) GetTimeout() time.Duration {
	return 30 * time.Minute
}

// ResolveConcurrencyKey overrides the default per-type key with per-provisioning
// locking so independent destroy operations can run in parallel.
func (t *TaskDestroy) ResolveConcurrencyKey() string {
	if t.Options.ProvisioningID == uuid.Nil {
		return ""
	}
	return ReferencePrefix + t.Options.ProvisioningID.String()
}

// Process executes the destroy orchestration.
func (t *TaskDestroy) Process(ctx context.Context, l *logger.Logger) error {
	return t.Service.HandleDestroy(ctx, l, t.Options.ProvisioningID)
}
