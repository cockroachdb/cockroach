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

// TaskProvision is the task that provisions infrastructure via OpenTofu.
type TaskProvision struct {
	tasks.Task
	Service types.IProvisioningTaskHandler
	Options TaskProvisionOptions
}

// TaskProvisionOptions contains the options for a provision task.
type TaskProvisionOptions struct {
	ProvisioningID uuid.UUID `json:"provisioning_id"`
}

// NewTaskProvision creates a new TaskProvision instance with the given
// provisioning ID serialized as the task payload.
func NewTaskProvision(provisioningID uuid.UUID) (*TaskProvision, error) {
	opts := TaskProvisionOptions{ProvisioningID: provisioningID}
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "marshal provision task options")
	}
	return &TaskProvision{
		Task: tasks.Task{
			Type:      string(ProvisioningsTaskProvision),
			Payload:   payload,
			Reference: ReferencePrefix + provisioningID.String(),
		},
		Options: opts,
	}, nil
}

// SetPayload overrides base Task.SetPayload to also deserialize Options.
func (t *TaskProvision) SetPayload(data []byte) error {
	t.Payload = data
	if len(data) > 0 {
		if err := json.Unmarshal(data, &t.Options); err != nil {
			return errors.Wrap(err, "unmarshal provision task options")
		}
	}
	return nil
}

// GetTimeout returns the maximum duration for a provision task.
func (t *TaskProvision) GetTimeout() time.Duration {
	return 30 * time.Minute
}

// ResolveConcurrencyKey overrides the default per-type key with per-provisioning
// locking so independent provisionings can run in parallel.
func (t *TaskProvision) ResolveConcurrencyKey() string {
	if t.Options.ProvisioningID == uuid.Nil {
		return ""
	}
	return ReferencePrefix + t.Options.ProvisioningID.String()
}

// Process executes the provisioning orchestration.
func (t *TaskProvision) Process(ctx context.Context, l *logger.Logger) error {
	return t.Service.HandleProvision(ctx, l, t.Options.ProvisioningID)
}
