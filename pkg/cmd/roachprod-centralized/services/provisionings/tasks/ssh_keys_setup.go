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

// TaskSSHKeysSetup is the task that runs ssh-keys-setup on a provisioning.
type TaskSSHKeysSetup struct {
	tasks.Task
	Service types.IProvisioningTaskHandler
	Options TaskSSHKeysSetupOptions
}

// TaskSSHKeysSetupOptions contains the options for an SSH keys setup task.
type TaskSSHKeysSetupOptions struct {
	ProvisioningID uuid.UUID `json:"provisioning_id"`
}

// NewTaskSSHKeysSetup creates a new TaskSSHKeysSetup instance with the given
// provisioning ID serialized as the task payload.
func NewTaskSSHKeysSetup(provisioningID uuid.UUID) (*TaskSSHKeysSetup, error) {
	opts := TaskSSHKeysSetupOptions{ProvisioningID: provisioningID}
	payload, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "marshal ssh-keys-setup task options")
	}
	return &TaskSSHKeysSetup{
		Task: tasks.Task{
			Type:      string(ProvisioningsTaskSSHKeysSetup),
			Payload:   payload,
			Reference: ReferencePrefix + provisioningID.String(),
		},
		Options: opts,
	}, nil
}

// SetPayload overrides base Task.SetPayload to also deserialize Options.
func (t *TaskSSHKeysSetup) SetPayload(data []byte) error {
	t.Payload = data
	if len(data) > 0 {
		if err := json.Unmarshal(data, &t.Options); err != nil {
			return errors.Wrap(err, "unmarshal ssh-keys-setup task options")
		}
	}
	return nil
}

// GetTimeout returns the maximum duration for an SSH keys setup task.
func (t *TaskSSHKeysSetup) GetTimeout() time.Duration {
	return 10 * time.Minute
}

// ResolveConcurrencyKey prevents concurrent SSH key setup on the same
// provisioning.
func (t *TaskSSHKeysSetup) ResolveConcurrencyKey() string {
	if t.Options.ProvisioningID == uuid.Nil {
		return ""
	}
	return "ssh-keys-setup#" + t.Options.ProvisioningID.String()
}

// Process executes the SSH keys setup.
func (t *TaskSSHKeysSetup) Process(ctx context.Context, l *logger.Logger) error {
	return t.Service.HandleSetupSSHKeys(ctx, l, t.Options.ProvisioningID)
}
