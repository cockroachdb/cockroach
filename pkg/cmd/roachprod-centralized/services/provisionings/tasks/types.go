// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tasks

const TaskServiceName = "provisionings"

// ReferencePrefix is the prefix used for task references linking to
// provisionings. The full reference format is "provisionings#<uuid>".
const ReferencePrefix = TaskServiceName + "#"

// ProvisioningsTaskType is the type of a provisioning task.
type ProvisioningsTaskType string

const (
	// ProvisioningsTaskProvision is the task type for provisioning infrastructure.
	ProvisioningsTaskProvision ProvisioningsTaskType = TaskServiceName + "_provision"
	// ProvisioningsTaskDestroy is the task type for destroying infrastructure.
	ProvisioningsTaskDestroy ProvisioningsTaskType = TaskServiceName + "_destroy"
	// ProvisioningsTaskGC is the task type for garbage-collecting expired
	// provisionings. Only one GC task runs at a time across all instances.
	ProvisioningsTaskGC ProvisioningsTaskType = TaskServiceName + "_gc"
	// ProvisioningsTaskSSHKeysSetup is the task type for running ssh-keys-setup
	// hooks on a provisioning.
	ProvisioningsTaskSSHKeysSetup ProvisioningsTaskType = TaskServiceName + "_ssh_keys_setup"
)
