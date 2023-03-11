// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acprovider

import "github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"

// Provider is an itnerface through which the auto config runner
// can receive new tasks to run.
type Provider interface {
	// RegisterTasksChannel returns a channel that receives a message
	// any time the current set of tasks changes.
	// The channel receives an initial event immediately.
	RegisterTasksChannel() <-chan struct{}

	// ReportLastKnownCompletedTaskID tells the provider that a
	// completion result is known for the given task. The provider can
	// use this information to truncate the list of remaining tasks.
	ReportLastKnownCompletedTaskID(taskID uint64)

	// GetTasks retrieves the current list of tasks, sorted by task ID
	// (lowest ID first).
	GetTasks() []autoconfigpb.Task
}

// NoTaskProvider is a stub provider which delivers no tasks.
type NoTaskProvider struct{}

func (NoTaskProvider) RegisterTasksChannel() <-chan struct{} { return make(chan struct{}) }
func (NoTaskProvider) ReportLastKnownCompletedTaskID(uint64) {}
func (NoTaskProvider) GetTasks() []autoconfigpb.Task         { return nil }
