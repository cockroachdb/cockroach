// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package configprofiles

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// profileTaskProvider is an implementation of acprovider.Provider
// which reports task to execute from a static configuration profile.
type profileTaskProvider struct {
	syncutil.Mutex

	envID       autoconfigpb.EnvironmentID
	tasks       []autoconfigpb.Task
	nextTaskIdx int
}

var _ acprovider.Provider = (*profileTaskProvider)(nil)

// EnvUpdate is part of the acprovider.Provider interface.
func (p *profileTaskProvider) EnvUpdate() <-chan struct{} {
	// This only ever reports 1 update because static profiles
	// never change.
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return ch
}

// ActiveEnvironments is part of the acprovider.Provider interface.
func (p *profileTaskProvider) ActiveEnvironments() []autoconfigpb.EnvironmentID {
	p.Lock()
	defer p.Unlock()
	if p.nextTaskIdx >= len(p.tasks) {
		return nil
	}
	return []autoconfigpb.EnvironmentID{p.envID}
}

// Peek is part of the acprovider.Provider interface.
func (p *profileTaskProvider) Peek(
	ctx context.Context, envID autoconfigpb.EnvironmentID,
) (autoconfigpb.Task, error) {
	p.Lock()
	defer p.Unlock()
	if p.envID != envID {
		return autoconfigpb.Task{}, acprovider.ErrNoMoreTasks
	}
	if p.nextTaskIdx >= len(p.tasks) {
		return autoconfigpb.Task{}, acprovider.ErrNoMoreTasks
	}
	task := p.tasks[p.nextTaskIdx]
	return task, nil
}

// Pop is part of the acprovider.Provider interface.
func (p *profileTaskProvider) Pop(
	ctx context.Context, envID autoconfigpb.EnvironmentID, completedTaskID autoconfigpb.TaskID,
) error {
	p.Lock()
	defer p.Unlock()
	if p.envID != envID {
		// Not this environment. Nothing to do.
		return nil
	}
	for p.nextTaskIdx < len(p.tasks) {
		if completedTaskID >= p.tasks[p.nextTaskIdx].TaskID {
			log.Infof(ctx, "sliding task %d out of queue", p.tasks[p.nextTaskIdx].TaskID)
			p.nextTaskIdx++
			continue
		}
		break
	}
	return nil
}
