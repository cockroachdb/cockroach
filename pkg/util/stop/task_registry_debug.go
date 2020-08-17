// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build race

package stop

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Drain and Register are never called concurrently.
type taskRegistry struct {
	mu       syncutil.Mutex
	wg       sync.WaitGroup
	numTasks int
	tasks    TaskMap
}

func makeTaskRegistry() taskRegistry {
	return taskRegistry{
		tasks: TaskMap{},
	}
}

func (r *taskRegistry) Register(taskName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wg.Add(1)
	r.numTasks++
	r.tasks[taskName]++
}

func (r *taskRegistry) Unregister(taskName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wg.Done()
	r.numTasks--
	r.tasks[taskName]--
}

func (r *taskRegistry) Quiesce() {
	r.wg.Wait()
}

func (r *taskRegistry) NumTasks() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.numTasks
}

// RunningTasks returns a map containing the count of running tasks keyed by
// call site.
func (r *taskRegistry) RunningTasks() TaskMap {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := TaskMap{}
	for k := range r.tasks {
		if r.tasks[k] == 0 {
			continue
		}
		m[k] = r.tasks[k]
	}
	return m
}
