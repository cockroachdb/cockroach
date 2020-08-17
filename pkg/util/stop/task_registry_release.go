// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !race

package stop

import "sync"

// Drain and Register are never called concurrently.
type taskRegistry struct {
	wg sync.WaitGroup
}

func makeTaskRegistry() taskRegistry {
	return taskRegistry{}
}

func (r *taskRegistry) Register(taskName string) {
	r.wg.Add(1)
}

func (r *taskRegistry) Unregister(taskName string) {
	r.wg.Done()
}

func (r *taskRegistry) Quiesce() {
	r.wg.Wait()
}

func (r *taskRegistry) NumTasks() int {
	return -1
}

func (r *taskRegistry) RunningTasks() TaskMap {
	return TaskMap{"tracking disabled": 0}
}
