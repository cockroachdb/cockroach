// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"container/list"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const maxRunningFlows = 500

type flowScheduler struct {
	log.AmbientContext
	stopper    *stop.Stopper
	flowDoneCh chan *Flow

	mu struct {
		syncutil.Mutex
		numRunning int
		queue      *list.List
	}
}

func newFlowScheduler(ambient log.AmbientContext, stopper *stop.Stopper) *flowScheduler {
	fs := &flowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan *Flow, 1),
	}
	fs.mu.queue = list.New()
	return fs
}

func (fs *flowScheduler) canRunFlow(_ *Flow) bool {
	// TODO(radu): we will have more complex resource accounting (like memory).
	// For now we just limit the number of concurrent flows.
	return fs.mu.numRunning < maxRunningFlows
}

func (fs *flowScheduler) runFlowNow(f *Flow) {
	fs.mu.numRunning++
	f.Start(fs.flowDoneCh)
	// TODO(radu): we could replace the f.waitGroup with a structure that keeps a
	// refcount and automatically runs Cleanup() when the count reaches 0.
	go func() {
		f.Wait()
		f.Cleanup()
	}()
}

// ScheduleFlow is the main interface of the flow scheduler: it runs or enqueues
// the given flow.
func (fs *flowScheduler) ScheduleFlow(f *Flow) error {
	return fs.stopper.RunTask(func() {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		if fs.canRunFlow(f) {
			fs.runFlowNow(f)
		} else {
			fs.mu.queue.PushBack(f)
		}
	})
}

// Start launches the main loop of the scheduler.
func (fs *flowScheduler) Start() {
	fs.stopper.RunWorker(func() {
		stopped := false
		fs.mu.Lock()
		defer fs.mu.Unlock()

		for {
			if stopped && fs.mu.numRunning == 0 {
				// TODO(radu): somehow error out the flows that are still in the queue.
				return
			}
			fs.mu.Unlock()
			select {
			case <-fs.flowDoneCh:
				fs.mu.Lock()
				fs.mu.numRunning--
				if !stopped {
					if frElem := fs.mu.queue.Front(); frElem != nil {
						f := frElem.Value.(*Flow)
						fs.mu.queue.Remove(frElem)
						fs.runFlowNow(f)
					}
				}

			case <-fs.stopper.ShouldStop():
				fs.mu.Lock()
				stopped = true
			}
		}
	})
}
