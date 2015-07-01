// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import (
	"fmt"

	"github.com/cockroachdb/cockroach/util/stop"
)

// eventDemux turns the unified MultiRaft.Events stream into a set of type-safe
// channels for ease of testing. It is not suitable for non-test use because
// unconsumed channels can become backlogged and block.
type eventDemux struct {
	LeaderElection            chan *EventLeaderElection
	CommandCommitted          chan *EventCommandCommitted
	MembershipChangeCommitted chan *EventMembershipChangeCommitted

	events <-chan interface{}
}

func newEventDemux(events <-chan interface{}) *eventDemux {
	return &eventDemux{
		make(chan *EventLeaderElection, 1000),
		make(chan *EventCommandCommitted, 1000),
		make(chan *EventMembershipChangeCommitted, 1000),
		events,
	}
}

func (e *eventDemux) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		for {
			select {
			case event := <-e.events:
				switch event := event.(type) {
				case *EventLeaderElection:
					e.LeaderElection <- event

				case *EventCommandCommitted:
					e.CommandCommitted <- event

				case *EventMembershipChangeCommitted:
					e.MembershipChangeCommitted <- event

				default:
					panic(fmt.Sprintf("got unknown event type %T", event))
				}

			case <-stopper.ShouldStop():
				close(e.CommandCommitted)
				close(e.MembershipChangeCommitted)
				close(e.LeaderElection)
				return
			}
		}
	})
}
