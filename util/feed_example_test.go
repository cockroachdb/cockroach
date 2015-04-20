// Copyright 2015 The Cockroach Authors.
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
// Author: Matt Tracy (matt@cockroachlabs.com)

package util

import (
	"fmt"
)

func ExampleFeed() {
	stopper := NewStopper()
	feed := StartFeed(stopper)

	output := make([][]string, 5)
	for i := 0; i < len(output); i++ {
		sub := feed.Subscribe()
		index := i
		stopper.RunWorker(func() {
			for event := range sub.Events {
				// events must be cast from interface{}
				output[index] = append(output[index], event.(string))
			}
		})
	}

	feed.Publish("Event 1")
	feed.Publish("Event 2")
	feed.Publish("Event 3")
	stopper.Stop()

	<-stopper.IsStopped()
	for i, out := range output {
		fmt.Printf("subscriber %d got output %v\n", i+1, out)
	}
	//Output:
	//subscriber 1 got output [Event 1 Event 2 Event 3]
	//subscriber 2 got output [Event 1 Event 2 Event 3]
	//subscriber 3 got output [Event 1 Event 2 Event 3]
	//subscriber 4 got output [Event 1 Event 2 Event 3]
	//subscriber 5 got output [Event 1 Event 2 Event 3]
}
