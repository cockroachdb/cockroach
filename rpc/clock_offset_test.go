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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/log"
)

func TestEndpointListSort(t *testing.T) {
	list := endpointList{
		endpoint{offset: 5, endType: +1},
		endpoint{offset: 3, endType: -1},
		endpoint{offset: 3, endType: +1},
		endpoint{offset: 1, endType: -1},
		endpoint{offset: 4, endType: +1},
	}

	sortedList := endpointList{
		endpoint{offset: 1, endType: -1},
		endpoint{offset: 3, endType: -1},
		endpoint{offset: 3, endType: +1},
		endpoint{offset: 4, endType: +1},
		endpoint{offset: 5, endType: +1},
	}

	sort.Sort(list)
	for i := range sortedList {
		if list[i] != sortedList[i] {
			t.Errorf("expected index %d of sorted list to be %v, instead %v",
				i, sortedList[i], list[i])
		}
	}

	if len(list) != len(sortedList) {
		t.Errorf("exptected endpoint list to be size %d, instead %d",
			len(sortedList), len(list))
	}
}

func TestBuildEndpointList(t *testing.T) {
	// Build the offsets we will turn into an interval list.
	offsets := make(map[string]RemoteOffset)
	offsets["0"] = RemoteOffset{Offset: 0, Error: 10}
	offsets["1"] = RemoteOffset{Offset: 1, Error: 10}
	offsets["2"] = RemoteOffset{Offset: 2, Error: 10}
	offsets["3"] = RemoteOffset{Offset: 3, Error: 10}
	log.Infof("base offsets: %v", offsets)
	remoteClocks := &RemoteClockMonitor{
		offsets:  offsets,
		maxDrift: 5 * time.Nanosecond,
	}

	expectedList := endpointList{
		endpoint{offset: -15, endType: -1},
		endpoint{offset: -14, endType: -1},
		endpoint{offset: -13, endType: -1},
		endpoint{offset: -12, endType: -1},
		endpoint{offset: 15, endType: 1},
		endpoint{offset: 16, endType: 1},
		endpoint{offset: 17, endType: 1},
		endpoint{offset: 18, endType: 1},
	}

	list := remoteClocks.buildEndpointList()
	sort.Sort(list)

	for i := range expectedList {
		if list[i] != expectedList[i] {
			t.Errorf("expected index %d of list to be %v, instead %v",
				i, expectedList[i], list[i])
		}
	}

	if len(list) != len(expectedList) {
		t.Errorf("exptected endpoint list to be size %d, instead %d",
			len(expectedList), len(list))
	}
}

func TestFindOffsetInterval(t *testing.T) {
	// Build the offsets. We will return the interval that the maximum number
	// of remote clocks overlap.
	offsets := make(map[string]RemoteOffset)
	offsets["0"] = RemoteOffset{Offset: 20, Error: 10}
	offsets["1"] = RemoteOffset{Offset: 58, Error: 20}
	offsets["2"] = RemoteOffset{Offset: 71, Error: 25}
	offsets["3"] = RemoteOffset{Offset: 91, Error: 31}
	log.Infof("base offsets: %v", offsets)
	remoteClocks := &RemoteClockMonitor{
		offsets:  offsets,
		maxDrift: 0 * time.Second,
	}

	interval := remoteClocks.findOffsetInterval()
	expectedInterval := ClusterOffsetInterval{Lowerbound: 60, Upperbound: 78}
	if interval != expectedInterval {
		t.Errorf("expected interval %v, instead %v", expectedInterval, interval)
	}
}

// TODO(embark): once we figure out what we want to do if there is no interval
// overlapped by a majority of the remote clock offsets, write tests to
// encompass such situations.
