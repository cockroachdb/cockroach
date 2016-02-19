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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf

package log

import "reflect"

type field interface {
	populate(entry *LogEntry, v interface{})
}

type nodeIDField struct{}

func (nodeIDField) String() string {
	return "NodeID"
}

func (nodeIDField) populate(entry *LogEntry, v interface{}) {
	entry.NodeID = new(int32)
	*entry.NodeID = int32(reflect.ValueOf(v).Int())
}

type storeIDField struct{}

func (storeIDField) String() string {
	return "StoreID"
}

func (storeIDField) populate(entry *LogEntry, v interface{}) {
	entry.StoreID = new(int32)
	*entry.StoreID = int32(reflect.ValueOf(v).Int())
}

type rangeIDField struct{}

func (rangeIDField) String() string {
	return "RangeID"
}

func (rangeIDField) populate(entry *LogEntry, v interface{}) {
	entry.RangeID = new(int64)
	*entry.RangeID = int64(reflect.ValueOf(v).Int())
}

var (
	// NodeID is a context key identifying a node ID.
	NodeID = nodeIDField{}
	// StoreID is a context key identifying a store ID.
	StoreID = storeIDField{}
	// RangeID is a context key identifying a range ID.
	RangeID = rangeIDField{}
)

var allFields = []field{
	NodeID,
	StoreID,
	RangeID,
}
