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

import (
	"bytes"
	"fmt"
)

type field interface {
	format(buf *bytes.Buffer, v interface{})
}

type nodeIDField struct{}

func (nodeIDField) String() string {
	return "NodeID"
}

func (nodeIDField) format(buf *bytes.Buffer, v interface{}) {
	fmt.Fprintf(buf, "node=%d", v)
}

type storeIDField struct{}

func (storeIDField) String() string {
	return "StoreID"
}

func (storeIDField) format(buf *bytes.Buffer, v interface{}) {
	fmt.Fprintf(buf, "store=%d", v)
}

type rangeIDField struct{}

func (rangeIDField) String() string {
	return "RangeID"
}

func (rangeIDField) format(buf *bytes.Buffer, v interface{}) {
	fmt.Fprintf(buf, "range=%d", v)
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
