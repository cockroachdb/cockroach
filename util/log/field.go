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
// Author: Tobias Schottdorf

package log

// A Field is an integer used to enumerate allowed field names in structured
// log output.
type Field int

//go:generate stringer -type Field
const (
	NodeID   Field = iota // the ID of the node
	StoreID               // the ID of the store
	RaftID                // the ID of the range
	Method                // the method being executed
	Client                // TODO: client on whose behalf we're acting
	Key                   // a proto.Key related to an event.
	maxField              // internal field bounding the range of allocated fields
)
