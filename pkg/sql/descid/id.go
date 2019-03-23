// Copyright 2018 The Cockroach Authors.
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

package descid

import "github.com/cockroachdb/cockroach/pkg/keys"

// T is a custom type for {Database,Table}Descriptor IDs.
type T int

// InvalidID is the uninitialised descriptor id.
const InvalidID T = 0

// Ts is a sortable list of Ts.
type Ts []T

func (ids Ts) Len() int           { return len(ids) }
func (ids Ts) Less(i, j int) bool { return ids[i] < ids[j] }
func (ids Ts) Swap(i, j int)      { ids[i], ids[j] = ids[j], ids[i] }

func (id T) IsReservedID() bool {
	return id > 0 && id <= keys.MaxReservedDescID
}
