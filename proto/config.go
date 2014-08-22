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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import (
	"bytes"
	"fmt"
)

// ContainsKey returns whether this RangeDescriptor contains the specified key.
func (r *RangeDescriptor) ContainsKey(key []byte) bool {
	return bytes.Compare(key, r.StartKey) >= 0 && bytes.Compare(key, r.EndKey) < 0
}

// ContainsKeyRange returns whether this RangeDescriptor contains the specified
// key range from start to end.
func (r *RangeDescriptor) ContainsKeyRange(start, end []byte) bool {
	if len(end) == 0 {
		end = start
	}
	if bytes.Compare(end, start) < 0 {
		panic(fmt.Sprintf("start key is larger than end key %q > %q", string(start), string(end)))
	}
	return bytes.Compare(start, r.StartKey) >= 0 && bytes.Compare(r.EndKey, end) >= 0
}
