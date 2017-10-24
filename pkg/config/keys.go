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

package config

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// DecodeObjectID decodes the object ID from the front of key. It returns the
// decoded object ID, the remainder of the key, and whether the result is valid
// (i.e., whether the key was within the structured key space).
func DecodeObjectID(key roachpb.RKey) (uint32, []byte, bool) {
	if key.Equal(roachpb.RKeyMax) {
		return 0, nil, false
	}
	if encoding.PeekType(key) != encoding.Int {
		// TODO(marc): this should eventually return SystemDatabaseID.
		return 0, nil, false
	}
	// Consume first encoded int.
	rem, id64, err := encoding.DecodeUvarintAscending(key)
	return uint32(id64), rem, err == nil
}
