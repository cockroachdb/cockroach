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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
)

// Constants for system-reserved keys in the KV map.
var (
	// KeyMin is a minimum key value which sorts before all other keys.
	KeyMin = Key("")
	// KeyMax is a maximum key value which sorts after all other
	// keys. Because keys are stored using an ordered encoding (see
	// storage/encoding.go), they will never start with \xff.
	KeyMax = Key("\xff")

	// KeyMeta1Prefix is the first level of key addressing. The value is a
	// slice of Replica structs.
	KeyMeta1Prefix = Key("\x00\x00meta1")
	// KeyMeta2Prefix is the second level of key addressing. The value is a
	// slice of Replica structs.
	KeyMeta2Prefix = Key("\x00\x00meta2")
	// KeyNodeIDGenerator contains a sequence generator for node IDs.
	KeyNodeIDGenerator = Key("\x00node-id-generator")
	// KeyStoreIDGeneratorPrefix specifies key prefixes for sequence
	// generators, one per node, for store IDs.
	KeyStoreIDGeneratorPrefix = Key("\x00store-id-generator-")
)

func MakeKey(prefix, suffix Key) Key {
	return Key(bytes.Join([][]byte{prefix, suffix}, []byte{}))
}
