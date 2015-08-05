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
// Author: Tamir Duberstein (tamird@gmail.com)

package structured

import (
	"strings"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// MakeNameMetadataKey returns the key for the namespace.
func MakeNameMetadataKey(parentID ID, name string) proto.Key {
	name = strings.ToLower(name)
	k := make([]byte, 0, len(keys.NameMetadataPrefix)+encoding.MaxUvarintSize+len(name))
	k = append(k, keys.NameMetadataPrefix...)
	k = encoding.EncodeUvarint(k, uint64(parentID))
	k = append(k, name...)
	return k
}

// MakeDescMetadataKey returns the key for the table in namespaceID.
func MakeDescMetadataKey(descID ID) proto.Key {
	k := make([]byte, 0, len(keys.DescMetadataPrefix)+encoding.MaxUvarintSize)
	k = append(k, keys.DescMetadataPrefix...)
	k = encoding.EncodeUvarint(k, uint64(descID))
	return k
}
