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

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// MakeNameMetadataKey returns the key for the name. Pass name == "" in order
// to generate the prefix key to use to scan over all of the names for the
// specified parentID.
func MakeNameMetadataKey(parentID ID, name string) roachpb.Key {
	k := keys.MakeTablePrefix(uint32(NamespaceTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(NamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentID))
	if name != "" {
		k = encoding.EncodeBytesAscending(k, []byte(name))
		k = keys.MakeFamilyKey(k, uint32(NamespaceTable.Columns[2].ID))
	}
	return k
}

// MakeAllDescsMetadataKey returns the key for all descriptors.
func MakeAllDescsMetadataKey() roachpb.Key {
	k := keys.MakeTablePrefix(uint32(DescriptorTable.ID))
	return encoding.EncodeUvarintAscending(k, uint64(DescriptorTable.PrimaryIndex.ID))
}

// MakeDescMetadataKey returns the key for the descriptor.
func MakeDescMetadataKey(descID ID) roachpb.Key {
	k := MakeAllDescsMetadataKey()
	k = encoding.EncodeUvarintAscending(k, uint64(descID))
	return keys.MakeFamilyKey(k, uint32(DescriptorTable.Columns[1].ID))
}
