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

package sql

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Special case normalization rules for Turkish/Azeri lowercase dotless-i and
// uppercase dotted-i. Fold both dotted and dotless 'i' into the ascii i/I, so
// our case-insensitive comparison functions can be locale-invariant. This
// mapping implements case-insensitivity for Turkish and other latin-derived
// languages simultaneously, with the additional quirk that it is also
// insensitive to the dottedness of the i's
var normalize = unicode.SpecialCase{
	unicode.CaseRange{
		Lo: 0x0130,
		Hi: 0x0130,
		Delta: [unicode.MaxCase]rune{
			0x49 - 0x130, // Upper
			0x69 - 0x130, // Lower
			0x49 - 0x130, // Title
		},
	},
	unicode.CaseRange{
		Lo: 0x0131,
		Hi: 0x0131,
		Delta: [unicode.MaxCase]rune{
			0x49 - 0x131, // Upper
			0x69 - 0x131, // Lower
			0x49 - 0x131, // Title
		},
	},
}

// normalizeName normalizes to lowercase and Unicode Normalization Form C
// (NFC).
func normalizeName(name string) string {
	return norm.NFC.String(strings.Map(normalize.ToLower, name))
}

// equalName returns true iff the normalizations of a and b are equal.
func equalName(a, b string) bool {
	return normalizeName(a) == normalizeName(b)
}

// MakeNameMetadataKey returns the key for the name. Pass name == "" in order
// to generate the prefix key to use to scan over all of the names for the
// specified parentID.
func MakeNameMetadataKey(parentID ID, name string) proto.Key {
	name = normalizeName(name)
	k := keys.MakeTablePrefix(uint32(NamespaceTable.ID))
	k = encoding.EncodeUvarint(k, uint64(NamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarint(k, uint64(parentID))
	if name != "" {
		k = encoding.EncodeBytes(k, []byte(name))
		k = encoding.EncodeUvarint(k, uint64(NamespaceTable.Columns[2].ID))
	}
	return k
}

// MakeDescMetadataKey returns the key for the descriptor.
func MakeDescMetadataKey(descID ID) proto.Key {
	k := keys.MakeTablePrefix(uint32(DescriptorTable.ID))
	k = encoding.EncodeUvarint(k, uint64(DescriptorTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarint(k, uint64(descID))
	k = encoding.EncodeUvarint(k, uint64(DescriptorTable.Columns[1].ID))
	return k
}

// MakeZoneKey returns the key for 'id's entry in the system.zones table.
func MakeZoneKey(id ID) proto.Key {
	k := keys.MakeTablePrefix(uint32(ZonesTable.ID))
	k = encoding.EncodeUvarint(k, uint64(ZonesTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarint(k, uint64(id))
	k = encoding.EncodeUvarint(k, uint64(ZonesTable.Columns[1].ID))
	return k
}

// MakeColumnKey returns the key for the column in the given row.
func MakeColumnKey(colID ColumnID, primaryKey []byte) proto.Key {
	var key []byte
	key = append(key, primaryKey...)
	return encoding.EncodeUvarint(key, uint64(colID))
}

// MakeIndexKeyPrefix returns the key prefix used for the index's data.
func MakeIndexKeyPrefix(tableID ID, indexID IndexID) []byte {
	var key []byte
	key = append(key, keys.TableDataPrefix...)
	key = encoding.EncodeUvarint(key, uint64(tableID))
	key = encoding.EncodeUvarint(key, uint64(indexID))
	return key
}
