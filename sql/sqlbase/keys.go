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
// Author: Tamir Duberstein (tamird@gmail.com)

package sqlbase

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
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

// NormalizeName normalizes to lowercase and Unicode Normalization Form C
// (NFC).
func NormalizeName(name parser.Name) string {
	lower := strings.Map(normalize.ToLower, string(name))
	if isASCII(lower) {
		return lower
	}
	return norm.NFC.String(lower)
}

// ReNormalizeName performs the same work as NormalizeName but when
// the string originates from the database. We define a different
// function so as to be able to track usage of this function (cf. #8200).
func ReNormalizeName(name string) string {
	return NormalizeName(parser.Name(name))
}

// EqualName returns true iff the normalizations of a and b are equal.
func EqualName(a, b parser.Name) bool {
	return NormalizeName(a) == NormalizeName(b)
}

func isASCII(s string) bool {
	for _, c := range s {
		if c > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// NormalizeTableName normalizes the TableName using NormalizeName().
func NormalizeTableName(tn parser.TableName) parser.TableName {
	return parser.TableName{
		DatabaseName: parser.Name(NormalizeName(tn.DatabaseName)),
		TableName:    parser.Name(NormalizeName(tn.TableName)),
	}
}

// MakeNameMetadataKey returns the key for the name. Pass name == "" in order
// to generate the prefix key to use to scan over all of the names for the
// specified parentID.
func MakeNameMetadataKey(parentID ID, name string) roachpb.Key {
	normName := ReNormalizeName(name)
	k := keys.MakeTablePrefix(uint32(NamespaceTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(NamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentID))
	if name != "" {
		k = encoding.EncodeBytesAscending(k, []byte(normName))
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

// MakeZoneKey returns the key for 'id's entry in the system.zones table.
func MakeZoneKey(id ID) roachpb.Key {
	k := keys.MakeTablePrefix(uint32(ZonesTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(ZonesTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(id))
	return keys.MakeFamilyKey(k, uint32(ZonesTable.Columns[1].ID))
}
