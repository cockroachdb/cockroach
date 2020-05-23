// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultDatabaseName is the name ofthe default CockroachDB database used
	// for connections without a current db set.
	DefaultDatabaseName = "defaultdb"

	// PgDatabaseName is the name of the default postgres system database.
	PgDatabaseName = "postgres"
)

// DefaultUserDBs is a set of the databases which are present in a new cluster.
var DefaultUserDBs = map[string]struct{}{
	DefaultDatabaseName: {},
	PgDatabaseName:      {},
}

// MaxDefaultDescriptorID is the maximum ID of a descriptor that exists in a
// new cluster.
var MaxDefaultDescriptorID = keys.MaxReservedDescID + ID(len(DefaultUserDBs))

// IsDefaultCreatedDescriptor returns whether or not a given descriptor ID is
// present at the time of starting a cluster.
func IsDefaultCreatedDescriptor(descID ID) bool {
	return descID <= MaxDefaultDescriptorID
}

// MakeNameMetadataKey returns the key for the name, as expected by
// versions >= 20.1.
// Pass name == "" in order to generate the prefix key to use to scan over all
// of the names for the specified parentID.
func MakeNameMetadataKey(
	codec keys.SQLCodec, parentID ID, parentSchemaID ID, name string,
) roachpb.Key {
	k := codec.IndexPrefix(uint32(NamespaceTable.ID), uint32(NamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentSchemaID))
	if name != "" {
		k = encoding.EncodeBytesAscending(k, []byte(name))
		k = keys.MakeFamilyKey(k, uint32(NamespaceTable.Columns[3].ID))
	}
	return k
}

// DecodeNameMetadataKey returns the components that make up the
// NameMetadataKey for version >= 20.1.
func DecodeNameMetadataKey(
	codec keys.SQLCodec, k roachpb.Key,
) (parentID ID, parentSchemaID ID, name string, err error) {
	k, _, err = codec.DecodeTablePrefix(k)
	if err != nil {
		return 0, 0, "", err
	}

	var buf uint64
	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, 0, "", err
	}
	if buf != uint64(NamespaceTable.PrimaryIndex.ID) {
		return 0, 0, "", errors.Newf("tried get table %d, but got %d", NamespaceTable.PrimaryIndex.ID, buf)
	}

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, 0, "", err
	}
	parentID = ID(buf)

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, 0, "", err
	}
	parentSchemaID = ID(buf)

	var bytesBuf []byte
	_, bytesBuf, err = encoding.DecodeBytesAscending(k, nil)
	if err != nil {
		return 0, 0, "", err
	}
	name = string(bytesBuf)

	return parentID, parentSchemaID, name, nil
}

// MakeDeprecatedNameMetadataKey returns the key for a name, as expected by
// versions < 20.1. Pass name == "" in order to generate the prefix key to use
// to scan over all of the names for the specified parentID.
func MakeDeprecatedNameMetadataKey(codec keys.SQLCodec, parentID ID, name string) roachpb.Key {
	k := codec.IndexPrefix(
		uint32(DeprecatedNamespaceTable.ID), uint32(DeprecatedNamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentID))
	if name != "" {
		k = encoding.EncodeBytesAscending(k, []byte(name))
		k = keys.MakeFamilyKey(k, uint32(DeprecatedNamespaceTable.Columns[2].ID))
	}
	return k
}

// MakeAllDescsMetadataKey returns the key for all descriptors.
func MakeAllDescsMetadataKey(codec keys.SQLCodec) roachpb.Key {
	return codec.DescMetadataPrefix()
}

// MakeDescMetadataKey returns the key for the descriptor.
func MakeDescMetadataKey(codec keys.SQLCodec, descID ID) roachpb.Key {
	return codec.DescMetadataKey(uint32(descID))
}

// IndexKeyValDirs returns the corresponding encoding.Directions for all the
// encoded values in index's "fullest" possible index key, including directions
// for table/index IDs, the interleaved sentinel and the index column values.
// For example, given
//    CREATE INDEX foo ON bar (a, b DESC) INTERLEAVED IN PARENT bar (a)
// a typical index key with all values specified could be
//    /51/1/42/#/51/2/1337
// which would return the slice
//    {ASC, ASC, ASC, 0, ASC, ASC, DESC}
func IndexKeyValDirs(index *IndexDescriptor) []encoding.Direction {
	if index == nil {
		return nil
	}

	dirs := make([]encoding.Direction, 0, (len(index.Interleave.Ancestors)+1)*2+len(index.ColumnDirections))

	colIdx := 0
	for _, ancs := range index.Interleave.Ancestors {
		// Table/Index IDs are always encoded ascending.
		dirs = append(dirs, encoding.Ascending, encoding.Ascending)
		for i := 0; i < int(ancs.SharedPrefixLen); i++ {
			d, err := index.ColumnDirections[colIdx].ToEncodingDirection()
			if err != nil {
				panic(err)
			}
			dirs = append(dirs, d)
			colIdx++
		}

		// The interleaved sentinel uses the 0 value for
		// encoding.Direction when pretty-printing (see
		// encoding.go:prettyPrintFirstValue).
		dirs = append(dirs, 0)
	}

	// The index's table/index ID.
	dirs = append(dirs, encoding.Ascending, encoding.Ascending)

	for colIdx < len(index.ColumnDirections) {
		d, err := index.ColumnDirections[colIdx].ToEncodingDirection()
		if err != nil {
			panic(err)
		}
		dirs = append(dirs, d)
		colIdx++
	}

	return dirs
}

// PrettyKey pretty-prints the specified key, skipping over the first `skip`
// fields. The pretty printed key looks like:
//
//   /Table/<tableID>/<indexID>/...
//
// We always strip off the /Table prefix and then `skip` more fields. Note that
// this assumes that the fields themselves do not contain '/', but that is
// currently true for the fields we care about stripping (the table and index
// ID).
func PrettyKey(valDirs []encoding.Direction, key roachpb.Key, skip int) string {
	p := key.StringWithDirs(valDirs, 0 /* maxLen */)
	for i := 0; i <= skip; i++ {
		n := strings.IndexByte(p[1:], '/')
		if n == -1 {
			return ""
		}
		p = p[n+1:]
	}
	return p
}

// PrettySpan returns a human-readable representation of a span.
func PrettySpan(valDirs []encoding.Direction, span roachpb.Span, skip int) string {
	var b strings.Builder
	b.WriteString(PrettyKey(valDirs, span.Key, skip))
	b.WriteByte('-')
	b.WriteString(PrettyKey(valDirs, span.EndKey, skip))
	return b.String()
}

// PrettySpans returns a human-readable description of the spans.
// If index is nil, then pretty print subroutines will use their default
// settings.
func PrettySpans(index *IndexDescriptor, spans []roachpb.Span, skip int) string {
	if len(spans) == 0 {
		return ""
	}

	valDirs := IndexKeyValDirs(index)

	var b strings.Builder
	for i, span := range spans {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(PrettySpan(valDirs, span, skip))
	}
	return b.String()
}
