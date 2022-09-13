// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalogkeys

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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
var DefaultUserDBs = []string{
	DefaultDatabaseName, PgDatabaseName,
}

// IndexColumnEncodingDirection converts a direction from the proto to an
// encoding.Direction.
func IndexColumnEncodingDirection(dir catpb.IndexColumn_Direction) (encoding.Direction, error) {
	switch dir {
	case catpb.IndexColumn_ASC:
		return encoding.Ascending, nil
	case catpb.IndexColumn_DESC:
		return encoding.Descending, nil
	default:
		return 0, errors.Errorf("invalid direction: %s", dir)
	}
}

// IndexKeyValDirs returns the corresponding encoding.Directions for all the
// encoded values in index's "fullest" possible index key, including directions
// for table/index IDs and the index column values.
func IndexKeyValDirs(index catalog.Index) []encoding.Direction {
	if index == nil {
		return nil
	}

	dirs := make([]encoding.Direction, 0, 2+index.NumKeyColumns())

	// The index's table/index ID.
	dirs = append(dirs, encoding.Ascending, encoding.Ascending)

	for colIdx := 0; colIdx < index.NumKeyColumns(); colIdx++ {
		d, err := IndexColumnEncodingDirection(index.GetKeyColumnDirection(colIdx))
		if err != nil {
			panic(err)
		}
		dirs = append(dirs, d)
	}

	return dirs
}

// PrettyKey pretty-prints the specified key, skipping over the first `skip`
// fields. The pretty printed key looks like:
//
//	/Table/<tableID>/<indexID>/...
//
// We always strip off the /Table prefix and then `skip` more fields. Note that
// this assumes that the fields themselves do not contain '/', but that is
// currently true for the fields we care about stripping (the table and index
// ID).
func PrettyKey(valDirs []encoding.Direction, key roachpb.Key, skip int) string {
	p := key.StringWithDirs(valDirs)
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
	if span.EndKey != nil {
		b.WriteByte('-')
		b.WriteString(PrettyKey(valDirs, span.EndKey, skip))
	}
	return b.String()
}

// PrettySpans returns a human-readable description of the spans.
// If index is nil, then pretty print subroutines will use their default
// settings.
func PrettySpans(index catalog.Index, spans []roachpb.Span, skip int) string {
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

// NewNameKeyComponents returns a new catalog.NameKey instance for the given object
// name scoped under the given parent schema and parent database.
func NewNameKeyComponents(
	parentID descpb.ID, parentSchemaID descpb.ID, name string,
) catalog.NameKey {
	return descpb.NameInfo{ParentID: parentID, ParentSchemaID: parentSchemaID, Name: name}
}

// MakeObjectNameKey returns the roachpb.Key for the given object name
// scoped under the given schema in the given database.
func MakeObjectNameKey(
	codec keys.SQLCodec, parentID, parentSchemaID descpb.ID, name string,
) roachpb.Key {
	return EncodeNameKey(codec, NewNameKeyComponents(parentID, parentSchemaID, name))
}

// MakePublicObjectNameKey returns the roachpb.Key for the given object name
// scoped under the public schema in the given database.
func MakePublicObjectNameKey(codec keys.SQLCodec, parentID descpb.ID, name string) roachpb.Key {
	return EncodeNameKey(codec, NewNameKeyComponents(parentID, keys.PublicSchemaID, name))
}

// MakeSchemaNameKey returns the roachpb.Key for the given schema name scoped
// under the given database.
func MakeSchemaNameKey(codec keys.SQLCodec, parentID descpb.ID, name string) roachpb.Key {
	return EncodeNameKey(codec, NewNameKeyComponents(parentID, keys.RootNamespaceID, name))
}

// MakeDatabaseNameKey returns the roachpb.Key corresponding to the database
// with the given name.
func MakeDatabaseNameKey(codec keys.SQLCodec, name string) roachpb.Key {
	return EncodeNameKey(codec, NewNameKeyComponents(keys.RootNamespaceID, keys.RootNamespaceID, name))
}

// EncodeNameKey encodes nameKey using codec.
func EncodeNameKey(codec keys.SQLCodec, nameKey catalog.NameKey) roachpb.Key {
	r := codec.IndexPrefix(keys.NamespaceTableID, catconstants.NamespaceTablePrimaryIndexID)
	r = encoding.EncodeUvarintAscending(r, uint64(nameKey.GetParentID()))
	r = encoding.EncodeUvarintAscending(r, uint64(nameKey.GetParentSchemaID()))
	if nameKey.GetName() != "" {
		r = encoding.EncodeBytesAscending(r, []byte(nameKey.GetName()))
		r = keys.MakeFamilyKey(r, catconstants.NamespaceTableFamilyID)
	}
	return r
}

// DecodeNameMetadataKey is the reciprocal of EncodeNameKey.
func DecodeNameMetadataKey(
	codec keys.SQLCodec, k roachpb.Key,
) (nameKey descpb.NameInfo, err error) {
	k, _, err = codec.DecodeTablePrefix(k)
	if err != nil {
		return nameKey, err
	}

	var buf uint64
	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return nameKey, err
	}
	if buf != uint64(catconstants.NamespaceTablePrimaryIndexID) {
		return nameKey, errors.Newf("tried get table %d, but got %d", catconstants.NamespaceTablePrimaryIndexID, buf)
	}

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return nameKey, err
	}
	nameKey.ParentID = descpb.ID(buf)

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return nameKey, err
	}
	nameKey.ParentSchemaID = descpb.ID(buf)

	var bytesBuf []byte
	_, bytesBuf, err = encoding.DecodeBytesAscending(k, nil)
	if err != nil {
		return nameKey, err
	}
	nameKey.Name = string(bytesBuf)

	return nameKey, nil
}

// MakeAllDescsMetadataKey returns the key for all descriptors.
func MakeAllDescsMetadataKey(codec keys.SQLCodec) roachpb.Key {
	return codec.DescMetadataPrefix()
}

// MakeDescMetadataKey returns the key for the descriptor.
func MakeDescMetadataKey(codec keys.SQLCodec, descID descpb.ID) roachpb.Key {
	return codec.DescMetadataKey(uint32(descID))
}
