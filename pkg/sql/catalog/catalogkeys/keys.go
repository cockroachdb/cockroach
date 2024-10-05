// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package catalogkeys describes keys used by the SQL catalog.
package catalogkeys

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

// CommentType the type of the schema object on which a comment has been
// applied.
type CommentType int

//go:generate stringer --type CommentType

// Note: please add the new comment types to AllCommentTypes as well.
// Note: do not change the numeric values of this enum -- they correspond
// to stored values in system.comments.
const (
	// DatabaseCommentType comment on a database.
	DatabaseCommentType CommentType = 0
	// TableCommentType comment on a table/view/sequence.
	TableCommentType CommentType = 1
	// ColumnCommentType comment on a column.
	ColumnCommentType CommentType = 2
	// IndexCommentType comment on an index.
	IndexCommentType CommentType = 3
	// SchemaCommentType comment on a schema.
	SchemaCommentType CommentType = 4
	// ConstraintCommentType comment on a constraint.
	ConstraintCommentType CommentType = 5
	// FunctionCommentType comment on a function.
	FunctionCommentType CommentType = 6
	// TypeCommentType comment on a type
	TypeCommentType CommentType = 7
	// MaxCommentTypeValue is the max possible integer of CommentType type.
	// Update this whenever a new comment type is added.
	MaxCommentTypeValue = TypeCommentType
)

// AllCommentTypes is a slice of all valid schema comment types.
var AllCommentTypes = []CommentType{
	DatabaseCommentType,
	TableCommentType,
	ColumnCommentType,
	IndexCommentType,
	SchemaCommentType,
	ConstraintCommentType,
	FunctionCommentType,
	TypeCommentType,
}

// IsValidCommentType checks if a given comment type is in the valid value
// range.
func IsValidCommentType(t CommentType) bool {
	return t >= 0 && t <= MaxCommentTypeValue
}

func init() {
	if len(AllCommentTypes) != int(MaxCommentTypeValue+1) {
		panic("AllCommentTypes should contains all comment types.")
	}
}

// AllTableCommentTypes is a slice of all valid comment types on table elements.
var AllTableCommentTypes = []CommentType{
	TableCommentType,
	ColumnCommentType,
	IndexCommentType,
	ConstraintCommentType,
}

// CommentKey represents the primary index key of system.comments table.
type CommentKey struct {
	// ObjectID is the id of a schema object (e.g. database, schema and table)
	ObjectID uint32
	// SubID is the id of child element of a schema object. For example, in
	// tables, this can be column id, index id or constraints id. CommentType is
	// used to distinguish different sub element types. Some objects, for example
	// database and schema, don't have child elements and SubID is zero for them.
	SubID uint32
	// CommentType represents which type of object or subelement the comment is
	// for.
	CommentType CommentType
}

// MakeCommentKey returns a new CommentKey.
func MakeCommentKey(objID uint32, subID uint32, cmtType CommentType) CommentKey {
	return CommentKey{
		ObjectID:    objID,
		SubID:       subID,
		CommentType: cmtType,
	}
}

// IndexColumnEncodingDirection converts a direction from the proto to an
// encoding.Direction.
func IndexColumnEncodingDirection(dir catenumpb.IndexColumn_Direction) (encoding.Direction, error) {
	switch dir {
	case catenumpb.IndexColumn_ASC:
		return encoding.Ascending, nil
	case catenumpb.IndexColumn_DESC:
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
	var buf redact.StringBuilder
	key.StringWithDirs(&buf, valDirs)
	p := buf.String()
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

// MakeDatabaseChildrenNameKeyPrefix constructs a key which is a prefix to all
// namespace entries for children of the requested database.
func MakeDatabaseChildrenNameKeyPrefix(codec keys.SQLCodec, parentID descpb.ID) roachpb.Key {
	r := codec.IndexPrefix(keys.NamespaceTableID, catconstants.NamespaceTablePrimaryIndexID)
	return encoding.EncodeUvarintAscending(r, uint64(parentID))
}

// EncodeNameKey encodes nameKey using codec.
func EncodeNameKey(codec keys.SQLCodec, nameKey catalog.NameKey) roachpb.Key {
	r := MakeDatabaseChildrenNameKeyPrefix(codec, nameKey.GetParentID())
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

// CommentsMetadataPrefix returns the key prefix for all comments in the
// system.comments table.
func CommentsMetadataPrefix(codec keys.SQLCodec) roachpb.Key {
	return codec.IndexPrefix(keys.CommentsTableID, keys.CommentsTablePrimaryKeyIndexID)
}

// MakeObjectCommentsMetadataPrefix returns the key prefix for a type of comments
// of a descriptor.
func MakeObjectCommentsMetadataPrefix(
	codec keys.SQLCodec, cmtKey CommentType, descID descpb.ID,
) roachpb.Key {
	k := CommentsMetadataPrefix(codec)
	k = encoding.EncodeUvarintAscending(k, uint64(cmtKey))
	return encoding.EncodeUvarintAscending(k, uint64(descID))
}

// DecodeCommentMetadataID decodes a CommentKey from comments metadata key.
func DecodeCommentMetadataID(codec keys.SQLCodec, key roachpb.Key) ([]byte, CommentKey, error) {
	remaining, tableID, indexID, err := codec.DecodeIndexPrefix(key)
	if err != nil {
		return nil, CommentKey{}, err
	}
	if tableID != keys.CommentsTableID || indexID != keys.CommentsTablePrimaryKeyIndexID {
		return nil, CommentKey{}, errors.Errorf("key is not a comments table entry: %v", key)
	}

	remaining, cmtType, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return nil, CommentKey{}, err
	}
	remaining, objID, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return nil, CommentKey{}, err
	}
	remaining, subID, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return nil, CommentKey{}, err
	}
	return remaining, MakeCommentKey(uint32(objID), uint32(subID), CommentType(cmtType)), nil
}
