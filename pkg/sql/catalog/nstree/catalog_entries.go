// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type catalogEntry interface {
	catalog.NameEntry

	// ByteSize returns the size of the underlying struct in memory.
	ByteSize() int64
}

type byNameEntry struct {
	id, parentID, parentSchemaID descpb.ID
	name                         string
	timestamp                    hlc.Timestamp
}

var _ catalogEntry = byNameEntry{}

// GetName implements the catalog.NameKey interface.
func (e byNameEntry) GetName() string {
	return e.name
}

// GetParentID implements the catalog.NameKey interface.
func (e byNameEntry) GetParentID() descpb.ID {
	return e.parentID
}

// GetParentSchemaID implements the catalog.NameKey interface.
func (e byNameEntry) GetParentSchemaID() descpb.ID {
	return e.parentSchemaID
}

// GetID implements the catalog.NameEntry interface.
func (e byNameEntry) GetID() descpb.ID {
	return e.id
}

// ByteSize implements the catalogEntry interface.
func (e byNameEntry) ByteSize() int64 {
	return int64(unsafe.Sizeof(e)) + int64(len(e.name))
}

// NamespaceEntry is a catalog.NameEntry augmented with an MVCC timestamp.
type NamespaceEntry interface {
	catalog.NameEntry
	GetMVCCTimestamp() hlc.Timestamp
}

// GetMVCCTimestamp implements the NamespaceEntry interface.
func (e byNameEntry) GetMVCCTimestamp() hlc.Timestamp {
	return e.timestamp
}

type commentsByType struct {
	subObjectOrdinals util.FastIntMap
	comments          []string
}

type byIDEntry struct {
	id       descpb.ID
	desc     catalog.Descriptor
	comments [catalogkeys.MaxCommentTypeValue + 1]commentsByType
	zc       catalog.ZoneConfig
}

var _ catalogEntry = byIDEntry{}

// GetName implements the catalog.NameKey interface.
func (e byIDEntry) GetName() string {
	return ""
}

// GetParentID implements the catalog.NameKey interface.
func (e byIDEntry) GetParentID() descpb.ID {
	return descpb.InvalidID
}

// GetParentSchemaID implements the catalog.NameKey interface.
func (e byIDEntry) GetParentSchemaID() descpb.ID {
	return descpb.InvalidID
}

// GetID implements the catalog.NameEntry interface.
func (e byIDEntry) GetID() descpb.ID {
	return e.id
}

// ByteSize implements the catalogEntry interface.
func (e byIDEntry) ByteSize() (n int64) {
	var dummy int
	n = int64(unsafe.Sizeof(e))
	if e.desc != nil {
		n += e.desc.ByteSize()
	}
	if e.zc != nil {
		n += int64(e.zc.Size())
	}
	for ct := range e.comments {
		c := &e.comments[ct]
		if len(c.comments) == 0 {
			continue
		}
		for _, s := range c.comments {
			n += int64(len(s))
		}
		n += int64(e.comments[ct].subObjectOrdinals.Len()*2) * int64(unsafe.Sizeof(dummy))
	}
	return n
}

func (e byIDEntry) forEachComment(fn func(key catalogkeys.CommentKey, value string) error) error {
	for ct := range e.comments {
		byType := &e.comments[ct]
		if byType.subObjectOrdinals.Empty() {
			continue
		}
		var err error
		visitor := func(subID, ordinal int) {
			if err != nil {
				return
			}
			key := catalogkeys.CommentKey{
				ObjectID:    uint32(e.id),
				SubID:       uint32(subID),
				CommentType: catalogkeys.CommentType(ct),
			}
			err = fn(key, byType.comments[ordinal])
		}
		byType.subObjectOrdinals.ForEach(visitor)
		if err != nil {
			return err
		}
	}
	return nil
}
