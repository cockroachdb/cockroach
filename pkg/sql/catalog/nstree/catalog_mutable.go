// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MutableCatalog is like Catalog but mutable.
type MutableCatalog struct {
	Catalog
}

var _ validate.ValidationDereferencer = MutableCatalog{}

func (mc *MutableCatalog) maybeInitialize() {
	if mc.IsInitialized() {
		return
	}
	mc.byID = makeByIDMap()
	mc.byName = makeByNameMap()
}

// Clear empties the MutableCatalog.
func (mc *MutableCatalog) Clear() {
	if mc.IsInitialized() {
		mc.byID.clear()
		mc.byName.clear()
	}
	*mc = MutableCatalog{}
}

func (mc *MutableCatalog) ensureForID(id descpb.ID) *byIDEntry {
	mc.maybeInitialize()
	newEntry := &byIDEntry{
		id: id,
	}
	if replaced := mc.byID.upsert(newEntry); replaced != nil {
		*newEntry = *(replaced.(*byIDEntry))
	} else {
		mc.byteSize += newEntry.ByteSize()
	}
	return newEntry
}

func (mc *MutableCatalog) maybeGetByID(id descpb.ID) *byIDEntry {
	if !mc.IsInitialized() {
		return nil
	}
	e, _ := mc.byID.get(id).(*byIDEntry)
	return e
}

func (mc *MutableCatalog) ensureForName(key catalog.NameKey) *byNameEntry {
	mc.maybeInitialize()
	newEntry := &byNameEntry{
		parentID:       key.GetParentID(),
		parentSchemaID: key.GetParentSchemaID(),
		name:           key.GetName(),
	}
	if replaced := mc.byName.upsert(newEntry); replaced != nil {
		*newEntry = *(replaced.(*byNameEntry))
	} else {
		mc.byteSize += newEntry.ByteSize()
	}
	return newEntry
}

// DeleteByName removes all by-name mappings from the MutableCatalog.
func (mc *MutableCatalog) DeleteByName(key catalog.NameKey) {
	if key == nil || !mc.IsInitialized() {
		return
	}
	if removed := mc.byName.delete(key); removed != nil {
		mc.byteSize -= removed.(catalogEntry).ByteSize()
	}
}

// UpsertNamespaceEntry adds a name -> id mapping to the MutableCatalog.
func (mc *MutableCatalog) UpsertNamespaceEntry(
	key catalog.NameKey, id descpb.ID, mvccTimestamp hlc.Timestamp,
) {
	if key == nil || id == descpb.InvalidID {
		return
	}
	e := mc.ensureForName(key)
	e.id = id
	e.timestamp = mvccTimestamp
}

// DeleteByID removes all by-ID mappings from the MutableCatalog.
func (mc *MutableCatalog) DeleteByID(id descpb.ID) {
	if !mc.IsInitialized() {
		return
	}
	if removed := mc.byID.delete(id); removed != nil {
		mc.byteSize -= removed.(catalogEntry).ByteSize()
	}
}

// UpsertDescriptor adds a descriptor to the MutableCatalog.
func (mc *MutableCatalog) UpsertDescriptor(desc catalog.Descriptor) {
	if desc == nil || desc.GetID() == descpb.InvalidID {
		return
	}
	e := mc.ensureForID(desc.GetID())
	mc.byteSize -= e.ByteSize()
	e.desc = desc
	mc.byteSize += e.ByteSize()
}

// UpsertComment upserts a ((ObjectID, SubID, CommentType) -> Comment) mapping
// into the catalog.
func (mc *MutableCatalog) UpsertComment(key catalogkeys.CommentKey, cmt string) {
	e := mc.ensureForID(descpb.ID(key.ObjectID))
	mc.byteSize -= e.ByteSize()
	c := &e.comments[key.CommentType]
	if ordinal, found := c.subObjectOrdinals.Get(int(key.SubID)); found {
		c.comments[ordinal] = cmt
	} else {
		c.subObjectOrdinals.Set(int(key.SubID), len(c.comments))
		c.comments = append(c.comments, cmt)
	}
	mc.byteSize += e.ByteSize()
}

// DeleteComment deletes a comment from the catalog.
func (mc *MutableCatalog) DeleteComment(key catalogkeys.CommentKey) {
	if !mc.IsInitialized() {
		return
	}
	e := mc.maybeGetByID(descpb.ID(key.ObjectID))
	if e == nil {
		return
	}
	oldByteSize := e.ByteSize()
	cbt := &e.comments[key.CommentType]
	oldCommentsByType := *cbt
	*cbt = commentsByType{}
	oldCommentsByType.subObjectOrdinals.ForEach(func(subID, oldOrdinal int) {
		if uint32(subID) == key.SubID {
			return
		}
		cbt.comments = append(cbt.comments, oldCommentsByType.comments[oldOrdinal])
		cbt.subObjectOrdinals.Set(subID, len(cbt.comments))
	})
	mc.byteSize += e.ByteSize() - oldByteSize
}

// UpsertZoneConfig upserts a (descriptor id -> zone config) mapping into the
// catalog.
func (mc *MutableCatalog) UpsertZoneConfig(
	id descpb.ID, zoneConfig *zonepb.ZoneConfig, rawBytes []byte,
) {
	e := mc.ensureForID(id)
	mc.byteSize -= e.ByteSize()
	e.zc = zone.NewZoneConfigWithRawBytes(zoneConfig, rawBytes)
	mc.byteSize += e.ByteSize()
}

// DeleteZoneConfig deletes a zone config from the catalog.
func (mc *MutableCatalog) DeleteZoneConfig(id descpb.ID) {
	if !mc.IsInitialized() {
		return
	}
	e := mc.maybeGetByID(id)
	if e == nil {
		return
	}
	oldByteSize := e.ByteSize()
	e.zc = nil
	mc.byteSize += e.ByteSize() - oldByteSize
}

// AddAll adds the contents of the provided catalog to this one.
func (mc *MutableCatalog) AddAll(c Catalog) {
	if !c.IsInitialized() {
		return
	}
	_ = c.byName.ascend(func(entry catalog.NameEntry) error {
		e := mc.ensureForName(entry)
		mc.byteSize -= e.ByteSize()
		*e = *(entry.(*byNameEntry))
		mc.byteSize += e.ByteSize()
		return nil
	})
	_ = c.byID.ascend(func(entry catalog.NameEntry) error {
		e := mc.ensureForID(entry.GetID())
		mc.byteSize -= e.ByteSize()
		*e = *(entry.(*byIDEntry))
		mc.byteSize += e.ByteSize()
		return nil
	})
}
