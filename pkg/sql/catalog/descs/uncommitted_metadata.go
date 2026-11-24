// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
)

// uncommittedMetadata tracks uncommitted comments and zone configs using
// an nstree.MutableCatalog for storage, plus separate sets for tracking
// entries that have been explicitly marked as absent.
type uncommittedMetadata struct {
	// mc stores actual uncommitted values (comments and zone configs).
	// This handles both storage and memory tracking via its built-in byteSize.
	mc nstree.MutableCatalog

	// absentCommentKeys tracks comment keys that have been looked up and
	// found to be absent (deleted or never existed). This is separate from
	// mc because MutableCatalog only stores actual values.
	absentCommentKeys map[catalogkeys.CommentKey]struct{}

	// absentZoneConfigIDs tracks descriptor IDs whose zone configs have
	// been looked up and found to be absent.
	absentZoneConfigIDs map[descpb.ID]struct{}
}

func makeUncommittedMetadata() uncommittedMetadata {
	return uncommittedMetadata{}
}

func (um *uncommittedMetadata) reset() {
	um.mc.Clear()
	um.absentCommentKeys = nil
	um.absentZoneConfigIDs = nil
}

// Comment methods

// getUncommittedComment returns the uncommitted comment for the given key.
// It returns (comment, hasComment, cached) where:
//   - cached=false means the key hasn't been looked up yet
//   - cached=true, hasComment=false means the key was looked up and is absent
//   - cached=true, hasComment=true means the key has a value
func (um *uncommittedMetadata) getUncommittedComment(
	key catalogkeys.CommentKey,
) (cmt string, hasCmt bool, cached bool) {
	if um.absentCommentKeys != nil {
		if _, absent := um.absentCommentKeys[key]; absent {
			return "", false, true
		}
	}
	if cmt, found := um.mc.LookupComment(key); found {
		return cmt, true, true
	}
	return "", false, false
}

// markNoComment marks the comment for this key as absent (deleted or never
// existed). This adds it to the cache so subsequent lookups know not to
// query storage.
func (um *uncommittedMetadata) markNoComment(key catalogkeys.CommentKey) {
	um.mc.DeleteComment(key)
	if um.absentCommentKeys == nil {
		um.absentCommentKeys = make(map[catalogkeys.CommentKey]struct{})
	}
	um.absentCommentKeys[key] = struct{}{}
}

// upsertComment adds or updates a comment in the uncommitted layer.
func (um *uncommittedMetadata) upsertComment(key catalogkeys.CommentKey, cmt string) error {
	delete(um.absentCommentKeys, key)
	return um.mc.UpsertComment(key, cmt)
}

// markTableCommentsDeleted removes all comments for the given table from the
// uncommitted layer. Note: this only affects comments that were previously
// added to the uncommitted layer; it does not mark stored comments as deleted.
func (um *uncommittedMetadata) markTableCommentsDeleted(tblID descpb.ID) {
	// Collect keys to delete from MutableCatalog.
	var keysToDelete []catalogkeys.CommentKey
	_ = um.mc.ForEachComment(func(key catalogkeys.CommentKey, _ string) error {
		if descpb.ID(key.ObjectID) == tblID {
			keysToDelete = append(keysToDelete, key)
		}
		return nil
	})
	for _, key := range keysToDelete {
		um.mc.DeleteComment(key)
	}
}

// addAllCommentsToCatalog copies all uncommitted comments to the target catalog.
func (um *uncommittedMetadata) addAllCommentsToCatalog(target *nstree.MutableCatalog) error {
	return um.mc.ForEachComment(func(key catalogkeys.CommentKey, cmt string) error {
		return target.UpsertComment(key, cmt)
	})
}

// isCommentCached returns true if the comment key is in the cache (either as
// a value or marked absent).
func (um *uncommittedMetadata) isCommentCached(key catalogkeys.CommentKey) bool {
	if um.absentCommentKeys != nil {
		if _, absent := um.absentCommentKeys[key]; absent {
			return true
		}
	}
	_, found := um.mc.LookupComment(key)
	return found
}

// Zone config methods

// getUncommittedZoneConfig returns the uncommitted zone config for the given ID.
// It returns (zoneConfig, cached) where:
//   - cached=false means the ID hasn't been looked up yet
//   - cached=true, zoneConfig=nil means the ID was looked up and is absent
//   - cached=true, zoneConfig!=nil means the ID has a value
func (um *uncommittedMetadata) getUncommittedZoneConfig(
	id descpb.ID,
) (zc catalog.ZoneConfig, cached bool) {
	if um.absentZoneConfigIDs != nil {
		if _, absent := um.absentZoneConfigIDs[id]; absent {
			return nil, true
		}
	}
	if zc := um.mc.LookupZoneConfig(id); zc != nil {
		return zc, true
	}
	return nil, false
}

// markNoZoneConfig marks the zone config for this ID as absent (deleted or
// never existed). This adds it to the cache so subsequent lookups know not
// to query storage.
func (um *uncommittedMetadata) markNoZoneConfig(id descpb.ID) {
	um.mc.DeleteZoneConfig(id)
	if um.absentZoneConfigIDs == nil {
		um.absentZoneConfigIDs = make(map[descpb.ID]struct{})
	}
	um.absentZoneConfigIDs[id] = struct{}{}
}

// upsertZoneConfig adds or updates a zone config in the uncommitted layer.
func (um *uncommittedMetadata) upsertZoneConfig(id descpb.ID, zc *zonepb.ZoneConfig) error {
	delete(um.absentZoneConfigIDs, id)
	var val roachpb.Value
	if err := val.SetProto(zc); err != nil {
		return err
	}
	rawBytes, err := val.GetBytes()
	if err != nil {
		return err
	}
	um.mc.UpsertZoneConfig(id, zc, rawBytes)
	return nil
}

// addAllZoneConfigsToCatalog copies all uncommitted zone configs to the target
// catalog.
func (um *uncommittedMetadata) addAllZoneConfigsToCatalog(target *nstree.MutableCatalog) {
	_ = um.mc.ForEachZoneConfig(func(id descpb.ID, zc catalog.ZoneConfig) error {
		target.UpsertZoneConfig(id, zc.ZoneConfigProto(), zc.GetRawBytesInStorage())
		return nil
	})
}

// isZoneConfigCached returns true if the zone config ID is in the cache
// (either as a value or marked absent).
func (um *uncommittedMetadata) isZoneConfigCached(id descpb.ID) bool {
	if um.absentZoneConfigIDs != nil {
		if _, absent := um.absentZoneConfigIDs[id]; absent {
			return true
		}
	}
	return um.mc.LookupZoneConfig(id) != nil
}
