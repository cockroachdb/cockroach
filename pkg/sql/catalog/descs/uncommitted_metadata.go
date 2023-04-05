// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
)

type uncommittedComments struct {
	uncommitted map[catalogkeys.CommentKey]string
	cachedKeys  map[catalogkeys.CommentKey]struct{}
}

func makeUncommittedComments() uncommittedComments {
	return uncommittedComments{
		uncommitted: make(map[catalogkeys.CommentKey]string),
		cachedKeys:  make(map[catalogkeys.CommentKey]struct{}),
	}
}

func (uc *uncommittedComments) reset() {
	uc.uncommitted = make(map[catalogkeys.CommentKey]string)
	uc.cachedKeys = make(map[catalogkeys.CommentKey]struct{})
}

func (uc *uncommittedComments) getUncommitted(
	key catalogkeys.CommentKey,
) (cmt string, hasCmt bool, cached bool) {
	if _, ok := uc.cachedKeys[key]; !ok {
		return "", false, false
	}

	cmt, hasCmt = uc.uncommitted[key]
	return cmt, hasCmt, true
}

// markNoComment lets the cache know that the comment for this key is dropped.
func (uc *uncommittedComments) markNoComment(key catalogkeys.CommentKey) {
	delete(uc.uncommitted, key)
	uc.cachedKeys[key] = struct{}{}
}

func (uc *uncommittedComments) markTableDeleted(tblID descpb.ID) {
	var keysToDel []catalogkeys.CommentKey
	for k := range uc.uncommitted {
		if k.ObjectID == uint32(tblID) {
			keysToDel = append(keysToDel, k)
		}
	}
	for _, k := range keysToDel {
		delete(uc.uncommitted, k)
	}
}

func (uc *uncommittedComments) upsert(key catalogkeys.CommentKey, cmt string) {
	uc.cachedKeys[key] = struct{}{}
	uc.uncommitted[key] = cmt
}

func (uc *uncommittedComments) addAllToCatalog(mc nstree.MutableCatalog) error {
	for ck, cmt := range uc.uncommitted {
		if err := mc.UpsertComment(ck, cmt); err != nil {
			return err
		}
	}
	return nil
}

type uncommittedZoneConfigs struct {
	uncommitted map[descpb.ID]catalog.ZoneConfig
	cachedDescs map[descpb.ID]struct{}
}

func makeUncommittedZoneConfigs() uncommittedZoneConfigs {
	return uncommittedZoneConfigs{
		uncommitted: make(map[descpb.ID]catalog.ZoneConfig),
		cachedDescs: make(map[descpb.ID]struct{}),
	}
}

func (uc *uncommittedZoneConfigs) reset() {
	uc.uncommitted = make(map[descpb.ID]catalog.ZoneConfig)
	uc.cachedDescs = make(map[descpb.ID]struct{})
}

func (uc *uncommittedZoneConfigs) getUncommitted(
	id descpb.ID,
) (zc catalog.ZoneConfig, cached bool) {
	if _, ok := uc.cachedDescs[id]; !ok {
		return nil, false
	}
	return uc.uncommitted[id], true
}

func (uc *uncommittedZoneConfigs) markNoZoneConfig(id descpb.ID) {
	delete(uc.uncommitted, id)
	uc.cachedDescs[id] = struct{}{}
}

func (uc *uncommittedZoneConfigs) upsert(id descpb.ID, zc *zonepb.ZoneConfig) error {
	uc.cachedDescs[id] = struct{}{}
	var val roachpb.Value
	if err := val.SetProto(zc); err != nil {
		return err
	}
	rawBytes, err := val.GetBytes()
	if err != nil {
		return err
	}
	uc.uncommitted[id] = zone.NewZoneConfigWithRawBytes(zc, rawBytes)
	return nil
}

func (uc *uncommittedZoneConfigs) addAllToCatalog(mc nstree.MutableCatalog) {
	for id, zc := range uc.uncommitted {
		mc.UpsertZoneConfig(id, zc.ZoneConfigProto(), zc.GetRawBytesInStorage())
	}
}
