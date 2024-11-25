// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/errors"
)

type immediateState struct {
	modifiedDescriptors        nstree.IDMap
	drainedNames               map[descpb.ID][]descpb.NameInfo
	descriptorsToDelete        catalog.DescriptorIDSet
	commentsToUpdate           []commentToUpdate
	newDescriptors             map[descpb.ID]catalog.MutableDescriptor
	addedNames                 map[descpb.ID]descpb.NameInfo
	withReset                  bool
	sequencesToInit            []sequenceToInit
	temporarySchemasToRegister map[descpb.ID]*temporarySchemaToRegister
	modifiedZoneConfigs        []zoneConfigToUpsert
	modifiedSubzoneConfigs     map[descpb.ID][]subzoneConfigToUpsert
	zoneConfigsToDelete        map[descpb.ID]struct{}
	subzoneConfigsToDelete     map[descpb.ID][]subzoneConfigToDelete
}

type temporarySchemaToRegister struct {
	parentID   descpb.ID
	schemaName string
}

type commentToUpdate struct {
	id          int64
	subID       int64
	commentType catalogkeys.CommentType
	comment     string
}

type sequenceToInit struct {
	id       descpb.ID
	startVal int64
}

// zoneConfigToUpsert is a struct that holds the information needed to update a
// zone config. This does not include subzone configs -- which are handled in
// subzoneConfigToUpsert.
type zoneConfigToUpsert struct {
	id descpb.ID
	zc *zonepb.ZoneConfig
}

// subzoneConfigToUpsert is a struct that holds the information needed to update
// a subzone config.
type subzoneConfigToUpsert struct {
	subzone        zonepb.Subzone
	subzoneSpans   []zonepb.SubzoneSpan
	idxRefToDelete int32
}

type subzoneConfigToDelete struct {
	subzone      zonepb.Subzone
	subzoneSpans []zonepb.SubzoneSpan
}

var _ scmutationexec.ImmediateMutationStateUpdater = (*immediateState)(nil)

func (s *immediateState) AddToCheckedOutDescriptors(mut catalog.MutableDescriptor) {
	mut.MaybeIncrementVersion()
	s.modifiedDescriptors.Upsert(mut)
}

func (s *immediateState) MaybeGetCheckedOutDescriptor(id descpb.ID) catalog.MutableDescriptor {
	if newDesc, ok := s.newDescriptors[id]; ok {
		return newDesc
	}
	entry := s.modifiedDescriptors.Get(id)
	if entry == nil {
		return nil
	}
	return entry.(catalog.MutableDescriptor)
}

func (s *immediateState) DeleteDescriptor(id descpb.ID) {
	s.descriptorsToDelete.Add(id)
}

func (s *immediateState) AddComment(
	id descpb.ID, subID int, commentType catalogkeys.CommentType, comment string,
) {
	s.commentsToUpdate = append(s.commentsToUpdate,
		commentToUpdate{
			id:          int64(id),
			subID:       int64(subID),
			commentType: commentType,
			comment:     comment,
		})
}

func (s *immediateState) DeleteComment(
	id descpb.ID, subID int, commentType catalogkeys.CommentType,
) {
	s.commentsToUpdate = append(s.commentsToUpdate,
		commentToUpdate{
			id:          int64(id),
			subID:       int64(subID),
			commentType: commentType,
		})
}

func (s *immediateState) DeleteName(id descpb.ID, nameInfo descpb.NameInfo) {
	if s.drainedNames == nil {
		s.drainedNames = make(map[descpb.ID][]descpb.NameInfo)
	}
	s.drainedNames[id] = append(s.drainedNames[id], nameInfo)
}

func (s *immediateState) AddName(id descpb.ID, nameInfo descpb.NameInfo) {
	if s.addedNames == nil {
		s.addedNames = make(map[descpb.ID]descpb.NameInfo)
	}
	if info, ok := s.addedNames[id]; ok {
		panic(errors.AssertionFailedf("descriptor %v already has a to-be-added name %v; get"+
			"a request to add another name %v for it", id, info.String(), nameInfo.String()))
	}
	s.addedNames[id] = nameInfo
}

func (s *immediateState) CreateDescriptor(desc catalog.MutableDescriptor) {
	if s.newDescriptors == nil {
		s.newDescriptors = make(map[descpb.ID]catalog.MutableDescriptor)
	}
	s.newDescriptors[desc.GetID()] = desc
}

func (s *immediateState) InitSequence(id descpb.ID, startVal int64) {
	s.sequencesToInit = append(s.sequencesToInit,
		sequenceToInit{
			id:       id,
			startVal: startVal,
		})
}

func (s *immediateState) UpdateZoneConfig(id descpb.ID, zc *zonepb.ZoneConfig) {
	s.modifiedZoneConfigs = append(s.modifiedZoneConfigs,
		zoneConfigToUpsert{
			id: id,
			zc: zc,
		})
}

func (s *immediateState) UpdateSubzoneConfig(
	tableID descpb.ID,
	subzone zonepb.Subzone,
	subzoneSpans []zonepb.SubzoneSpan,
	idxRefToDelete int32,
) {
	if s.modifiedSubzoneConfigs == nil {
		s.modifiedSubzoneConfigs = make(map[descpb.ID][]subzoneConfigToUpsert)
	}
	szCfgToUpsert := subzoneConfigToUpsert{
		subzone:        subzone,
		subzoneSpans:   subzoneSpans,
		idxRefToDelete: idxRefToDelete,
	}
	if szCfgs, ok := s.modifiedSubzoneConfigs[tableID]; ok {
		s.modifiedSubzoneConfigs[tableID] = append(szCfgs, szCfgToUpsert)
	} else {
		s.modifiedSubzoneConfigs[tableID] = []subzoneConfigToUpsert{szCfgToUpsert}
	}
}

func (s *immediateState) DeleteZoneConfig(id descpb.ID) {
	if s.zoneConfigsToDelete == nil {
		s.zoneConfigsToDelete = make(map[descpb.ID]struct{})
	}
	if _, ok := s.zoneConfigsToDelete[id]; !ok {
		s.zoneConfigsToDelete[id] = struct{}{}
	}
}

func (s *immediateState) DeleteSubzoneConfig(
	tableID descpb.ID, subzone zonepb.Subzone, subzoneSpans []zonepb.SubzoneSpan,
) {
	if s.subzoneConfigsToDelete == nil {
		s.subzoneConfigsToDelete = make(map[descpb.ID][]subzoneConfigToDelete)
	}
	szCfgToDelete := subzoneConfigToDelete{
		subzone:      subzone,
		subzoneSpans: subzoneSpans,
	}
	if szCfgs, ok := s.subzoneConfigsToDelete[tableID]; ok {
		s.subzoneConfigsToDelete[tableID] = append(szCfgs, szCfgToDelete)
	} else {
		s.subzoneConfigsToDelete[tableID] = []subzoneConfigToDelete{szCfgToDelete}
	}
}

func (s *immediateState) Reset() {
	s.withReset = true
}

func (s *immediateState) exec(ctx context.Context, c Catalog) error {
	if s.withReset {
		if err := c.Reset(ctx); err != nil {
			return err
		}
	}
	s.descriptorsToDelete.ForEach(func(id descpb.ID) {
		s.modifiedDescriptors.Remove(id)
	})
	for _, newDescID := range getOrderedNewDescriptorIDs(s.newDescriptors) {
		// Create new descs by the ascending order of their ID. This determinism
		// helps avoid flakes in end-to-end tests in which we assert a particular
		// order of desc upsertion.
		if err := c.CreateOrUpdateDescriptor(ctx, s.newDescriptors[newDescID]); err != nil {
			return err
		}
	}
	err := s.modifiedDescriptors.Iterate(func(entry catalog.NameEntry) error {
		return c.CreateOrUpdateDescriptor(ctx, entry.(catalog.MutableDescriptor))
	})
	if err != nil {
		return err
	}
	for _, id := range s.descriptorsToDelete.Ordered() {
		if err := c.DeleteDescriptor(ctx, id); err != nil {
			return err
		}
	}
	for id, drainedNames := range s.drainedNames {
		for _, name := range drainedNames {
			if err := c.DeleteName(ctx, name, id); err != nil {
				return err
			}
		}
	}
	for id, name := range s.addedNames {
		if err := c.AddName(ctx, name, id); err != nil {
			return err
		}
		if tempIdx := s.temporarySchemasToRegister[id]; tempIdx != nil {
			tempIdx.schemaName = name.Name
		}
	}
	for _, u := range s.commentsToUpdate {
		k := catalogkeys.MakeCommentKey(uint32(u.id), uint32(u.subID), u.commentType)
		if len(u.comment) > 0 {
			if err := c.UpdateComment(ctx, k, u.comment); err != nil {
				return err
			}
		} else {
			if err := c.DeleteComment(ctx, k); err != nil {
				return err
			}
		}
	}
	for _, s := range s.sequencesToInit {
		c.InitializeSequence(s.id, s.startVal)
	}
	for tempIdxId, tempIdxToRegister := range s.temporarySchemasToRegister {
		c.InsertTemporarySchema(tempIdxToRegister.schemaName, tempIdxToRegister.parentID, tempIdxId)
	}

	for id := range s.zoneConfigsToDelete {
		if err = c.DeleteZoneConfig(ctx, id); err != nil {
			return err
		}
	}

	for _, zcToUpdate := range s.modifiedZoneConfigs {
		if err = c.UpdateZoneConfig(ctx, zcToUpdate.id, zcToUpdate.zc); err != nil {
			return err
		}
	}

	for id, szcs := range s.subzoneConfigsToDelete {
		for _, toDelete := range szcs {
			if err = c.DeleteSubzoneConfig(
				ctx, id, toDelete.subzone, toDelete.subzoneSpans); err != nil {
				return err
			}
		}
	}

	for id, szcs := range s.modifiedSubzoneConfigs {
		zcToWrite, err := c.GetZoneConfig(ctx, id)
		if err != nil {
			return err
		}
		for _, toUpdate := range szcs {
			zcToWrite, err = c.UpdateSubzoneConfig(ctx, zcToWrite, toUpdate.subzone,
				toUpdate.subzoneSpans, toUpdate.idxRefToDelete)
			if err != nil {
				return err
			}
		}
		if err = c.WriteZoneConfigToBatch(ctx, id, zcToWrite); err != nil {
			return err
		}
	}

	return c.Validate(ctx)
}

func (s *immediateState) AddTemporarySchema(id descpb.ID) {
	if s.temporarySchemasToRegister == nil {
		s.temporarySchemasToRegister = make(map[descpb.ID]*temporarySchemaToRegister)
	}
	s.temporarySchemasToRegister[id] = &temporarySchemaToRegister{}
}

func (s *immediateState) AddTemporarySchemaParent(id descpb.ID, databaseID descpb.ID) {
	s.temporarySchemasToRegister[id].parentID = databaseID
}

// getOrderedNewDescriptorIDs returns ids in `newDescriptors` in ascending order.
func getOrderedNewDescriptorIDs(
	newDescriptors map[descpb.ID]catalog.MutableDescriptor,
) []descpb.ID {
	res := make([]descpb.ID, 0, len(newDescriptors))
	for id := range newDescriptors {
		res = append(res, id)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})
	return res
}
