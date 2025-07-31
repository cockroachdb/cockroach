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
	zoneConfigsToUpdate        []zoneConfigToUpdate
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

// zoneConfigToUpdate is a struct that holds the information needed to update a
// zone config or subzone configs. If zc is subzone config, then we treat this
// as a subzone write and update the subzone configs (along with their subzone
// spans for the table). Otherwise, we write the whole zone config as an update.
type zoneConfigToUpdate struct {
	id              descpb.ID
	zc              *zonepb.ZoneConfig
	isSubzoneConfig bool
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
	s.zoneConfigsToUpdate = append(s.zoneConfigsToUpdate,
		zoneConfigToUpdate{
			id: id,
			zc: zc,
		})
}

func (s *immediateState) UpdateSubzoneConfig(
	tableid descpb.ID, subzone zonepb.Subzone, subzoneSpans []zonepb.SubzoneSpan,
) {
	zc := &zonepb.ZoneConfig{Subzones: []zonepb.Subzone{subzone}, SubzoneSpans: subzoneSpans}
	s.zoneConfigsToUpdate = append(s.zoneConfigsToUpdate,
		zoneConfigToUpdate{
			id:              tableid,
			zc:              zc,
			isSubzoneConfig: true,
		})
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

	for _, zc := range s.zoneConfigsToUpdate {
		if zc.isSubzoneConfig {
			if err := c.UpdateSubzoneConfig(ctx, zc.id, zc.zc.Subzones,
				zc.zc.SubzoneSpans); err != nil {
				return err
			}
		} else {

			if err := c.UpdateZoneConfig(ctx, zc.id, zc.zc); err != nil {
				return err
			}
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
