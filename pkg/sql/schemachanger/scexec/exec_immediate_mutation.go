// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
)

type immediateState struct {
	modifiedDescriptors nstree.IDMap
	drainedNames        map[descpb.ID][]descpb.NameInfo
	descriptorsToDelete catalog.DescriptorIDSet
	commentsToUpdate    []commentToUpdate
	newDescriptors      nstree.IDMap
	withReset           bool
}

type commentToUpdate struct {
	id          int64
	subID       int64
	commentType catalogkeys.CommentType
	comment     string
}

var _ scmutationexec.ImmediateMutationStateUpdater = (*immediateState)(nil)

func (s *immediateState) AddToCheckedOutDescriptors(mut catalog.MutableDescriptor) {
	mut.MaybeIncrementVersion()
	s.modifiedDescriptors.Upsert(mut)
}

func (s *immediateState) MaybeGetCheckedOutDescriptor(id descpb.ID) catalog.MutableDescriptor {
	if newDesc := s.newDescriptors.Get(id); newDesc != nil {
		return newDesc.(catalog.MutableDescriptor)
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

func (s *immediateState) CreateDescriptor(desc catalog.MutableDescriptor) {
	s.newDescriptors.Upsert(desc)
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
	err := s.newDescriptors.Iterate(func(entry catalog.NameEntry) error {
		return c.CreateOrUpdateDescriptor(ctx, entry.(catalog.MutableDescriptor))
	})
	if err != nil {
		return err
	}
	err = s.modifiedDescriptors.Iterate(func(entry catalog.NameEntry) error {
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
	return c.Validate(ctx)
}
