// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ListGroups lists all groups.
func (s *Service) ListGroups(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	inputDTO authtypes.InputListGroupsDTO,
) ([]*auth.Group, int, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return []*auth.Group{}, 0, nil
	}
	return s.repo.ListGroups(ctx, l, inputDTO.Filters)
}

// GetGroup retrieves a group by ID.
// Returns ErrGroupNotFound if the group does not exist.
func (s *Service) GetGroup(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) (*auth.Group, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return nil, authtypes.ErrUnauthorized
	}
	group, err := s.repo.GetGroup(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, authtypes.ErrGroupNotFound
		}
		return nil, errors.Wrap(err, "failed to get group")
	}
	return group, nil
}

// GetGroupByExternalID retrieves a group by external ID.
func (s *Service) GetGroupByExternalID(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, externalID string,
) (*auth.Group, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return nil, authtypes.ErrUnauthorized
	}
	return s.repo.GetGroupByExternalID(ctx, l, externalID)
}

// GetGroupWithMembers retrieves a group and its members.
// Returns ErrGroupNotFound if the group does not exist.
func (s *Service) GetGroupWithMembers(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) (*auth.Group, []*auth.GroupMember, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return nil, nil, authtypes.ErrUnauthorized
	}
	group, err := s.repo.GetGroup(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, nil, authtypes.ErrGroupNotFound
		}
		return nil, nil, errors.Wrap(err, "failed to get group")
	}
	members, _, err := s.repo.GetGroupMembers(ctx, l, id, *filters.NewFilterSet())
	if err != nil {
		return nil, nil, err
	}
	return group, members, nil
}

// CreateGroupWithMembers creates a new group with optional initial members.
// All operations are performed atomically.
func (s *Service) CreateGroupWithMembers(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	input authtypes.CreateGroupInput,
) (*auth.Group, []*auth.GroupMember, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return nil, nil, authtypes.ErrUnauthorized
	}

	// Check for duplicate external ID (SCIM requires unique externalId)
	if input.ExternalID != nil {
		existingGroup, err := s.repo.GetGroupByExternalID(ctx, l, *input.ExternalID)
		if err != nil && !errors.Is(err, rauth.ErrNotFound) {
			return nil, nil, errors.Wrap(err, "failed to check for duplicate external ID")
		}
		if existingGroup != nil {
			return nil, nil, authtypes.ErrDuplicateGroup
		}
	}

	now := timeutil.Now()
	group := &auth.Group{
		ID:          uuid.MakeV4(),
		ExternalID:  input.ExternalID,
		DisplayName: input.DisplayName,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Single atomic transaction: create group + add members
	if err := s.repo.CreateGroupWithMembers(ctx, l, group, input.Members); err != nil {
		return nil, nil, err
	}

	// Fetch members after transaction (separate query is acceptable)
	members, _, err := s.repo.GetGroupMembers(ctx, l, group.ID, *filters.NewFilterSet())
	if err != nil {
		return nil, nil, err
	}

	s.auditEvent(ctx, l, principal, AuditSCIMGroupCreated, "success", map[string]interface{}{
		"group_id":     group.ID.String(),
		"display_name": group.DisplayName,
		"member_count": len(members),
	})

	// Record metrics
	s.metrics.RecordSCIMGroupCreated()
	if len(members) > 0 {
		s.metrics.RecordSCIMGroupMemberAdded(len(members))
	}

	return group, members, nil
}

// ReplaceGroup replaces a group's attributes and members atomically.
func (s *Service) ReplaceGroup(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	id uuid.UUID,
	input authtypes.ReplaceGroupInput,
) (*auth.Group, []*auth.GroupMember, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return nil, nil, authtypes.ErrUnauthorized
	}

	// Get existing group
	group, err := s.repo.GetGroup(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, nil, authtypes.ErrGroupNotFound
		}
		return nil, nil, errors.Wrap(err, "failed to get group")
	}

	// Get current members to compute diff
	currentMembers, _, err := s.repo.GetGroupMembers(ctx, l, id, *filters.NewFilterSet())
	if err != nil {
		return nil, nil, err
	}

	// Prepare group update (PUT replaces the entire resource per RFC 7644 §3.5.1)
	group.ExternalID = input.ExternalID
	group.DisplayName = input.DisplayName
	group.UpdatedAt = timeutil.Now()

	// Compute member diff
	currentSet := make(map[uuid.UUID]bool)
	for _, m := range currentMembers {
		currentSet[m.UserID] = true
	}
	desiredSet := make(map[uuid.UUID]bool)
	for _, userID := range input.Members {
		desiredSet[userID] = true
	}

	var ops []rauth.GroupMemberOperation
	for _, userID := range input.Members {
		if !currentSet[userID] {
			ops = append(ops, rauth.GroupMemberOperation{Op: "add", UserID: userID})
		}
	}
	for _, m := range currentMembers {
		if !desiredSet[m.UserID] {
			ops = append(ops, rauth.GroupMemberOperation{Op: "remove", UserID: m.UserID})
		}
	}

	// Single atomic transaction: update group + apply member ops
	if err := s.repo.UpdateGroupWithMembers(ctx, l, group, ops); err != nil {
		return nil, nil, err
	}

	// Fetch members after transaction
	members, _, err := s.repo.GetGroupMembers(ctx, l, id, *filters.NewFilterSet())
	if err != nil {
		return nil, nil, err
	}

	s.auditEvent(ctx, l, principal, AuditSCIMGroupUpdated, "success", map[string]interface{}{
		"group_id":     group.ID.String(),
		"display_name": group.DisplayName,
	})

	// Record metrics
	s.metrics.RecordSCIMGroupUpdated()
	if len(ops) > 0 {
		s.auditEvent(ctx, l, principal, AuditSCIMGroupMembersUpdated, "success", map[string]interface{}{
			"group_id":      group.ID.String(),
			"added_count":   countMemberOps(ops, "add"),
			"removed_count": countMemberOps(ops, "remove"),
		})
		addedCount := countMemberOps(ops, "add")
		removedCount := countMemberOps(ops, "remove")
		if addedCount > 0 {
			s.metrics.RecordSCIMGroupMemberAdded(addedCount)
		}
		if removedCount > 0 {
			s.metrics.RecordSCIMGroupMemberRemoved(removedCount)
		}
	}

	return group, members, nil
}

// PatchGroup applies patch operations to a group atomically.
// Returns ErrGroupNotFound if the group does not exist.
func (s *Service) PatchGroup(
	ctx context.Context,
	l *logger.Logger,
	principal *pkgauth.Principal,
	id uuid.UUID,
	input authtypes.PatchGroupInput,
) (*authtypes.PatchGroupOutput, error) {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return nil, authtypes.ErrUnauthorized
	}

	// Get existing group
	group, err := s.repo.GetGroup(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return nil, authtypes.ErrGroupNotFound
		}
		return nil, errors.Wrap(err, "failed to get group")
	}

	needsUpdate := false
	var memberOps []rauth.GroupMemberOperation

	// Process operations to build group updates and member ops
	for _, op := range input.Operations {
		switch {
		case op.Op == "replace" && op.Path == "displayName":
			if displayName, ok := op.Value.(string); ok {
				group.DisplayName = displayName
				needsUpdate = true
			}
		case op.Op == "replace" && op.Path == "externalId":
			if externalID, ok := op.Value.(string); ok {
				group.ExternalID = &externalID
				needsUpdate = true
			}
		case op.Op == "add" && op.Path == "members":
			memberOps = append(memberOps, s.parseMemberOps("add", op.Value)...)
		case op.Op == "remove" && op.Path == "members":
			// If filters provided, use them to identify members to remove
			if op.Filters != nil {
				currentMembers, _, err := s.repo.GetGroupMembers(ctx, l, id, *op.Filters)
				if err != nil {
					return nil, err
				}
				for _, m := range currentMembers {
					memberOps = append(memberOps, rauth.GroupMemberOperation{Op: "remove", UserID: m.UserID})
				}
			} else {
				memberOps = append(memberOps, s.parseMemberOps("remove", op.Value)...)
			}
		case op.Op == "replace" && op.Path == "members":
			// Replace all members: remove all current, then add new
			currentMembers, _, err := s.repo.GetGroupMembers(ctx, l, id, *filters.NewFilterSet())
			if err != nil {
				return nil, err
			}
			for _, m := range currentMembers {
				memberOps = append(memberOps, rauth.GroupMemberOperation{Op: "remove", UserID: m.UserID})
			}
			memberOps = append(memberOps, s.parseMemberOps("add", op.Value)...)
		}
	}

	// Update timestamp whenever group or its membership changes
	if needsUpdate || len(memberOps) > 0 {
		group.UpdatedAt = timeutil.Now()
		// Single atomic transaction: update group + apply member ops
		if err := s.repo.UpdateGroupWithMembers(ctx, l, group, memberOps); err != nil {
			return nil, err
		}
	}

	// Fetch members after transaction
	members, _, err := s.repo.GetGroupMembers(ctx, l, id, *filters.NewFilterSet())
	if err != nil {
		return nil, err
	}

	if needsUpdate {
		s.auditEvent(ctx, l, principal, AuditSCIMGroupUpdated, "success", map[string]interface{}{
			"group_id": group.ID.String(),
		})
		s.metrics.RecordSCIMGroupUpdated()
	}
	if len(memberOps) > 0 {
		s.auditEvent(ctx, l, principal, AuditSCIMGroupMembersUpdated, "success", map[string]interface{}{
			"group_id":      group.ID.String(),
			"added_count":   countMemberOps(memberOps, "add"),
			"removed_count": countMemberOps(memberOps, "remove"),
		})
		addedCount := countMemberOps(memberOps, "add")
		removedCount := countMemberOps(memberOps, "remove")
		if addedCount > 0 {
			s.metrics.RecordSCIMGroupMemberAdded(addedCount)
		}
		if removedCount > 0 {
			s.metrics.RecordSCIMGroupMemberRemoved(removedCount)
		}
	}

	return &authtypes.PatchGroupOutput{
		Group:   group,
		Members: members,
	}, nil
}

// parseMemberOps parses member values from patch operation value into GroupMemberOperations.
// Controllers normalize member values to []map[string]string before passing to service.
func (s *Service) parseMemberOps(op string, value any) []rauth.GroupMemberOperation {
	var ops []rauth.GroupMemberOperation

	switch v := value.(type) {
	case []map[string]string:
		// Normalized format from SCIM and admin controllers
		for _, member := range v {
			if valueStr, ok := member["value"]; ok {
				if userID, err := uuid.FromString(valueStr); err == nil {
					ops = append(ops, rauth.GroupMemberOperation{Op: op, UserID: userID})
				}
			}
		}
	case []uuid.UUID:
		// Direct UUID slice (used by ReplaceGroup and internal operations)
		for _, userID := range v {
			ops = append(ops, rauth.GroupMemberOperation{Op: op, UserID: userID})
		}
	}

	return ops
}

// DeleteGroup deletes a group.
// Returns ErrGroupNotFound if the group does not exist.
func (s *Service) DeleteGroup(
	ctx context.Context, l *logger.Logger, principal *pkgauth.Principal, id uuid.UUID,
) error {
	if !principal.HasPermission(authtypes.PermissionScimManageUser) {
		return authtypes.ErrUnauthorized
	}
	err := s.repo.DeleteGroup(ctx, l, id)
	if err != nil {
		if errors.Is(err, rauth.ErrNotFound) {
			return authtypes.ErrGroupNotFound
		}
		return errors.Wrap(err, "failed to delete group")
	}

	s.auditEvent(ctx, l, principal, AuditSCIMGroupDeleted, "success", map[string]interface{}{
		"group_id": id.String(),
	})

	s.metrics.RecordSCIMGroupDeleted()

	return nil
}

// countMemberOps counts operations of a specific type.
func countMemberOps(ops []rauth.GroupMemberOperation, opType string) int {
	count := 0
	for _, op := range ops {
		if op.Op == opType {
			count++
		}
	}
	return count
}
