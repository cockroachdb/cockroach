// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import "github.com/cockroachdb/cockroach/pkg/clusterversion"

// HasDeprecatedElements returns if the target contains any element marked
// for deprecation.
func HasDeprecatedElements(version clusterversion.ClusterVersion, target Target) bool {
	if version.IsActive(clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates) &&
		target.GetSecondaryIndexPartial() != nil {
		return true
	}
	return false
}

// migrateTargetElement migrates an individual target at a given index.
func migrateTargetElement(targets []Target, idx int) {
	targetToMigrate := targets[idx]
	switch t := targetToMigrate.Element().(type) {
	case *SecondaryIndexPartial:
		for _, target := range targets {
			if secondaryIndex := target.GetSecondaryIndex(); secondaryIndex != nil &&
				secondaryIndex.TableID == t.TableID &&
				secondaryIndex.IndexID == t.IndexID &&
				target.TargetStatus == targetToMigrate.TargetStatus {
				secondaryIndex.EmbeddedExpr = &t.Expression
				break
			}
		}
	}
}

// migrateStatuses used to migrate individual statuses and generate
// new current and target statuses.
func migrateStatuses(
	currentStatus Status, targetStatus Status,
) (newCurrentStatus Status, newTargetStatus Status, updated bool) {
	// Target state of TXN_DROPPED has been removed, so push plans further along.
	// Note: No version is required for this transition, since it will be valid
	// for all releases.
	if targetStatus == Status_ABSENT && currentStatus == Status_TXN_DROPPED {
		return Status_PUBLIC, targetStatus, true
	} else if targetStatus == Status_PUBLIC && currentStatus == Status_TXN_DROPPED {
		return Status_ABSENT, targetStatus, true
	}
	return currentStatus, targetStatus, false
}

// MigrateCurrentState migrates a current state by upgrading elements based
// on the current version number.
func MigrateCurrentState(version clusterversion.ClusterVersion, state *CurrentState) bool {
	// Nothing to do for empty states.
	if state == nil {
		return false
	}
	targetsToRemove := make(map[int]struct{})
	updated := false
	for idx, target := range state.Targets {
		if HasDeprecatedElements(version, target) {
			updated = true
			migrateTargetElement(state.Targets, idx)
			targetsToRemove[idx] = struct{}{}
		}
		current, targetStatus, update := migrateStatuses(state.Current[idx], target.TargetStatus)
		if update {
			state.Current[idx] = current
			target.TargetStatus = targetStatus
			updated = true
		}
	}
	if !updated {
		return updated
	}
	existingTargets := state.Targets
	existingStatuses := state.Current
	initialStatuses := state.Initial

	state.Targets = make([]Target, 0, len(existingTargets))
	state.Current = make([]Status, 0, len(existingStatuses))
	state.Initial = make([]Status, 0, len(initialStatuses))
	for idx := range existingTargets {
		if _, ok := targetsToRemove[idx]; ok {
			continue
		}
		state.Targets = append(state.Targets, existingTargets[idx])
		state.Current = append(state.Current, existingStatuses[idx])
		state.Initial = append(state.Initial, initialStatuses[idx])

	}
	return updated
}

// MigrateDescriptorState migrates descriptor state and applies any changes
// relevant for the current cluster version.
func MigrateDescriptorState(version clusterversion.ClusterVersion, state *DescriptorState) bool {
	// Nothing to do for empty states.
	if state == nil {
		return false
	}
	targetsToRemove := make(map[int]struct{})
	updated := false
	for idx, target := range state.Targets {
		if HasDeprecatedElements(version, target) {
			updated = true
			migrateTargetElement(state.Targets, idx)
			targetsToRemove[idx] = struct{}{}
		}
		current, targetStatus, update := migrateStatuses(state.CurrentStatuses[idx], target.TargetStatus)
		if update {
			state.CurrentStatuses[idx] = current
			target.TargetStatus = targetStatus
			updated = true
		}
	}
	if !updated {
		return updated
	}
	existingTargets := state.Targets
	existingTargetRanks := state.TargetRanks
	existingStatuses := state.CurrentStatuses
	state.Targets = make([]Target, 0, len(existingTargets))
	state.TargetRanks = make([]uint32, 0, len(existingTargetRanks))
	state.CurrentStatuses = make([]Status, 0, len(existingStatuses))
	for idx := range existingTargets {
		if _, ok := targetsToRemove[idx]; ok {
			continue
		}
		state.Targets = append(state.Targets, existingTargets[idx])
		state.TargetRanks = append(state.TargetRanks, existingTargetRanks[idx])
		state.CurrentStatuses = append(state.CurrentStatuses, existingStatuses[idx])
	}
	return updated
}
