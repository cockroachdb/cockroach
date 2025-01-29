// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	idxtype "github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
)

// HasDeprecatedElements returns if the target contains any element or fields
// marked for deprecation.
func HasDeprecatedElements(version clusterversion.ClusterVersion, target Target) bool {
	return target.GetSecondaryIndexPartial() != nil
}

// migrateDeprecatedFields will check if any of the deprecated fields are being
// used. If one is found, it will make the necessary migration, potentially
// returning a new target that must be added.
func migrateDeprecatedFields(
	version clusterversion.ClusterVersion, target Target,
) (migrated bool, newTargets []Target) {
	newTargets = make([]Target, 0)

	// Migrate IsInverted boolean to index Type enumeration.
	var idx *Index
	if primary := target.GetPrimaryIndex(); primary != nil {
		idx = &primary.Index
	}
	if secondary := target.GetSecondaryIndex(); secondary != nil {
		idx = &secondary.Index
	}
	if temp := target.GetTemporaryIndex(); temp != nil {
		idx = &temp.Index
	}
	if idx != nil && idx.IsInverted && idx.Type != idxtype.INVERTED {
		idx.Type = idxtype.INVERTED
		migrated = true
	}

	// Migrate ComputeExpr field  to separate ColumnComputeExpression target.
	if columnType := target.GetColumnType(); columnType != nil {
		if columnType.ComputeExpr != nil {
			newTarget := MakeTarget(
				AsTargetStatus(target.TargetStatus),
				&ColumnComputeExpression{
					TableID:    columnType.TableID,
					ColumnID:   columnType.ColumnID,
					Expression: *columnType.ComputeExpr,
				},
				&target.Metadata,
			)
			newTargets = append(newTargets, newTarget)
			columnType.ComputeExpr = nil
			migrated = true
		}
	}
	return
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

func checkForTableDataElement(target Target) (createID catid.DescID, existingID catid.DescID) {
	if target.TargetStatus != Status_PUBLIC {
		return catid.InvalidDescID, catid.InvalidDescID
	}
	switch e := target.Element().(type) {
	case *PrimaryIndex:
		return e.TableID, catid.InvalidDescID
	case *SecondaryIndex:
		return e.TableID, catid.InvalidDescID
	case *TableData:
		return catid.InvalidDescID, e.TableID
	}
	return catid.InvalidDescID, catid.InvalidDescID
}

// MigrateDescriptorState migrates descriptor state and applies any changes
// relevant for the current cluster version.
func MigrateDescriptorState(
	version clusterversion.ClusterVersion, parentID catid.DescID, state *DescriptorState,
) bool {
	// Nothing to do for empty states.
	if state == nil {
		return false
	}
	targetsToRemove := make(map[int]struct{})
	targetsToAdd := make([]Target, 0)
	currentStatusesToAdd := make([]Status, 0)
	newIndexes := make(map[catid.DescID]*TargetMetadata)
	newTargets := 0
	updated := false
	for idx, target := range state.Targets {
		if HasDeprecatedElements(version, target) {
			updated = true
			migrateTargetElement(state.Targets, idx)
			targetsToRemove[idx] = struct{}{}
		}
		if newIndexID, descID := checkForTableDataElement(target); descID != catid.InvalidDescID || newIndexID != catid.InvalidDescID {
			if _, ok := newIndexes[newIndexID]; newIndexID != catid.InvalidDescID && !ok {
				newIndexes[newIndexID] = &target.Metadata
			}
			if descID != catid.InvalidDescID {
				newIndexes[descID] = nil
			}
		}
		current, targetStatus, update := migrateStatuses(state.CurrentStatuses[idx], target.TargetStatus)
		if update {
			state.CurrentStatuses[idx] = current
			target.TargetStatus = targetStatus
			updated = true
		}
		if migrated, newTargets := migrateDeprecatedFields(version, target); migrated {
			updated = true
			for i := range newTargets {
				targetsToAdd = append(targetsToAdd, newTargets[i])
				// When adding new targets, we will just match the status of the target
				// that caused the migration.
				currentStatusesToAdd = append(currentStatusesToAdd, state.CurrentStatuses[idx])
			}
		}
	}
	for id, md := range newIndexes {
		if md == nil {
			continue
		}
		// Generate a TableData element
		state.Targets = append(state.Targets, MakeTarget(ToPublic, &TableData{
			TableID:    id,
			DatabaseID: parentID,
		}, md))
		state.CurrentStatuses = append(state.CurrentStatuses, Status_PUBLIC)
		state.TargetRanks = append(state.TargetRanks, math.MaxUint32-uint32(newTargets))
		newTargets += 1
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
	var maxRank uint32 // Keep track of rank in case we have targets to add
	for idx := range existingTargets {
		if _, ok := targetsToRemove[idx]; ok {
			continue
		}
		state.Targets = append(state.Targets, existingTargets[idx])
		state.TargetRanks = append(state.TargetRanks, existingTargetRanks[idx])
		maxRank = max(maxRank, existingTargetRanks[idx])
		state.CurrentStatuses = append(state.CurrentStatuses, existingStatuses[idx])
	}
	for idx, newTarget := range targetsToAdd {
		state.Targets = append(state.Targets, newTarget)
		// These targets are appended to the end. So, we will just set the rank to be max + 1.
		state.TargetRanks = append(state.TargetRanks, maxRank+1)
		maxRank++
		state.CurrentStatuses = append(state.CurrentStatuses, currentStatusesToAdd[idx])
	}

	return updated
}
