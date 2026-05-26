// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// HumanReadableNameProvider maps IDs to human-readable names
// for the purpose of decorating EXPLAIN(DDL) and other
// human-readable output.
type HumanReadableNameProvider interface {

	// Name returns the name mapped to a descriptor ID, falling back
	// to a placeholder if none was found.
	Name(id catid.DescID) string

	// IndexName returns the name mapped to an index ID, falling back
	// to a placeholder if none was found.
	IndexName(relationID catid.DescID, indexID catid.IndexID) string

	// ColumnName returns the name mapped to a column ID, falling back
	// to a placeholder if none was found.
	ColumnName(relationID catid.DescID, columnID catid.ColumnID) string

	// FamilyName returns the name mapped to a family ID, falling back
	// to a placeholder if none was found.
	FamilyName(relationID catid.DescID, familyID catid.FamilyID) string

	// ConstraintName returns the name mapped to a constraint ID, falling back
	// to a placeholder if none was found.
	ConstraintName(relationID catid.DescID, constraintID catid.ConstraintID) string
}

// TargetState implements the HumanReadableNameProvider interface by mapping
// the selected ID or ID tuple to a name element in the target state if there
// is one. If there is none, then the schema change does not involve name
// elements and TargetState delegates to its NameMappings, effectively falling
// back on the names such as they were prior to the schema change.
var _ HumanReadableNameProvider = TargetState{}

// Name implements the HumanReadableNameProvider interface.
func (ts TargetState) Name(id catid.DescID) string {
	if name := maybeNameFromTargets(ts, func(e Element) string {
		switch e := e.(type) {
		case *Namespace:
			if e.DescriptorID == id {
				return e.Name
			}
		case *FunctionName:
			if e.FunctionID == id {
				return e.Name
			}
		}
		return ""
	}); len(name) > 0 {
		return name
	}
	return NameMappings(ts.NameMappings).Name(id)
}

// IndexName implements the HumanReadableNameProvider interface.
func (ts TargetState) IndexName(relationID catid.DescID, indexID catid.IndexID) string {
	if name := maybeNameFromTargets(ts, func(e Element) string {
		switch e := e.(type) {
		case *IndexName:
			if e.TableID == relationID && e.IndexID == indexID {
				return e.Name
			}
		}
		return ""
	}); len(name) > 0 {
		return name
	}
	return NameMappings(ts.NameMappings).IndexName(relationID, indexID)
}

// ColumnName implements the HumanReadableNameProvider interface.
func (ts TargetState) ColumnName(relationID catid.DescID, columnID catid.ColumnID) string {
	if name := maybeNameFromTargets(ts, func(e Element) string {
		switch e := e.(type) {
		case *ColumnName:
			if e.TableID == relationID && e.ColumnID == columnID {
				return e.Name
			}
		}
		return ""
	}); len(name) > 0 {
		return name
	}
	return NameMappings(ts.NameMappings).ColumnName(relationID, columnID)
}

// FamilyName implements the HumanReadableNameProvider interface.
func (ts TargetState) FamilyName(relationID catid.DescID, familyID catid.FamilyID) string {
	if name := maybeNameFromTargets(ts, func(e Element) string {
		switch e := e.(type) {
		case *ColumnFamily:
			if e.TableID == relationID && e.FamilyID == familyID {
				return e.Name
			}
		}
		return ""
	}); len(name) > 0 {
		return name
	}
	return NameMappings(ts.NameMappings).FamilyName(relationID, familyID)
}

// ConstraintName implements the HumanReadableNameProvider interface.
func (ts TargetState) ConstraintName(
	relationID catid.DescID, constraintID catid.ConstraintID,
) string {
	if name := maybeNameFromTargets(ts, func(e Element) string {
		switch e := e.(type) {
		case *ConstraintWithoutIndexName:
			if e.TableID == relationID && e.ConstraintID == constraintID {
				return e.Name
			}
		}
		return ""
	}); len(name) > 0 {
		return name
	}
	return NameMappings(ts.NameMappings).ConstraintName(relationID, constraintID)
}

func maybeNameFromTargets(ts TargetState, maybeNameFn func(e Element) string) string {
	var added, transient, dropped string
	for _, t := range ts.Targets {
		if name := maybeNameFn(t.Element()); len(name) > 0 {
			switch t.TargetStatus {
			case Status_PUBLIC:
				added = name
			case Status_ABSENT:
				dropped = name
			default:
				transient = name
			}
		}
	}
	var sb strings.Builder
	if len(dropped) > 0 {
		sb.WriteString(dropped)
		sb.WriteRune('-')
	}
	if len(transient) > 0 {
		sb.WriteString(transient)
		sb.WriteRune('~')
	}
	if len(added) > 0 {
		sb.WriteString(added)
		sb.WriteRune('+')
	}
	return sb.String()
}

// NameMappings implements the HumanReadableNameProvider interface.
// This data structure includes all of the names of things in each of
// the descriptors involved in the schema change, such as they were
// at build-time.
type NameMappings []NameMapping

var _ sort.Interface = (*NameMappings)(nil)
var _ HumanReadableNameProvider = (*NameMappings)(nil)

// Len implements the sort.Interface interface.
func (nms NameMappings) Len() int {
	return len(nms)
}

// Less implements the sort.Interface interface.
func (nms NameMappings) Less(i, j int) bool {
	return nms[i].ID < nms[j].ID
}

// Swap implements the sort.Interface interface.
func (nms NameMappings) Swap(i, j int) {
	nms[i], nms[j] = nms[j], nms[i]
}

// Name implements the HumanReadableNameProvider interface.
func (nms NameMappings) Name(id catid.DescID) string {
	nm := nms.Find(id)
	if nm == nil {
		return fmt.Sprintf("#%d", id)
	}
	return nm.Name
}

// IndexName implements the HumanReadableNameProvider interface.
func (nms NameMappings) IndexName(relationID catid.DescID, indexID catid.IndexID) string {
	nm := nms.Find(relationID)
	if nm == nil {
		return fmt.Sprintf("[%d AS t]@[%d]", relationID, indexID)
	}
	if indexName, ok := nm.Indexes[indexID]; ok {
		return indexName
	}
	return fmt.Sprintf("%s@[%d]", nm.Name, indexID)
}

// ColumnName implements the HumanReadableNameProvider interface.
func (nms NameMappings) ColumnName(relationID catid.DescID, columnID catid.ColumnID) string {
	nm := nms.Find(relationID)
	if nm == nil {
		return fmt.Sprintf("[%d AS t].[%d]", relationID, columnID)
	}
	if columnName, ok := nm.Columns[columnID]; ok {
		return columnName
	}
	return fmt.Sprintf("%s.[%d]", nm.Name, columnID)
}

// FamilyName implements the HumanReadableNameProvider interface.
func (nms NameMappings) FamilyName(relationID catid.DescID, familyID catid.FamilyID) string {
	nm := nms.Find(relationID)
	if nm == nil {
		return fmt.Sprintf("[%d AS t].[family %d]", relationID, familyID)
	}
	if familyName, ok := nm.Families[familyID]; ok {
		return familyName
	}
	return fmt.Sprintf("%s.[family %d]", nm.Name, familyID)
}

// ConstraintName implements the HumanReadableNameProvider interface.
func (nms NameMappings) ConstraintName(
	relationID catid.DescID, constraintID catid.ConstraintID,
) string {
	nm := nms.Find(relationID)
	if nm == nil {
		return fmt.Sprintf("[%d AS t].[constraint %d]", relationID, constraintID)
	}
	if constraintName, ok := nm.Constraints[constraintID]; ok {
		return constraintName
	}
	return fmt.Sprintf("%s.[constraint %d]", nm.Name, constraintID)
}

// Find returns a pointer to the NameMapping for the given ID, nil otherwise.
func (nms NameMappings) Find(id catid.DescID) *NameMapping {
	for i, nm := range nms {
		if nm.ID < id {
			continue
		}
		if nm.ID > id {
			break
		}
		return &nms[i]
	}
	return nil
}
