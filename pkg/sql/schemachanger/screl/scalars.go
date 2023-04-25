// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// GetDescID retrieves the descriptor ID from the element.
func GetDescID(e scpb.Element) catid.DescID {
	id, err := Schema.GetAttribute(DescID, e)
	if err != nil {
		// Note that this is safe because we have a unit test that ensures that
		// all elements don't panic on this.
		panic(errors.NewAssertionErrorWithWrappedErrf(
			err, "failed to retrieve descriptor ID for %T", e,
		))
	}
	return id.(catid.DescID)
}

// GetIndexID retrieves the index ID from the element if it has one.
func GetIndexID(e scpb.Element) (catid.IndexID, bool) {
	v, err := Schema.GetAttribute(IndexID, e)
	if err != nil {
		return 0, false
	}
	return v.(catid.IndexID), true
}

// AllTargetDescIDs returns all the descriptor IDs referenced in the
// target state's elements. This is a superset of the IDs of the descriptors
// affected by the schema change.
func AllTargetDescIDs(s scpb.TargetState) (ids catalog.DescriptorIDSet) {
	for i := range s.Targets {
		e := s.Targets[i].Element()
		// Handle special cases to tighten this superset a bit.
		switch te := e.(type) {
		case *scpb.Namespace:
			// Ignore the parent database and schema in the namespace element:
			// - the parent schema of an object has no back-references to it,
			// - the parent database has back-references to a schema, but these
			//   will be captured by the scpb.SchemaParent target.
			ids.Add(te.DescriptorID)
		case *scpb.SchemaChild:
			// Ignore the parent schema, it won't have back-references.
			ids.Add(te.ChildObjectID)
		case *scpb.TableData:
			// Ignore the parent database in the table data element, the parent
			// database won't have back-references to any tables.
			ids.Add(te.TableID)
		default:
			_ = WalkDescIDs(e, func(id *catid.DescID) error {
				ids.Add(*id)
				return nil
			})
		}
	}
	return ids
}

// AllDescIDs returns all the IDs referenced by an element.
func AllDescIDs(e scpb.Element) (ids catalog.DescriptorIDSet) {
	if e == nil {
		return ids
	}
	// For certain elements the references needed will not be attributes, so manually
	// include these.
	_ = WalkDescIDs(e, func(id *catid.DescID) error {
		ids.Add(*id)
		return nil
	})
	return ids
}

// ContainsDescID searches the element to see if it contains a descriptor id.
func ContainsDescID(haystack scpb.Element, needle catid.DescID) (contains bool) {
	_ = WalkDescIDs(haystack, func(id *catid.DescID) error {
		if contains = *id == needle; contains {
			return iterutil.StopIteration()
		}
		return nil
	})
	return contains
}

// MinElementVersion returns the minimum cluster version at which an element may
// be used.
func MinElementVersion(el scpb.Element) clusterversion.Key {
	switch el.(type) {
	case *scpb.Database, *scpb.Schema, *scpb.View, *scpb.Sequence, *scpb.Table,
		*scpb.AliasType, *scpb.ColumnFamily, *scpb.Column, *scpb.PrimaryIndex,
		*scpb.SecondaryIndex, *scpb.TemporaryIndex, *scpb.EnumType,
		*scpb.UniqueWithoutIndexConstraint, *scpb.CheckConstraint,
		*scpb.ForeignKeyConstraint, *scpb.TableComment, *scpb.RowLevelTTL,
		*scpb.TableLocalityGlobal, *scpb.TableLocalityPrimaryRegion,
		*scpb.TableLocalitySecondaryRegion, *scpb.TableLocalityRegionalByRow,
		*scpb.ColumnName, *scpb.ColumnType, *scpb.ColumnDefaultExpression,
		*scpb.ColumnOnUpdateExpression, *scpb.SequenceOwner, *scpb.ColumnComment,
		*scpb.IndexName, *scpb.IndexPartitioning, *scpb.SecondaryIndexPartial,
		*scpb.IndexComment, *scpb.ConstraintWithoutIndexName, *scpb.ConstraintComment,
		*scpb.Namespace, *scpb.Owner, *scpb.UserPrivileges,
		*scpb.DatabaseRegionConfig, *scpb.DatabaseRoleSetting, *scpb.DatabaseComment,
		*scpb.SchemaParent, *scpb.SchemaComment, *scpb.SchemaChild:
		return clusterversion.TODODelete_V22_1
	case *scpb.CompositeType, *scpb.CompositeTypeAttrType, *scpb.CompositeTypeAttrName:
		return clusterversion.V23_1
	case *scpb.IndexColumn, *scpb.EnumTypeValue, *scpb.TableZoneConfig:
		return clusterversion.V22_2
	case *scpb.DatabaseData, *scpb.TableData, *scpb.IndexData, *scpb.TablePartitioning,
		*scpb.Function, *scpb.FunctionName, *scpb.FunctionVolatility, *scpb.FunctionLeakProof,
		*scpb.FunctionNullInputBehavior, *scpb.FunctionBody, *scpb.FunctionParamDefaultExpression:
		return clusterversion.V23_1
	case *scpb.ColumnNotNull, *scpb.CheckConstraintUnvalidated,
		*scpb.UniqueWithoutIndexConstraintUnvalidated, *scpb.ForeignKeyConstraintUnvalidated,
		*scpb.IndexZoneConfig, *scpb.TableSchemaLocked:
		return clusterversion.V23_1
	default:
		panic(errors.AssertionFailedf("unknown element %T", el))
	}
}

// MaxElementVersion returns the minimum cluster version at which an element may
// be used.
func MaxElementVersion(el scpb.Element) (version *clusterversion.Key) {
	var v clusterversion.Key
	switch el.(type) {
	case *scpb.SecondaryIndexPartial:
		v = clusterversion.V23_1_SchemaChangerDeprecatedIndexPredicates
		return &v
	}
	return nil
}
