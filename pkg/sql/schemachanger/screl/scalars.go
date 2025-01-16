// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// AllTargetStateDescIDs applies AllTargetDescIDs to the whole target state.
func AllTargetStateDescIDs(s scpb.TargetState) (ids catalog.DescriptorIDSet) {
	for i := range s.Targets {
		AllTargetDescIDs(s.Targets[i].Element()).ForEach(ids.Add)
	}
	return ids
}

// AllTargetDescIDs returns all the descriptor IDs referenced in the element.
// This is a superset of the IDs of the descriptors actually affected by the
// schema change.
func AllTargetDescIDs(e scpb.Element) (ids catalog.DescriptorIDSet) {
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

// VersionSupportsElementUse checks if an element may be used at a given version.
func VersionSupportsElementUse(el scpb.Element, version clusterversion.ClusterVersion) bool {
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
		// These elements need v22.1 so they can be used without checking any version gates.
		return true
	case *scpb.IndexColumn, *scpb.EnumTypeValue, *scpb.TableZoneConfig:
		// These elements need v22.2 so they can be used without checking any version gates.
		return true
	case *scpb.DatabaseData, *scpb.TableData, *scpb.IndexData, *scpb.TablePartitioning,
		*scpb.Function, *scpb.FunctionName, *scpb.FunctionVolatility, *scpb.FunctionLeakProof,
		*scpb.FunctionNullInputBehavior, *scpb.FunctionBody,
		*scpb.ColumnNotNull, *scpb.CheckConstraintUnvalidated, *scpb.UniqueWithoutIndexConstraintUnvalidated,
		*scpb.ForeignKeyConstraintUnvalidated, *scpb.IndexZoneConfig, *scpb.TableSchemaLocked, *scpb.CompositeType,
		*scpb.CompositeTypeAttrType, *scpb.CompositeTypeAttrName:
		// These elements need v23.1 so they can be used without checking any version gates.
		return true
	case *scpb.SequenceOption:
		// These elements need v23.2 so they can be used without checking any version gates.
		return true
	case *scpb.TypeComment, *scpb.DatabaseZoneConfig:
		// These elements need v24.2 so they can be used without checking any version gates.
		return true
	case *scpb.ColumnComputeExpression, *scpb.FunctionSecurity, *scpb.LDRJobIDs,
		*scpb.PartitionZoneConfig, *scpb.Trigger, *scpb.TriggerName,
		*scpb.TriggerEnabled, *scpb.TriggerTiming, *scpb.TriggerEvents, *scpb.TriggerTransition,
		*scpb.TriggerWhen, *scpb.TriggerFunctionCall, *scpb.TriggerDeps:
		// These elements need v24.3 so they can be used without checking any version gates.
		return true
	case *scpb.NamedRangeZoneConfig, *scpb.Policy, *scpb.PolicyName, *scpb.PolicyRole:
		return version.IsActive(clusterversion.V25_1)
	default:
		panic(errors.AssertionFailedf("unknown element %T", el))
	}
}

// MaxElementVersion returns the maximum cluster version at which an element
// may be used.
func MaxElementVersion(el scpb.Element) (version clusterversion.Key, exists bool) {
	return 0, false /* exists */
}
