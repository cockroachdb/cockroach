// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

// PartitionByForRegionalByRow constructs the tree.PartitionBy clause for
// REGIONAL BY ROW tables.
func PartitionByForRegionalByRow(regionConfig RegionConfig, col tree.Name) *tree.PartitionBy {
	listPartition := make([]tree.ListPartition, len(regionConfig.Regions()))
	for i, region := range regionConfig.Regions() {
		listPartition[i] = tree.ListPartition{
			Name:  tree.Name(region),
			Exprs: tree.Exprs{tree.NewStrVal(string(region))},
		}
	}

	return &tree.PartitionBy{
		Fields: tree.NameList{col},
		List:   listPartition,
	}
}

// RegionalByRowDefaultColDef builds the default column definition of the
// `crdb_region` column for REGIONAL BY ROW tables.
func RegionalByRowDefaultColDef(
	oid oid.Oid, defaultExpr tree.Expr, onUpdateExpr tree.Expr,
) *tree.ColumnTableDef {
	c := &tree.ColumnTableDef{
		Name:   tree.RegionalByRowRegionDefaultColName,
		Type:   &tree.OIDTypeReference{OID: oid},
		Hidden: true,
	}
	c.Nullable.Nullability = tree.NotNull
	c.DefaultExpr.Expr = defaultExpr
	c.OnUpdateExpr.Expr = onUpdateExpr

	return c
}

// RegionalByRowGatewayRegionDefaultExpr builds an expression which returns the
// default gateway region of a REGIONAL BY ROW table.
func RegionalByRowGatewayRegionDefaultExpr(oid oid.Oid) tree.Expr {
	return &tree.CastExpr{
		Expr: &tree.FuncExpr{
			Func: tree.WrapFunction(builtinconstants.DefaultToDatabasePrimaryRegionBuiltinName),
			Exprs: []tree.Expr{
				&tree.FuncExpr{
					Func: tree.WrapFunction(builtinconstants.GatewayRegionBuiltinName),
				},
			},
		},
		Type:       &tree.OIDTypeReference{OID: oid},
		SyntaxMode: tree.CastShort,
	}
}

// MaybeRegionalByRowOnUpdateExpr returns a gateway region default statement if
// the auto rehoming session setting is enabled, nil otherwise.
func MaybeRegionalByRowOnUpdateExpr(evalCtx *eval.Context, enumOid oid.Oid) tree.Expr {
	if evalCtx.SessionData().AutoRehomingEnabled {
		return &tree.CastExpr{
			Expr: &tree.FuncExpr{
				Func: tree.WrapFunction(builtinconstants.RehomeRowBuiltinName),
			},
			Type:       &tree.OIDTypeReference{OID: enumOid},
			SyntaxMode: tree.CastShort,
		}
	}
	return nil
}

func SynthesizeRegionConfig(
	regionEnumDesc catalog.RegionEnumTypeDescriptor,
	dbDesc catalog.DatabaseDescriptor,
	o SynthesizeRegionConfigOptions,
) (RegionConfig, error) {
	var regionNames, transitioningRegionNames, addingRegionNames catpb.RegionNames
	_ = regionEnumDesc.ForEachRegion(func(name catpb.RegionName, transition descpb.TypeDescriptor_EnumMember_Direction) error {
		switch transition {
		case descpb.TypeDescriptor_EnumMember_NONE:
			regionNames = append(regionNames, name)
		case descpb.TypeDescriptor_EnumMember_ADD:
			transitioningRegionNames = append(transitioningRegionNames, name)
			addingRegionNames = append(addingRegionNames, name)
		case descpb.TypeDescriptor_EnumMember_REMOVE:
			transitioningRegionNames = append(transitioningRegionNames, name)
			if o.ForValidation {
				// Since the partitions and zone configs are only updated when a transaction
				// commits, this must ignore all regions being added (since they will not be
				// reflected in the zone configuration yet), but it must include all region
				// being dropped (since they will not be dropped from the zone configuration
				// until they are fully removed from the type descriptor, again, at the end
				// of the transaction).
				regionNames = append(regionNames, name)
			}
		}
		return nil
	})
	survivalGoal := dbDesc.GetRegionConfig().SurvivalGoal
	if o.ForceSurvivalGoal != nil {
		survivalGoal = *o.ForceSurvivalGoal
	}
	regionConfig := MakeRegionConfig(
		regionNames,
		dbDesc.GetRegionConfig().PrimaryRegion,
		survivalGoal,
		regionEnumDesc.GetID(),
		dbDesc.GetRegionConfig().Placement,
		regionEnumDesc.TypeDesc().RegionConfig.SuperRegions,
		regionEnumDesc.TypeDesc().RegionConfig.ZoneConfigExtensions,
		WithTransitioningRegions(transitioningRegionNames),
		WithAddingRegions(addingRegionNames),
		WithSecondaryRegion(dbDesc.GetRegionConfig().SecondaryRegion),
	)

	if err := ValidateRegionConfig(regionConfig, dbDesc.GetID() == keys.SystemDatabaseID); err != nil {
		return RegionConfig{}, err
	}

	return regionConfig, nil
}

type SynthesizeRegionConfigOptions struct {
	IncludeOffline    bool
	ForValidation     bool
	UseCache          bool
	ForceSurvivalGoal *descpb.SurvivalGoal
}

// SynthesizeRegionConfigOption is an option to pass into SynthesizeRegionConfig.
type SynthesizeRegionConfigOption func(o *SynthesizeRegionConfigOptions)

// SynthesizeRegionConfigOptionIncludeOffline includes offline descriptors for use
// in RESTORE.
var SynthesizeRegionConfigOptionIncludeOffline SynthesizeRegionConfigOption = func(o *SynthesizeRegionConfigOptions) {
	o.IncludeOffline = true
}

// SynthesizeRegionConfigOptionForValidation includes descriptors which are being dropped
// as part of the regions field, allowing validation to account for regions in the
// process of being dropped.
var SynthesizeRegionConfigOptionForValidation SynthesizeRegionConfigOption = func(o *SynthesizeRegionConfigOptions) {
	o.ForValidation = true
}

// SynthesizeRegionConfigOptionUseCache uses a cache for synthesizing the region
// config.
var SynthesizeRegionConfigOptionUseCache SynthesizeRegionConfigOption = func(o *SynthesizeRegionConfigOptions) {
	o.UseCache = true
}

// SynthesizeRegionConfigOptionForceSurvivalZone forces the zone survival goal
// instead of inheriting from the system database.
var SynthesizeRegionConfigOptionForceSurvivalZone SynthesizeRegionConfigOption = func(o *SynthesizeRegionConfigOptions) {
	z := descpb.SurvivalGoal_ZONE_FAILURE
	o.ForceSurvivalGoal = &z
}
