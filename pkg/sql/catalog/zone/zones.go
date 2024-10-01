// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package zone

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	yaml "gopkg.in/yaml.v2"
)

type OptionValue struct {
	InheritValue  bool
	ExplicitValue tree.TypedExpr
}

// ZoneConfigOption describes a Field one can set on a ZoneConfig.
type ZoneConfigOption struct {
	Field        config.Field
	RequiredType *types.T
	// Setter is used to set `Field` in the `zone config` to `datum`.
	Setter func(*zonepb.ZoneConfig, tree.Datum)
	// CheckAllowed, if not nil, is called to check if one is allowed to set this
	// zone config field.
	CheckAllowed func(context.Context, *cluster.Settings, tree.Datum) error
}

func loadYAML(dst interface{}, yamlString string) {
	if err := yaml.UnmarshalStrict([]byte(yamlString), dst); err != nil {
		panic(err)
	}
}

// SupportedZoneConfigOptions indicates how to translate SQL variable
// assignments in ALTER CONFIGURE ZONE to assignments to the member
// fields of zonepb.ZoneConfig.
var SupportedZoneConfigOptions map[tree.Name]ZoneConfigOption

// ZoneOptionKeys contains the keys from suportedZoneConfigOptions in
// deterministic order. Needed to make the event log output
// deterministic.
var ZoneOptionKeys []string

func init() {
	opts := []ZoneConfigOption{
		{
			Field:        config.RangeMinBytes,
			RequiredType: types.Int,
			Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.RangeMinBytes = proto.Int64(int64(tree.MustBeDInt(d))) },
		},
		{
			Field:        config.RangeMaxBytes,
			RequiredType: types.Int,
			Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.RangeMaxBytes = proto.Int64(int64(tree.MustBeDInt(d))) },
		},
		{
			Field:        config.GlobalReads,
			RequiredType: types.Bool,
			Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.GlobalReads = proto.Bool(bool(tree.MustBeDBool(d))) },
			CheckAllowed: func(ctx context.Context, settings *cluster.Settings, d tree.Datum) error {
				if !tree.MustBeDBool(d) {
					// Always allow the value to be unset.
					return nil
				}
				return base.CheckEnterpriseEnabled(
					settings,
					"global_reads",
				)
			},
		},
		{
			Field:        config.NumReplicas,
			RequiredType: types.Int,
			Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.NumReplicas = proto.Int32(int32(tree.MustBeDInt(d))) },
		},
		{
			Field:        config.NumVoters,
			RequiredType: types.Int,
			Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.NumVoters = proto.Int32(int32(tree.MustBeDInt(d))) },
		},
		{
			Field:        config.GCTTL,
			RequiredType: types.Int,
			Setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
				c.GC = &zonepb.GCPolicy{TTLSeconds: int32(tree.MustBeDInt(d))}
			},
		},
		{
			Field:        config.Constraints,
			RequiredType: types.String,
			Setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
				constraintsList := zonepb.ConstraintsList{
					Constraints: c.Constraints,
					Inherited:   c.InheritedConstraints,
				}
				loadYAML(&constraintsList, string(tree.MustBeDString(d)))
				c.Constraints = constraintsList.Constraints
				c.InheritedConstraints = false
			},
		},
		{
			Field:        config.VoterConstraints,
			RequiredType: types.String,
			Setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
				voterConstraintsList := zonepb.ConstraintsList{
					Constraints: c.VoterConstraints,
					Inherited:   c.InheritedVoterConstraints(),
				}
				loadYAML(&voterConstraintsList, string(tree.MustBeDString(d)))
				c.VoterConstraints = voterConstraintsList.Constraints
				c.NullVoterConstraintsIsEmpty = true
			},
		},
		{
			Field:        config.LeasePreferences,
			RequiredType: types.String,
			Setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
				loadYAML(&c.LeasePreferences, string(tree.MustBeDString(d)))
				c.InheritedLeasePreferences = false
			},
		},
	}
	SupportedZoneConfigOptions = make(map[tree.Name]ZoneConfigOption, len(opts))
	ZoneOptionKeys = make([]string, len(opts))
	for i, opt := range opts {
		name := opt.Field.String()
		key := tree.Name(name)
		if _, exists := SupportedZoneConfigOptions[key]; exists {
			panic(errors.AssertionFailedf("duplicate entry for key %s", name))
		}
		SupportedZoneConfigOptions[key] = opt
		ZoneOptionKeys[i] = name
	}
	sort.Strings(ZoneOptionKeys)
}
