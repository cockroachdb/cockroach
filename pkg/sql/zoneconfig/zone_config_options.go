// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zoneconfig

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v2"
)

// EnterpriseCheckCallback call back for validating if a feature requires an
// enterprise license.
type EnterpriseCheckCallback func(featureName string) error

// SupportedZoneConfigOptions indicates how to translate SQL variable
// assignments in ALTER CONFIGURE ZONE to assignments to the member
// fields of zonepb.ZoneConfig.
var SupportedZoneConfigOptions = map[tree.Name]struct {
	RequiredType *types.T
	Setter       func(*zonepb.ZoneConfig, tree.Datum)
	Getter       func(*zonepb.ZoneConfig) (tree.Datum, error)
	CheckAllowed func(context.Context, EnterpriseCheckCallback, tree.Datum) error // optional
}{
	"range_min_bytes": {
		RequiredType: types.Int,
		Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.RangeMinBytes = proto.Int64(int64(tree.MustBeDInt(d))) },
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.RangeMinBytes != nil {
				return tree.NewDInt(tree.DInt(*c.RangeMinBytes)), nil
			}
			return tree.DNull, nil
		},
	},
	"range_max_bytes": {
		RequiredType: types.Int,
		Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.RangeMaxBytes = proto.Int64(int64(tree.MustBeDInt(d))) },
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.RangeMaxBytes != nil {
				return tree.NewDInt(tree.DInt(*c.RangeMinBytes)), nil
			}
			return tree.DNull, nil
		},
	},
	"global_reads": {
		RequiredType: types.Bool,
		Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.GlobalReads = proto.Bool(bool(tree.MustBeDBool(d))) },
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.GlobalReads != nil {
				return tree.MakeDBool(tree.DBool(*c.GlobalReads)), nil
			}
			return tree.DNull, nil
		},
		CheckAllowed: func(ctx context.Context, featureCheck EnterpriseCheckCallback, d tree.Datum) error {
			if !tree.MustBeDBool(d) {
				// Always allow the value to be unset.
				return nil
			}
			return featureCheck("global_reads")
		},
	},
	"num_replicas": {
		RequiredType: types.Int,
		Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.NumReplicas = proto.Int32(int32(tree.MustBeDInt(d))) },
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.NumReplicas != nil {
				return tree.NewDInt(tree.DInt(*c.NumReplicas)), nil
			}
			return tree.DNull, nil
		},
	},
	"num_voters": {
		RequiredType: types.Int,
		Setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.NumVoters = proto.Int32(int32(tree.MustBeDInt(d))) },
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.NumVoters != nil {
				return tree.NewDInt(tree.DInt(*c.NumVoters)), nil
			}
			return tree.DNull, nil
		},
	},
	"gc.ttlseconds": {
		RequiredType: types.Int,
		Setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
			c.GC = &zonepb.GCPolicy{TTLSeconds: int32(tree.MustBeDInt(d))}
		},
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.GC != nil {
				return tree.NewDInt(tree.DInt(c.GC.TTLSeconds)), nil
			}
			return tree.DNull, nil
		},
	},
	"constraints": {
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
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.Constraints != nil && !c.InheritedConstraints {
				constraintsList := zonepb.ConstraintsList{
					Constraints: c.Constraints,
					Inherited:   c.InheritedConstraints,
				}
				out, err := yaml.Marshal(constraintsList)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(string(out)), nil
			}
			return tree.DNull, nil
		},
	},
	"voter_constraints": {
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
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.VoterConstraints != nil && !c.NullVoterConstraintsIsEmpty {
				out, err := yaml.Marshal(c.VoterConstraints)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(string(out)), nil
			}
			return tree.DNull, nil
		},
	},
	"lease_preferences": {
		RequiredType: types.String,
		Setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
			loadYAML(&c.LeasePreferences, string(tree.MustBeDString(d)))
			c.InheritedLeasePreferences = false
		},
		Getter: func(c *zonepb.ZoneConfig) (tree.Datum, error) {
			if c.LeasePreferences != nil && !c.InheritedLeasePreferences {
				out, err := yaml.Marshal(c.LeasePreferences)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(string(out)), nil
			}
			return tree.DNull, nil
		},
	},
}

// ZoneOptionKeys contains the keys from SupportedZoneConfigOptions in
// deterministic order. Needed to make the event log output
// deterministic.
var ZoneOptionKeys = func() []string {
	l := make([]string, 0, len(SupportedZoneConfigOptions))
	for k := range SupportedZoneConfigOptions {
		l = append(l, string(k))
	}
	sort.Strings(l)
	return l
}()

func loadYAML(dst interface{}, yamlString string) {
	if err := yaml.UnmarshalStrict([]byte(yamlString), dst); err != nil {
		panic(err)
	}
}
